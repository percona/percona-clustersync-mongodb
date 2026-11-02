//go:build integration

package repl //nolint:testpackage // Exercises recovery from exact index-application windows.

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/sel"
)

//nolint:paralleltest // Each case owns two MongoDB containers.
func TestRepl_PCSM128_IndexRecoveryWindows(t *testing.T) {
	for _, window := range []string{"before index", "partial indexes", "after drop"} {
		t.Run(window, func(t *testing.T) {
			runReplIntegrationTest(t, func(ctx context.Context) {
				const db, coll, textIndex = "pcsm128_windows", "data", "text_1"
				runCtx, cancelRun := context.WithCancel(ctx)
				defer cancelRun()
				var recovering atomic.Bool
				created, dropped := make(chan struct{}, 4), make(chan struct{}, 2)
				source := connectReplTestClient(ctx, t, newReplTestReplicaSet(ctx, t), nil)
				target := connectReplTestClient(ctx, t, newReplTestReplicaSet(ctx, t), &event.CommandMonitor{
					Succeeded: func(_ context.Context, e *event.CommandSucceededEvent) {
						if e.DatabaseName != db {
							return
						}
						switch e.CommandName {
						case "createIndexes":
							// Stop after the first acknowledged target index,
							// before the catalog can issue its next command.
							if window == "partial indexes" && !recovering.Load() {
								cancelRun()
							}
							created <- struct{}{}
						case "dropIndexes":
							dropped <- struct{}{}
						}
					},
				})
				version, err := mdb.Version(ctx, source)
				require.NoError(t, err)
				cat := catalog.NewCatalog(source, target, version)
				require.NoError(t, source.Database(db).CreateCollection(ctx, coll))
				require.NoError(t, cat.CreateCollection(ctx, db, coll, &catalog.CreateCollectionOptions{}))
				sourceColl := source.Database(db).Collection(coll,
					options.Collection().SetWriteConcern(writeconcern.Majority()))
				targetColl := target.Database(db).Collection(coll)
				type snapshot struct {
					Repl    *Checkpoint         `bson:"repl"`
					Catalog *catalog.Checkpoint `bson:"catalog"`
				}
				checkpoints := target.Database(config.PCSMDatabase).Collection("index_windows",
					options.Collection().SetWriteConcern(writeconcern.Majority()))
				var r *Repl
				persist := func(saveCtx context.Context) error {
					cat.LockWrite()
					raw, marshalErr := bson.Marshal(snapshot{Repl: r.Checkpoint(), Catalog: cat.Checkpoint()})
					cat.UnlockWrite()
					if marshalErr != nil {
						return errors.Wrap(marshalErr, "marshal window checkpoint")
					}
					_, saveErr := checkpoints.ReplaceOne(saveCtx, bson.D{{Key: "_id", Value: "boundary"}},
						bson.Raw(raw), options.Replace().SetUpsert(true))

					return errors.Wrap(saveErr, "persist window checkpoint")
				}
				r = NewRepl(source, target, cat, sel.MakeFilter([]string{db + "." + coll}, nil),
					&Options{NumWorkers: 1, Checkpoint: func(saveCtx context.Context) error {
						if saveErr := persist(saveCtx); saveErr != nil {
							return saveErr
						}
						if window == "before index" {
							cancelRun()

							return errors.Wrap(runCtx.Err(), "interrupt before index command")
						}

						return nil
					}}, version, false, false)
				startAt, err := mdb.AdvanceClusterTime(ctx, source)
				require.NoError(t, err)
				require.NoError(t, r.Start(runCtx, startAt))
				originalDone := r.Done()
				defer func() {
					cancelRun()
					awaitSignal(ctx, t, originalDone, "original run did not stop")
				}()

				_, err = sourceColl.InsertOne(ctx, bson.D{
					{Key: "_id", Value: "deleted"}, {Key: "a", Value: bson.D{{Key: "b", Value: bson.A{}}}},
					{Key: "words", Value: "omnibus"},
				})
				require.NoError(t, err)
				_, err = sourceColl.DeleteOne(ctx, bson.D{{Key: "_id", Value: "deleted"}})
				require.NoError(t, err)
				indexes := []mongo.IndexModel{{
					Keys:    bson.D{{Key: "a.b", Value: 1}, {Key: "words", Value: "text"}},
					Options: options.Index().SetName(textIndex),
				}}
				if window == "partial indexes" {
					indexes = append([]mongo.IndexModel{{
						Keys: bson.D{{Key: "a.b", Value: 1}}, Options: options.Index().SetName("first_1"),
					}}, indexes...)
				}
				_, err = sourceColl.Indexes().CreateMany(ctx, indexes)
				require.NoError(t, err)
				if window == "after drop" {
					awaitSignal(ctx, t, created, "target index was not created")
					require.NoError(t, sourceColl.Indexes().DropOne(ctx, textIndex))
					awaitSignal(ctx, t, dropped, "target index was not dropped")
					cancelRun()
				}
				awaitSignal(ctx, t, originalDone, "interrupted run did not stop")
				require.NoError(t, r.Status().Err)
				targetBefore := checkpointWindowIndexes(ctx, t, targetColl)
				expectedCount := 1
				if window == "partial indexes" {
					expectedCount = 2
				}
				require.Len(t, targetBefore, expectedCount, "requested interruption window was not reached")

				var saved snapshot
				require.NoError(t, checkpoints.FindOne(ctx, bson.D{{Key: "_id", Value: "boundary"}}).Decode(&saved))
				require.NotNil(t, saved.Repl)
				require.NotEmpty(t, saved.Repl.StartAfter)
				cat = catalog.NewCatalog(source, target, version)
				require.NoError(t, cat.Recover(saved.Catalog))
				r = NewRepl(source, target, cat, sel.MakeFilter([]string{db + "." + coll}, nil),
					&Options{NumWorkers: 1, Checkpoint: persist}, version, false, false)
				require.NoError(t, r.Recover(ctx, saved.Repl))
				newWrites, err := targetColl.Watch(ctx, mongo.Pipeline{bson.D{{Key: "$match", Value: bson.D{
					{Key: "operationType", Value: "insert"}, {Key: "documentKey._id", Value: "after-recovery"},
				}}}})
				require.NoError(t, err)
				defer func() { require.NoError(t, newWrites.Close(ctx)) }()
				recovering.Store(true)
				resumeCtx, cancelResume := context.WithCancel(ctx)
				defer cancelResume()
				require.NoError(t, r.Resume(resumeCtx))
				resumedDone := r.Done()
				defer func() {
					cancelResume()
					awaitSignal(ctx, t, resumedDone, "recovered run did not stop")
				}()
				_, err = sourceColl.InsertOne(ctx, bson.D{
					{Key: "_id", Value: "after-recovery"}, {Key: "a", Value: bson.D{{Key: "b", Value: 1}}},
					{Key: "words", Value: "live"},
				})
				require.NoError(t, err)
				require.True(t, newWrites.Next(ctx), "recovery did not continue: %v; repl: %v",
					newWrites.Err(), r.Status().Err)
				cancelResume()
				awaitSignal(ctx, t, resumedDone, "recovered run did not finish")
				require.NoError(t, r.Status().Err)
				require.ErrorIs(t, targetColl.FindOne(ctx, bson.D{{Key: "_id", Value: "deleted"}}).Err(),
					mongo.ErrNoDocuments)
				expected := checkpointWindowIndexes(ctx, t, sourceColl)
				actual := checkpointWindowIndexes(ctx, t, targetColl)
				require.Len(t, actual, len(expected))
				for name, spec := range expected {
					require.Contains(t, actual, name)
					require.Equal(t, spec.Lookup("key"), actual[name].Lookup("key"), name)
					require.Equal(t, spec.Lookup("weights"), actual[name].Lookup("weights"), name)
				}
			})
		})
	}
}

func checkpointWindowIndexes(ctx context.Context, t *testing.T, coll *mongo.Collection) map[string]bson.Raw {
	t.Helper()
	cursor, err := coll.Indexes().List(ctx)
	require.NoError(t, err)
	var specs []bson.Raw
	require.NoError(t, cursor.All(ctx, &specs))
	result := make(map[string]bson.Raw, len(specs))
	for _, spec := range specs {
		result[spec.Lookup("name").StringValue()] = spec
	}

	return result
}
