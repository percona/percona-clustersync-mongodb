//go:build integration

package repl //nolint:testpackage // Exercises the real stream, workers, catalog, and recovery.

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/sel"
)

//nolint:paralleltest // MongoDB testcontainers are serialized to avoid Docker resource contention.
func TestRepl_PCSM128_IndexCheckpointRecoversWithoutReplayingDeletedInsert(t *testing.T) {
	runReplIntegrationTest(t, func(ctx context.Context) {
		const db, coll, index = "pcsm128", "data", "nested_text"
		var acknowledged, recovering, deletedOnTarget atomic.Bool
		resumeCommands := make(chan bson.Raw, 1)
		source := connectReplTestClient(ctx, t, newReplTestReplicaSet(ctx, t), &event.CommandMonitor{
			Started: func(_ context.Context, e *event.CommandStartedEvent) {
				if e.CommandName == "aggregate" && recovering.Load() {
					select {
					case resumeCommands <- append(bson.Raw(nil), e.Command...):
					default:
					}
				}
			},
		})
		indexStarted := make(chan bool, 4)
		indexSucceeded := make(chan struct{}, 4)
		target := connectReplTestClient(ctx, t, newReplTestReplicaSet(ctx, t), &event.CommandMonitor{
			Started: func(_ context.Context, e *event.CommandStartedEvent) {
				if e.CommandName == "createIndexes" && e.DatabaseName == db {
					indexStarted <- acknowledged.Load()
				}
			},
			Succeeded: func(_ context.Context, e *event.CommandSucceededEvent) {
				if e.CommandName == "delete" && e.DatabaseName == db {
					deletedOnTarget.Store(true)
				}
				if e.CommandName == "createIndexes" && e.DatabaseName == db {
					indexSucceeded <- struct{}{}
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
		targetColl := target.Database(db).Collection(coll,
			options.Collection().SetReadConcern(readconcern.Majority()))

		// Subscribe before any writes: the delete token is the exact predecessor,
		// and the target stream proves continued replication after fresh recovery.
		deletes, err := sourceColl.Watch(ctx, mongo.Pipeline{
			bson.D{{Key: "$match", Value: bson.D{{Key: "operationType", Value: "delete"}}}},
		})
		require.NoError(t, err)
		defer func() { require.NoError(t, deletes.Close(ctx)) }()
		newWrites, err := targetColl.Watch(ctx, mongo.Pipeline{
			bson.D{{Key: "$match", Value: bson.D{
				{Key: "operationType", Value: "insert"},
				{Key: "documentKey._id", Value: "after-recovery"},
			}}},
		})
		require.NoError(t, err)
		defer func() { require.NoError(t, newWrites.Close(ctx)) }()

		type snapshot struct {
			Repl    *Checkpoint         `bson:"repl"`
			Catalog *catalog.Checkpoint `bson:"catalog"`
		}
		checkpoints := target.Database(config.PCSMDatabase).Collection("pcsm128_checkpoints",
			options.Collection().SetWriteConcern(writeconcern.Majority()).
				SetReadConcern(readconcern.Majority()))
		var r *Repl
		persist := func(saveCtx context.Context, id string) error {
			cat.LockWrite()
			raw, marshalErr := bson.Marshal(snapshot{Repl: r.Checkpoint(), Catalog: cat.Checkpoint()})
			cat.UnlockWrite()
			if marshalErr != nil {
				return errors.Wrap(marshalErr, "marshal integration checkpoint")
			}
			_, saveErr := checkpoints.ReplaceOne(saveCtx, bson.D{{Key: "_id", Value: id}}, bson.Raw(raw),
				options.Replace().SetUpsert(true))

			return errors.Wrap(saveErr, "persist integration checkpoint")
		}
		boundarySaved := make(chan bool, 1)
		ack := make(chan struct{})
		opts := &Options{
			NumWorkers:             1,
			UseCollectionBulkWrite: true,
			Checkpoint: func(saveCtx context.Context) error {
				drained := deletedOnTarget.Load()
				if saveErr := persist(saveCtx, "boundary"); saveErr != nil {
					return saveErr
				}
				boundarySaved <- drained
				select {
				case <-ack:
					return nil
				case <-saveCtx.Done():
					return errors.Wrap(saveCtx.Err(), "checkpoint acknowledgement canceled")
				}
			},
		}
		r = NewRepl(source, target, cat, sel.MakeFilter([]string{db + "." + coll}, nil),
			opts, version, false, false)
		startAt, err := mdb.AdvanceClusterTime(ctx, source)
		require.NoError(t, err)
		runCtx, cancelRun := context.WithCancel(ctx)
		require.NoError(t, r.Start(runCtx, startAt))
		originalDone := r.Done()
		defer func() {
			cancelRun()
			awaitSignal(ctx, t, originalDone, "original replicator did not stop during cleanup")
		}()
		require.NoError(t, persist(ctx, "initial"))

		// The insert is legal before the text index, but replaying it after the
		// index exists fails with "cannot have array ancestors of path".
		_, err = sourceColl.InsertOne(ctx, bson.D{
			{Key: "_id", Value: "deleted"},
			{Key: "a", Value: bson.D{{Key: "b", Value: bson.A{}}}},
			{Key: "words", Value: "omnibus"},
		})
		require.NoError(t, err)
		_, err = sourceColl.DeleteOne(ctx, bson.D{{Key: "_id", Value: "deleted"}})
		require.NoError(t, err)
		require.True(t, deletes.Next(ctx), "delete event missing: %v", deletes.Err())
		deleteToken := append(bson.Raw(nil), deletes.ResumeToken()...)
		_, err = sourceColl.Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys:    bson.D{{Key: "a.b", Value: 1}, {Key: "words", Value: "text"}},
			Options: options.Index().SetName(index),
		})
		require.NoError(t, err)
		select {
		case drained := <-boundarySaved:
			require.True(t, drained, "checkpoint was requested before the worker's delete completed")
		case <-indexStarted:
			require.FailNow(t, "createIndexes reached target before checkpoint acknowledgement")
		case <-ctx.Done():
			require.FailNow(t, "pre-index checkpoint was never persisted", ctx.Err().Error())
		}
		require.NoError(t, r.Status().Err)
		count, err := targetColl.CountDocuments(ctx, bson.D{})
		require.NoError(t, err)
		require.Zero(t, count, "checkpoint boundary did not drain the insert and delete")
		select {
		case <-indexStarted:
			require.FailNow(t, "createIndexes reached target while checkpoint acknowledgement was held")
		default:
		}
		var saved snapshot
		require.NoError(t, checkpoints.FindOne(ctx, bson.D{{Key: "_id", Value: "boundary"}}).Decode(&saved))
		require.NotNil(t, saved.Repl)
		require.Equal(t, deleteToken, saved.Repl.StartAfter, "boundary must exclude the drained delete")
		require.NotNil(t, saved.Catalog)
		acknowledged.Store(true)
		close(ack)
		select {
		case afterAck := <-indexStarted:
			require.True(t, afterAck, "createIndexes preceded checkpoint acknowledgement")
		case <-ctx.Done():
			require.FailNow(t, "acknowledged index was never sent", ctx.Err().Error())
		}
		awaitSignal(ctx, t, indexSucceeded, "target index did not finish")
		cancelRun()
		awaitSignal(ctx, t, originalDone, "original replicator did not stop")
		require.NoError(t, r.Status().Err)

		// No periodic writer exists here. Only the pre-index snapshot is restored;
		// the post-index in-memory frontier cannot hide an unsafe saved position.
		cat = catalog.NewCatalog(source, target, version)
		require.NoError(t, cat.Recover(saved.Catalog))
		r = NewRepl(source, target, cat, sel.MakeFilter([]string{db + "." + coll}, nil),
			&Options{NumWorkers: 1, Checkpoint: func(saveCtx context.Context) error {
				return persist(saveCtx, "recovered")
			}}, version, false, false)
		require.NoError(t, r.Recover(ctx, saved.Repl))
		recovering.Store(true)
		resumeCtx, cancelResume := context.WithCancel(ctx)
		require.NoError(t, r.Resume(resumeCtx))
		resumedDone := r.Done()
		defer func() {
			cancelResume()
			awaitSignal(ctx, t, resumedDone, "recovered replicator did not stop during cleanup")
		}()
		select {
		case command := <-resumeCommands:
			changeStream := command.Lookup("pipeline").Array().Index(0).
				Document().Lookup("$changeStream").Document()
			startAfter, exact := changeStream.Lookup("startAfter").DocumentOK()
			require.True(t, exact, "fresh recovery lost its exclusive startAfter boundary")
			require.Equal(t, deleteToken, startAfter,
				"fresh recovery must open the real stream at the exclusive boundary")
		case <-ctx.Done():
			require.FailNow(t, "recovered change stream did not open", ctx.Err().Error())
		}
		awaitSignal(ctx, t, indexSucceeded, "fresh catalog did not replay the pending index")
		_, err = sourceColl.InsertOne(ctx, bson.D{
			{Key: "_id", Value: "after-recovery"},
			{Key: "a", Value: bson.D{{Key: "b", Value: 1}}},
			{Key: "words", Value: "omnibus"},
		})
		require.NoError(t, err)
		require.True(t, newWrites.Next(ctx), "new source write was not replicated: %v; repl: %v",
			newWrites.Err(), r.Status().Err)
		cancelResume()
		awaitSignal(ctx, t, resumedDone, "recovered replicator did not stop")
		require.NoError(t, r.Status().Err)
		require.ErrorIs(t, targetColl.FindOne(ctx, bson.D{{Key: "_id", Value: "deleted"}}).Err(),
			mongo.ErrNoDocuments)
		indexCursor, err := targetColl.Indexes().List(ctx)
		require.NoError(t, err)
		var indexes []bson.Raw
		require.NoError(t, indexCursor.All(ctx, &indexes))
		require.Len(t, indexes, 2, "expected only _id and the compound text index")
		for _, spec := range indexes {
			if spec.Lookup("name").StringValue() != index {
				continue
			}
			var key bson.D
			require.NoError(t, bson.Unmarshal(spec.Lookup("key").Document(), &key))
			require.Equal(t, bson.D{
				{Key: "a.b", Value: int32(1)}, {Key: "_fts", Value: "text"}, {Key: "_ftsx", Value: int32(1)},
			}, key)
			var weights bson.D
			require.NoError(t, bson.Unmarshal(spec.Lookup("weights").Document(), &weights))
			require.Equal(t, bson.D{{Key: "words", Value: int32(1)}}, weights)
			return
		}
		require.FailNow(t, "compound text index missing after recovery")
	})
}
