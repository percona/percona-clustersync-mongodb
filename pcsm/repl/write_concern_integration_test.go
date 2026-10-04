//go:build integration

package repl //nolint:testpackage // Exercises worker factories and fallback writes.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
)

//nolint:paralleltest // Shares the integration replica set across cases.
func TestReplicationWriteConcernCommands(t *testing.T) {
	runReplIntegrationTest(t, func(ctx context.Context) {
		uri := newReplTestReplicaSet(ctx, t)
		for _, tt := range []struct {
			name string
			wc   *writeconcern.WriteConcern
			want any
		}{
			{"default majority", nil, "majority"},
			{"explicit one", writeconcern.W1(), int32(1)},
		} {
			t.Run(tt.name, func(t *testing.T) {
				commands := make(chan bson.Raw, 100)
				client, err := mongo.Connect(options.Client().ApplyURI(uri).
					SetServerSelectionTimeout(30 * time.Second).
					SetWriteConcern(writeconcern.W1()).
					SetMonitor(&event.CommandMonitor{
						Started: func(_ context.Context, e *event.CommandStartedEvent) {
							switch e.CommandName {
							case "bulkWrite", "insert", "update", "delete":
								commands <- append(bson.Raw(nil), e.Command...)
							}
						},
					}))
				require.NoError(t, err)
				defer disconnectReplTestClient(t, client)
				version, err := mdb.Version(ctx, client)
				require.NoError(t, err)

				checkCommands := func(minCount int) {
					t.Helper()
					count := 0
					for {
						select {
						case command := <-commands:
							count++
							var concern struct {
								W any `bson:"w"`
							}
							require.NoError(t, bson.Unmarshal(command.Lookup("writeConcern").Document(), &concern))
							assert.Equal(t, tt.want, concern.W)
						default:
							require.GreaterOrEqual(t, count, minCount, "expected target data write commands")

							return
						}
					}
				}

				db := "pcsm311_" + bson.NewObjectID().Hex()
				defer func() { require.NoError(t, client.Database(db).Drop(ctx)) }()
				ns := catalog.Namespace{Database: db, Collection: "data"}
				opts := &Options{WriteConcern: tt.wc}
				opts.applyDefaults()

				for _, collectionBulk := range []bool{true, false} {
					if !collectionBulk && !mdb.Support(version).ClientBulkWrite() {
						continue
					}
					w := newWorker(0, opts, client, client, collectionBulk, false, make(chan error, 1))
					for i := range 2 {
						bw := w.newBulkWriter()
						id := bson.NewObjectID()
						raw, marshalErr := bson.Marshal(bson.D{{"_id", id}, {"batch", i}, {"arr", bson.A{nil}}})
						require.NoError(t, marshalErr)
						bw.Insert(ns, &InsertEvent{DocumentKey: bson.D{{"_id", id}}, FullDocument: raw})
						size, writeErr := bw.Do(ctx, client)
						require.NoError(t, writeErr)
						require.Equal(t, 1, size)
						checkCommands(1)

						// A mismatching upsert filter forces duplicate _id
						// recovery through this writer's delete/insert path.
						bw = w.newBulkWriter()
						bw.Insert(ns, &InsertEvent{
							DocumentKey:  bson.D{{"_id", id}, {"missing", true}},
							FullDocument: raw,
						})
						size, writeErr = bw.Do(ctx, client)
						require.NoError(t, writeErr)
						require.Equal(t, 1, size)
						checkCommands(3)

						// The null array element makes this delta fail with
						// PathNotViable, forcing the writer's refetch/replace
						// path. Source aliases target here: only the emitted
						// target write concern is under test.
						bw = w.newBulkWriter()
						bw.Update(ns, &UpdateEvent{
							DocumentKey: bson.D{{"_id", id}},
							UpdateDescription: UpdateDescription{
								UpdatedFields: bson.D{{"arr.0.x", 1}},
							},
						})
						size, writeErr = bw.Do(ctx, client)
						require.NoError(t, writeErr)
						require.Equal(t, 1, size)
						checkCommands(2)
					}
				}

				targetColl := client.Database(db).Collection("fallback",
					options.Collection().SetWriteConcern(opts.WriteConcern))
				doc := bson.D{{"_id", "fallback"}, {"value", "source"}}
				require.NoError(t, handleDuplicateKeyError(ctx, targetColl, doc, false))
				checkCommands(2)

				// Source and target collections differ so the refetch must
				// replace target content, then delete it after source removal.
				sourceNS := catalog.Namespace{Database: db, Collection: "source"}
				sourceColl := client.Database(db).Collection(sourceNS.Collection)
				_, err = sourceColl.InsertOne(ctx, doc)
				require.NoError(t, err)
				<-commands // Source setup is not a target replication write.
				filter := bson.D{{"_id", "fallback"}}
				require.NoError(t, handleRecoverableUpdateError(ctx, client, targetColl, sourceNS, filter, false))
				checkCommands(1)
				_, err = sourceColl.DeleteOne(ctx, filter)
				require.NoError(t, err)
				<-commands
				require.NoError(t, handleRecoverableUpdateError(ctx, client, targetColl, sourceNS, filter, false))
				checkCommands(1)
				count, err := targetColl.CountDocuments(ctx, filter)
				require.NoError(t, err)
				assert.Zero(t, count)
			})
		}
	})
}
