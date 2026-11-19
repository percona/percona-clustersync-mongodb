//go:build integration

package repl //nolint:testpackage // Exercises the worker's bulk writers.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
)

// TestBulkWriteRetriesUnlabeledTransientError returns a PrimarySteppedDown
// write concern error without the RetryableWriteError label, so the driver
// does not retry it. The writer must back off and retry instead of stopping.
//
//nolint:paralleltest // Shares the integration replica set across cases.
func TestBulkWriteRetriesUnlabeledTransientError(t *testing.T) {
	runReplIntegrationTest(t, func(ctx context.Context) {
		uri := newReplTestReplicaSet(ctx, t)
		// Counts the write commands, to prove the fail point fired and the
		// writer retried.
		var attempts atomic.Int32
		client, err := mongo.Connect(options.Client().ApplyURI(uri).
			SetServerSelectionTimeout(30 * time.Second).
			SetMonitor(&event.CommandMonitor{
				Started: func(_ context.Context, e *event.CommandStartedEvent) {
					if e.CommandName == "update" || e.CommandName == "bulkWrite" {
						attempts.Add(1)
					}
				},
			}))
		require.NoError(t, err)
		defer disconnectReplTestClient(t, client)

		version, err := mdb.Version(ctx, client)
		require.NoError(t, err)

		admin := client.Database("admin")

		for _, tt := range []struct {
			name           string
			collectionBulk bool
		}{
			{"collection bulk write", true},
			{"client bulk write", false},
		} {
			t.Run(tt.name, func(t *testing.T) {
				if !tt.collectionBulk && !mdb.Support(version).ClientBulkWrite() {
					t.Skip("client bulk write needs MongoDB 8.0+")
				}

				db := "pcsm339_" + bson.NewObjectID().Hex()
				defer func() { require.NoError(t, client.Database(db).Drop(ctx)) }()

				require.NoError(t, admin.RunCommand(ctx, bson.D{
					{"configureFailPoint", "failCommand"},
					{"mode", bson.D{{"times", 1}}},
					{"data", bson.D{
						{"failCommands", bson.A{"bulkWrite", "update"}},
						{"writeConcernError", bson.D{
							{"code", 189}, {"codeName", "PrimarySteppedDown"}, {"errmsg", "injected"},
						}},
						{"errorLabels", bson.A{}},
					}},
				}).Err())
				defer func() {
					require.NoError(t, admin.RunCommand(context.WithoutCancel(ctx), bson.D{
						{"configureFailPoint", "failCommand"}, {"mode", "off"},
					}).Err())
				}()

				opts := &Options{}
				opts.applyDefaults()
				w := newWorker(0, opts, client, client, tt.collectionBulk, false, make(chan error, 1))
				bw := w.newBulkWriter()
				id := bson.NewObjectID()
				raw, err := bson.Marshal(bson.D{{"_id", id}})
				require.NoError(t, err)
				bw.Insert(catalog.Namespace{Database: db, Collection: "data"},
					&InsertEvent{DocumentKey: bson.D{{"_id", id}}, FullDocument: raw})

				attempts.Store(0)
				size, err := bw.Do(ctx, client)
				require.NoError(t, err)
				require.Equal(t, 1, size)
				require.EqualValues(t, 2, attempts.Load(), "fail point must fire and the writer must retry")
			})
		}
	})
}
