//go:build integration

package repl //nolint:testpackage // Integration tests exercise unexported replication internals.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
)

// An update of a field whose name contains a dot is applied as the whole source document
// (PCSM-139): a replace while the document exists, a delete once it is gone.
//
//nolint:paralleltest // MongoDB testcontainers are serialized to avoid Docker resource contention.
func TestWorker_AddToCurrentBulk_RefetchesDocumentForDottedFieldName(t *testing.T) {
	runReplIntegrationTest(t, func(ctx context.Context) {
		// Given
		var refetchReadConcern atomic.Pointer[bson.Raw]

		source, err := mongo.Connect(
			options.Client().
				ApplyURI(newReplTestReplicaSet(ctx, t)).
				SetMonitor(&event.CommandMonitor{
					Started: func(_ context.Context, started *event.CommandStartedEvent) {
						readConcern, ok := started.Command.Lookup("readConcern").DocumentOK()
						if started.CommandName == "find" && ok && readConcern.Lookup("afterClusterTime").Type != 0 {
							readConcern = append(bson.Raw(nil), readConcern...) // the command buffer is reused
							refetchReadConcern.Store(&readConcern)
						}
					},
				}).
				SetServerSelectionTimeout(30 * time.Second).
				SetReadConcern(readconcern.Majority()).
				SetWriteConcern(writeconcern.Majority()),
		)
		require.NoError(t, err)
		t.Cleanup(func() {
			disconnectReplTestClient(t, source)
		})

		ns := catalog.Namespace{Database: "dotted_field_test", Collection: "documents"}
		coll := source.Database(ns.Database).Collection(ns.Collection)

		// Large enough that the server records the update as a diff, not as a replacement.
		padding := string(make([]byte, 4096))
		_, err = coll.InsertOne(ctx, bson.D{
			{"_id", 1},
			{"padding", padding},
			{"files", bson.D{{"main.ts", "v1"}, {"main", bson.D{{"ts", "nested"}}}}},
		})
		require.NoError(t, err)

		startAt, err := mdb.AdvanceClusterTime(ctx, source)
		require.NoError(t, err)

		_, err = coll.UpdateOne(ctx, bson.D{{"_id", 1}}, mongo.Pipeline{{{"$set", bson.D{{"files", bson.D{
			{"$setField", bson.D{{"field", "main.ts"}, {"input", "$files"}, {"value", "v2"}}},
		}}}}}})
		require.NoError(t, err)

		stream, err := coll.Watch(ctx, mongo.Pipeline{},
			options.ChangeStream().SetShowExpandedEvents(true).SetStartAtOperationTime(&startAt))
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, stream.Close(context.WithoutCancel(ctx)))
		})
		require.True(t, stream.Next(ctx))

		newEvent := func() *routedEvent {
			change := &ChangeEvent{RawData: append(bson.Raw(nil), stream.Current...)}
			require.NoError(t, parseEventHeader(change.RawData, &change.EventHeader))
			require.Equal(t, Update, change.OperationType)

			return &routedEvent{ns: ns, change: change}
		}

		bw := &recordingBulkWriter{}
		w := &worker{source: source, currentBulkWrite: bw}

		// When the document still exists on source
		updateEvent := newEvent()
		require.NoError(t, w.addToCurrentBulk(ctx, updateEvent))

		// Then the refetch is a majority read that waits for the event's clusterTime,
		// so the fetched document cannot be older than the event
		require.NotNil(t, refetchReadConcern.Load())
		assert.Equal(t, "majority", refetchReadConcern.Load().Lookup("level").StringValue())

		seconds, ordinal := refetchReadConcern.Load().Lookup("afterClusterTime").Timestamp()
		afterClusterTime := bson.Timestamp{T: seconds, I: ordinal}
		assert.Equal(t, updateEvent.change.ClusterTime, afterClusterTime)

		// And the event carries the whole source document instead of the ambiguous path
		require.Len(t, bw.replaces, 1)
		assert.Empty(t, bw.updates)

		var replaced, sourceDoc bson.D
		require.NoError(t, bson.Unmarshal(bw.replaces[0].FullDocument, &replaced))
		require.NoError(t, coll.FindOne(ctx, bson.D{{"_id", 1}}).Decode(&sourceDoc))
		assert.Equal(t, sourceDoc, replaced)
		assert.Equal(t, bson.D{{"main.ts", "v2"}, {"main", bson.D{{"ts", "nested"}}}}, replaced[2].Value)

		var documentKey bson.D
		require.NoError(t, bson.Unmarshal(bw.replaces[0].DocumentKey, &documentKey))
		assert.Equal(t, bson.D{{"_id", int32(1)}}, documentKey)

		// When the document has been deleted on source since the update
		_, err = coll.DeleteOne(ctx, bson.D{{"_id", 1}})
		require.NoError(t, err)
		require.NoError(t, w.addToCurrentBulk(ctx, newEvent()))

		// Then the target document is deleted too
		require.Len(t, bw.deletes, 1)
		assert.Equal(t, bson.D{{"_id", int32(1)}}, bw.deletes[0].DocumentKey)
		assert.Len(t, bw.replaces, 1)
		assert.Empty(t, bw.updates)
	})
}
