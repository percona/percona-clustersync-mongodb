//go:build integration

package repl //nolint:testpackage // Integration tests exercise unexported replication internals.

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
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/util"
)

type monitoredWriteOutcome[T any] struct {
	result T
	err    error
}

//nolint:paralleltest // MongoDB testcontainers are serialized to avoid Docker resource contention.
func TestWatchChangeEvents_EmitsMonotonicTimestampsWhenWritesCommitAroundAppendOplogNote(t *testing.T) {
	runReplIntegrationTest(t, func(setupCtx context.Context) {
		// Given
		mongoURI := newReplTestReplicaSet(setupCtx, t)

		writeClient, err := mongo.Connect(
			options.Client().
				ApplyURI(mongoURI).
				SetServerSelectionTimeout(30 * time.Second).
				SetWriteConcern(writeconcern.Majority()),
		)
		require.NoError(t, err)
		t.Cleanup(func() {
			disconnectReplTestClient(t, writeClient)
		})
		require.NoError(t, writeClient.Ping(setupCtx, nil))

		collection := writeClient.Database("watch_change_events_test").Collection("documents")
		insertResult, err := collection.InsertOne(setupCtx, bson.D{{"_id", "seed"}})
		require.NoError(t, err)
		require.True(t, insertResult.Acknowledged)

		startAt, err := mdb.AdvanceClusterTime(setupCtx, writeClient)
		require.NoError(t, err)

		deleteDone := make(chan monitoredWriteOutcome[*mongo.DeleteResult], 1)
		insertDone := make(chan monitoredWriteOutcome[*mongo.InsertOneResult], 1)
		var deleteCalls atomic.Int64
		var insertCalls atomic.Int64
		var appendStartedCalls atomic.Int64
		var appendSucceededCalls atomic.Int64
		commandMonitor := &event.CommandMonitor{
			Started: func(ctx context.Context, started *event.CommandStartedEvent) {
				if started.CommandName != "appendOplogNote" {
					return
				}

				appendNumber := appendStartedCalls.Add(1)
				if appendNumber != 1 {
					return
				}

				deleteCalls.Add(1)
				result, deleteErr := collection.DeleteOne(ctx, bson.D{{"_id", "seed"}})
				deleteDone <- monitoredWriteOutcome[*mongo.DeleteResult]{result: result, err: deleteErr}
			},
			Succeeded: func(ctx context.Context, succeeded *event.CommandSucceededEvent) {
				if succeeded.CommandName != "appendOplogNote" {
					return
				}

				appendNumber := appendSucceededCalls.Add(1)
				if appendNumber != 2 {
					return
				}

				insertCalls.Add(1)
				result, insertErr := collection.InsertOne(ctx, bson.D{{"_id", "overtakes-pending-tick"}})
				insertDone <- monitoredWriteOutcome[*mongo.InsertOneResult]{result: result, err: insertErr}
			},
		}

		sourceClient, err := mongo.Connect(
			options.Client().
				ApplyURI(mongoURI).
				SetServerSelectionTimeout(30 * time.Second).
				SetMonitor(commandMonitor),
		)
		require.NoError(t, err)
		t.Cleanup(func() {
			disconnectReplTestClient(t, sourceClient)
		})
		require.NoError(t, sourceClient.Ping(setupCtx, nil))

		replOptions := &Options{ChangeStreamBatchSize: 1}
		replOptions.applyDefaults()
		r := &Repl{source: sourceClient, options: replOptions}

		require.NoError(t, util.CtxWithTimeout(setupCtx, 20*time.Second, func(watchCtx context.Context) error {
			changeEvents := make(chan *ChangeEvent)
			watchErr := make(chan error, 1)
			go func() {
				watchErr <- r.watchChangeEvents(
					watchCtx,
					options.ChangeStream().SetStartAtOperationTime(&startAt),
					changeEvents,
				)
			}()
			t.Cleanup(func() {
				require.NoError(t, util.CtxWithTimeout(context.Background(), 10*time.Second,
					func(cleanupCtx context.Context) error {
						for {
							select {
							case <-changeEvents:
							case watchError := <-watchErr:
								if watchError == nil || errors.Is(watchError, context.Canceled) {
									return nil
								}

								return errors.Wrap(watchError, "watchChangeEvents stopped")
							case <-cleanupCtx.Done():
								return errors.Wrap(cleanupCtx.Err(),
									"watchChangeEvents did not stop after cancellation")
							}
						}
					},
				))
			})

			// When
			events := make([]*ChangeEvent, 0, 4)
			for len(events) < cap(events) {
				select {
				case change := <-changeEvents:
					events = append(events, change)
				case <-watchCtx.Done():
					require.FailNow(t, "watchChangeEvents did not emit the expected event sequence",
						watchCtx.Err().Error())
				}
			}

			// Then
			var deleteOutcome monitoredWriteOutcome[*mongo.DeleteResult]
			select {
			case deleteOutcome = <-deleteDone:
			case <-watchCtx.Done():
				require.FailNow(t, "first appendOplogNote callback did not delete the seeded document",
					watchCtx.Err().Error())
			}
			require.NoError(t, deleteOutcome.err)
			require.NotNil(t, deleteOutcome.result)
			require.True(t, deleteOutcome.result.Acknowledged)
			require.EqualValues(t, 1, deleteOutcome.result.DeletedCount)

			var insertOutcome monitoredWriteOutcome[*mongo.InsertOneResult]
			select {
			case insertOutcome = <-insertDone:
			case <-watchCtx.Done():
				require.FailNow(t, "second appendOplogNote callback did not insert the overtaking document",
					watchCtx.Err().Error())
			}
			require.NoError(t, insertOutcome.err)
			require.NotNil(t, insertOutcome.result)
			require.True(t, insertOutcome.result.Acknowledged)

			require.EqualValues(t, 1, deleteCalls.Load())
			require.EqualValues(t, 1, insertCalls.Load())
			require.Equal(t, Delete, events[0].OperationType)
			require.Equal(t, OperationType(advanceTimePseudoEvent), events[1].OperationType)
			require.Equal(t, Insert, events[2].OperationType)
			require.Equal(t, OperationType(advanceTimePseudoEvent), events[3].OperationType)
			for i := 1; i < len(events); i++ {
				require.Falsef(
					t,
					events[i-1].ClusterTime.After(events[i].ClusterTime),
					"event %d timestamp %v was followed by older event %d timestamp %v",
					i-1,
					events[i-1].ClusterTime,
					i,
					events[i].ClusterTime,
				)
			}

			return nil
		}))
	})
}
