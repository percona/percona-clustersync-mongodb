//go:build integration

package repl //nolint:testpackage // Integration tests exercise the watch-loop boundary.

import (
	"context"
	"sync"
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

//nolint:paralleltest // MongoDB testcontainers are serialized to avoid Docker resource contention.
func TestWatchChangeEvents_TickMatchesServerScannedFrontier(t *testing.T) {
	runReplIntegrationTest(t, func(setupCtx context.Context) {
		mongoURI := newReplTestReplicaSet(setupCtx, t)
		writeClient, err := mongo.Connect(options.Client().ApplyURI(mongoURI).
			SetServerSelectionTimeout(30 * time.Second).SetWriteConcern(writeconcern.Majority()))
		require.NoError(t, err)
		t.Cleanup(func() { disconnectReplTestClient(t, writeClient) })
		require.NoError(t, writeClient.Ping(setupCtx, nil))
		version, err := mdb.Version(setupCtx, writeClient)
		require.NoError(t, err)
		t.Logf("Server under test: %s", version.FullString())

		db := writeClient.Database("watch_frontier_test")
		require.NoError(t, db.CreateCollection(setupCtx, "documents"))
		collection := db.Collection("documents")
		startAt, err := mdb.AdvanceClusterTime(setupCtx, writeClient)
		require.NoError(t, err)

		firstToken := make(chan bson.Raw, 1)
		var capturedToken atomic.Bool
		writesCommitted := make(chan struct{})
		releaseWrites := sync.OnceFunc(func() { close(writesCommitted) })
		defer releaseWrites()
		monitor := &event.CommandMonitor{
			Started: func(ctx context.Context, started *event.CommandStartedEvent) {
				if started.CommandName == "appendOplogNote" {
					// The first tick is already emitted. Hold the watcher here
					// until the whole batch is committed, making it real backlog.
					select {
					case <-writesCommitted:
					case <-ctx.Done():
					}
				}
			},
			Succeeded: func(_ context.Context, succeeded *event.CommandSucceededEvent) {
				// TryNext can consume the empty aggregate firstBatch before
				// issuing a getMore; both responses carry the scanned frontier.
				if (succeeded.CommandName == "aggregate" || succeeded.CommandName == "getMore") &&
					capturedToken.CompareAndSwap(false, true) {
					token, _ := succeeded.Reply.Lookup("cursor", "postBatchResumeToken").DocumentOK()
					firstToken <- append(bson.Raw(nil), token...)
				}
			},
		}
		source, err := mongo.Connect(options.Client().ApplyURI(mongoURI).
			SetServerSelectionTimeout(30 * time.Second).SetMonitor(monitor))
		require.NoError(t, err)
		t.Cleanup(func() { disconnectReplTestClient(t, source) })
		require.NoError(t, source.Ping(setupCtx, nil))
		opts := &Options{ChangeStreamBatchSize: 2}
		opts.applyDefaults()
		r := &Repl{source: source, options: opts}

		require.NoError(t, util.CtxWithTimeout(setupCtx, 30*time.Second, func(watchCtx context.Context) error {
			before, err := mdb.ClusterTime(watchCtx, writeClient)
			require.NoError(t, err)
			changeCh := make(chan *ChangeEvent)
			watchErr := make(chan error, 1)
			go func() {
				watchErr <- r.watchChangeEvents(watchCtx,
					options.ChangeStream().SetStartAtOperationTime(&startAt), changeCh)
			}()
			t.Cleanup(func() {
				require.NoError(t, util.CtxWithTimeout(context.Background(), 10*time.Second,
					func(cleanupCtx context.Context) error {
						for {
							select {
							case <-changeCh:
							case watchError := <-watchErr:
								if watchError == nil || errors.Is(watchError, context.Canceled) {
									return nil
								}

								return errors.Wrap(watchError, "watchChangeEvents stopped")
							case <-cleanupCtx.Done():
								return errors.Wrap(cleanupCtx.Err(), "watchChangeEvents did not stop")
							}
						}
					}))
			})
			receiveChange := func() *ChangeEvent {
				select {
				case change := <-changeCh:
					return change
				case <-watchCtx.Done():
					require.FailNow(t, "watchChangeEvents did not emit the expected event", watchCtx.Err().Error())

					return nil
				}
			}

			first := receiveChange()
			require.Equal(t, OperationType(advanceTimePseudoEvent), first.OperationType)
			after, err := mdb.ClusterTime(watchCtx, writeClient)
			require.NoError(t, err)
			require.False(t, before.After(after))
			require.False(t, first.ClusterTime.Before(startAt))
			require.False(t, first.ClusterTime.After(after))
			select {
			case token := <-firstToken:
				ts, decodeErr := mdb.ResumeTokenTimestamp(token)
				require.NoError(t, decodeErr, "real server PBRT must use the supported KeyString layout")
				require.Equal(t, ts, first.ClusterTime, "first tick must be the server's scanned frontier")
				t.Logf("startAt=%v before=%v first tick=%v after=%v token=%s", startAt, before, first.ClusterTime, after, token)
			case <-watchCtx.Done():
				require.FailNow(t, "first cursor response did not provide a resume token", watchCtx.Err().Error())
			}

			const count = 20
			docs := make([]any, count)
			for i := range count {
				docs[i] = bson.D{{"_id", i}}
			}
			result, err := collection.InsertMany(watchCtx, docs)
			require.NoError(t, err)
			require.True(t, result.Acknowledged)
			require.Len(t, result.InsertedIDs, count)
			releaseWrites()

			var inserts int
			var ticksDuringBacklog []bson.Timestamp
			var lastInsert bson.Timestamp
			previous := first.ClusterTime
			for {
				change := receiveChange()
				require.False(t, change.ClusterTime.Before(previous), "watch timestamps must never regress")
				previous = change.ClusterTime
				if change.OperationType == advanceTimePseudoEvent {
					if inserts == count {
						break
					}
					if inserts > 0 {
						ticksDuringBacklog = append(ticksDuringBacklog, change.ClusterTime)
					}

					continue
				}

				require.Equal(t, Insert, change.OperationType)
				require.EqualValues(t, inserts, change.RawData.Lookup("documentKey", "_id").Int32())
				inserts++
				lastInsert = change.ClusterTime
			}
			require.Equal(t, count, inserts)
			for _, tick := range ticksDuringBacklog {
				require.False(t, tick.After(lastInsert), "tick must not overtake undelivered backlog")
			}

			return nil
		}))
	})
}
