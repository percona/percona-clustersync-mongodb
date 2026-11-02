//go:build integration

package repl //nolint:testpackage // Integration tests exercise the replication lifecycle.

import (
	"context"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/sel"
	"github.com/percona/percona-clustersync-mongodb/util"
)

//nolint:paralleltest // Each case owns replica sets and a server failpoint; keep resource use serial.
func TestRepl_Start_stopsOnHistoryLost(t *testing.T) {
	for _, command := range []string{"aggregate", "getMore"} {
		t.Run(command, func(t *testing.T) {
			runReplIntegrationTest(t, func(ctx context.Context) {
				// Given: only the monitored source client can trigger the one-shot failure.
				sourceURI := newReplTestReplicaSet(ctx, t)
				targetURI := newReplTestReplicaSet(ctx, t)
				connect := func(clientCtx context.Context, opts *options.ClientOptions) *mongo.Client {
					t.Helper()
					client, err := mongo.Connect(opts)
					require.NoError(t, err)
					t.Cleanup(func() {
						assert.NoError(t, util.CtxWithTimeout(context.WithoutCancel(clientCtx),
							10*time.Second, client.Disconnect))
					})
					require.NoError(t, client.Ping(clientCtx, nil))

					return client
				}
				admin := connect(ctx, options.Client().ApplyURI(sourceURI))
				target := connect(ctx, options.Client().ApplyURI(targetURI))
				appName := "pcsm-history-lost-" + bson.NewObjectID().Hex()

				var mu sync.Mutex
				counts := map[string]int{"aggregate": 0, "getMore": 0}
				var countsAtFailure map[string]int
				var wireErr error
				var failedCommand string
				var aggregateRequest, getMoreRequest, failedRequest int64
				var openedCursor, readCursor int64
				retried := make(chan string, 1)
				monitor := &event.CommandMonitor{
					Started: func(_ context.Context, evt *event.CommandStartedEvent) {
						if evt.CommandName != "aggregate" && evt.CommandName != "getMore" {
							return
						}
						mu.Lock()
						defer mu.Unlock()
						counts[evt.CommandName]++
						if countsAtFailure != nil {
							select {
							case retried <- evt.CommandName:
							default:
							}
						}
						if evt.CommandName == "aggregate" {
							aggregateRequest = evt.RequestID
						} else {
							getMoreRequest = evt.RequestID
							readCursor, _ = evt.Command.Lookup("getMore").Int64OK()
						}
					},
					Succeeded: func(_ context.Context, evt *event.CommandSucceededEvent) {
						if evt.CommandName != "aggregate" {
							return
						}
						mu.Lock()
						defer mu.Unlock()
						if evt.RequestID == aggregateRequest {
							openedCursor, _ = evt.Reply.Lookup("cursor", "id").Int64OK()
						}
					},
					Failed: func(_ context.Context, evt *event.CommandFailedEvent) {
						if evt.CommandName != "aggregate" && evt.CommandName != "getMore" {
							return
						}
						mu.Lock()
						defer mu.Unlock()
						if countsAtFailure == nil {
							wireErr = evt.Failure
							failedCommand = evt.CommandName
							failedRequest = evt.RequestID
							countsAtFailure = maps.Clone(counts)
						}
					},
				}
				source := connect(ctx, options.Client().ApplyURI(sourceURI).
					SetAppName(appName).SetMonitor(monitor))
				sourceVersion, err := mdb.Version(ctx, admin)
				require.NoError(t, err)
				startAt, err := mdb.AdvanceClusterTime(ctx, admin)
				require.NoError(t, err)
				require.False(t, startAt.IsZero())
				r := NewRepl(source, target,
					catalog.NewCatalog(source, target, sourceVersion),
					sel.AllowAllFilter, &Options{NumWorkers: 1}, sourceVersion, false, false)
				runCtx, cancelRun := context.WithCancel(ctx)
				started := false
				t.Cleanup(func() {
					cancelRun()
					if started {
						assert.NoError(t, util.CtxWithTimeout(context.WithoutCancel(ctx), 10*time.Second,
							func(cleanupCtx context.Context) error {
								select {
								case <-r.Done():
									return nil
								case <-cleanupCtx.Done():
									return errors.Wrap(cleanupCtx.Err(), "join replication")
								}
							}))
					}
					assert.NoError(t, util.CtxWithTimeout(context.WithoutCancel(ctx), 10*time.Second,
						func(cleanupCtx context.Context) error {
							return admin.Database("admin").RunCommand(cleanupCtx, bson.D{
								{Key: "configureFailPoint", Value: "failCommand"},
								{Key: "mode", Value: "off"},
							}).Err()
						}))
				})
				require.NoError(t, admin.Database("admin").RunCommand(ctx, bson.D{
					{Key: "configureFailPoint", Value: "failCommand"},
					{Key: "mode", Value: bson.D{{Key: "times", Value: 1}}},
					{Key: "data", Value: bson.D{
						{Key: "failCommands", Value: bson.A{command}},
						{Key: "appName", Value: appName},
						{Key: "errorCode", Value: 286},
						{Key: "errorLabels", Value: bson.A{}},
					}},
				}).Err())

				// When: drive the real stream opener, reader, retry loop and terminal status.
				require.NoError(t, r.Start(runCtx, startAt))
				started = true
				guardCtx, cancelGuard := context.WithTimeout(ctx, 30*time.Second)
				defer cancelGuard()
				select {
				case <-r.Done():
				case retry := <-retried:
					require.FailNow(t, "retried after history loss", retry)
				case <-guardCtx.Done():
					require.FailNow(t, "replication did not terminate after history loss", guardCtx.Err().Error())
				}

				// Then: Done joins the watcher. Neither cancellation nor a timeout ends the run.
				require.NoError(t, runCtx.Err())
				require.ErrorIs(t, r.Status().Err, ErrOplogHistoryLost)
				mu.Lock()
				finalCounts := maps.Clone(counts)
				failureCounts := maps.Clone(countsAtFailure)
				failure := wireErr
				failed := failedCommand
				openID, readID := openedCursor, readCursor
				openRequest, readRequest, failureRequest := aggregateRequest, getMoreRequest, failedRequest
				mu.Unlock()
				// Also catches a retry racing with Done in the select above.
				select {
				case retry := <-retried:
					require.FailNow(t, "retried after history loss", retry)
				default:
				}
				require.Equal(t, failureCounts, finalCounts, "command counts changed after failure and watcher join")
				require.Equal(t, command, failed)
				// Command monitoring exposes the wire error before mongo translates it.
				var commandErr driver.Error
				require.ErrorAs(t, failure, &commandErr)
				require.EqualValues(t, 286, commandErr.Code)
				require.Equal(t, "ChangeStreamHistoryLost", commandErr.Name)
				require.Empty(t, commandErr.Labels)
				if command == "getMore" {
					require.NotZero(t, openID, "aggregate must return a live cursor")
					require.Equal(t, openID, readID, "failed getMore must read the opened stream")
					require.Equal(t, readRequest, failureRequest)
					require.Equal(t, map[string]int{"aggregate": 1, "getMore": 1}, finalCounts)
				} else {
					require.Equal(t, openRequest, failureRequest)
					require.Equal(t, map[string]int{"aggregate": 1, "getMore": 0}, finalCounts)
				}
			})
		})
	}
}
