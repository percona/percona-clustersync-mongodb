//go:build integration

package repl //nolint:testpackage // Integration tests exercise unexported replication internals.

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/sel"
)

const (
	cancelTestDB     = "repl_cancel_db"
	cancelTestColl   = "docs"
	cancelTestMarker = "parked-by-cancel"
)

// targetWriteGate parks the first target data write carrying the marker inside
// its CommandStarted hook until that write's own context is canceled (the
// fence under test) or the gate is released (failure bound), and signals a
// later succeeded data write carrying the marker.
type targetWriteGate struct {
	marker  []byte
	mu      sync.Mutex
	parked  bool
	started map[int64]struct{}

	armed     chan struct{}
	release   chan struct{}
	relOnce   sync.Once
	parkedEnd chan bool // true when the parked hook returned on its ctx
	succeeded chan struct{}
}

func newTargetWriteGate(marker string) *targetWriteGate {
	return &targetWriteGate{
		marker:    []byte(marker),
		started:   make(map[int64]struct{}),
		armed:     make(chan struct{}),
		release:   make(chan struct{}),
		parkedEnd: make(chan bool, 1),
		succeeded: make(chan struct{}, 1),
	}
}

func (g *targetWriteGate) releaseAll() { g.relOnce.Do(func() { close(g.release) }) }

func isCancelTestDataWrite(cmd *event.CommandStartedEvent) bool {
	switch cmd.CommandName {
	case "bulkWrite":
		return true
	case "insert", "update", "delete":
		return cmd.DatabaseName == cancelTestDB
	default:
		return false
	}
}

func (g *targetWriteGate) monitor() *event.CommandMonitor {
	return &event.CommandMonitor{
		Started: func(opCtx context.Context, cmd *event.CommandStartedEvent) {
			if !isCancelTestDataWrite(cmd) || !bytes.Contains(cmd.Command, g.marker) {
				return
			}

			g.mu.Lock()
			g.started[cmd.RequestID] = struct{}{}
			park := !g.parked
			g.parked = true
			g.mu.Unlock()

			if !park {
				return
			}

			close(g.armed)

			select {
			case <-opCtx.Done():
				g.parkedEnd <- true
			case <-g.release:
				g.parkedEnd <- false
			}
		},
		Succeeded: func(_ context.Context, evt *event.CommandSucceededEvent) {
			g.mu.Lock()
			_, carries := g.started[evt.RequestID]
			g.mu.Unlock()

			if !carries {
				return
			}

			select {
			case g.succeeded <- struct{}{}:
			default:
			}
		},
	}
}

func connectReplTestClient(
	ctx context.Context, t *testing.T, uri string, mon *event.CommandMonitor,
) *mongo.Client {
	t.Helper()

	opts := options.Client().
		ApplyURI(uri).
		SetServerSelectionTimeout(30 * time.Second).
		SetRetryWrites(false)
	if mon != nil {
		opts.SetMonitor(mon)
	}

	client, err := mongo.Connect(opts)
	require.NoError(t, err)
	t.Cleanup(func() { disconnectReplTestClient(t, client) })
	require.NoError(t, client.Ping(ctx, nil))

	return client
}

func awaitSignal[T any](ctx context.Context, t *testing.T, ch <-chan T, msg string) {
	t.Helper()

	select {
	case <-ch:
	case <-ctx.Done():
		require.FailNow(t, msg, ctx.Err().Error())
	}
}

// TestRepl_CancelSuspendsAtFloorAndResumeReplays pins the repl half of the
// fence: canceling the run context ends a parked target write before it
// reaches the wire, the run settles paused at its inclusive floor with no
// error, and Resume replays the abandoned event exactly once.
//
//nolint:paralleltest // MongoDB testcontainers are serialized to avoid Docker resource contention.
func TestRepl_CancelSuspendsAtFloorAndResumeReplays(t *testing.T) {
	runReplIntegrationTest(t, func(ctx context.Context) {
		// Given: a source replica set and a separate target whose first data
		// write carrying the marker is parked in flight.
		source := connectReplTestClient(ctx, t, newReplTestReplicaSet(ctx, t), nil)
		gate := newTargetWriteGate(cancelTestMarker)
		defer gate.releaseAll()
		target := connectReplTestClient(ctx, t, newReplTestReplicaSet(ctx, t), gate.monitor())

		sourceVersion, err := mdb.Version(ctx, source)
		require.NoError(t, err)
		r := NewRepl(source, target, catalog.NewCatalog(source, target, sourceVersion),
			sel.AllowAllFilter, &Options{NumWorkers: 1}, sourceVersion, false, false)

		startAt, err := mdb.AdvanceClusterTime(ctx, source)
		require.NoError(t, err)

		runCtx, cancelRun := context.WithCancel(ctx)
		defer cancelRun()
		require.NoError(t, r.Start(runCtx, startAt))
		done := r.Done()

		_, err = source.Database(cancelTestDB).Collection(cancelTestColl).
			InsertOne(ctx, bson.D{{Key: "_id", Value: cancelTestMarker}})
		require.NoError(t, err)
		awaitSignal(ctx, t, gate.armed, "the marker write was never issued to the target")

		// When: the run is canceled while the write is parked.
		cancelRun()
		awaitSignal(ctx, t, done, "the run did not stop after cancellation")

		// Then: the parked write was ended by its own context, the run settled
		// paused with no error, and nothing landed on the target.
		select {
		case byCancel := <-gate.parkedEnd:
			require.True(t, byCancel, "cancellation did not reach the parked write's context")
		case <-ctx.Done():
			require.FailNow(t, "parked write never returned", ctx.Err().Error())
		}

		status := r.Status()
		require.True(t, status.IsPaused())
		require.False(t, status.Pausing)
		require.NoError(t, status.Err)
		require.False(t, status.CheckpointOpTime.Before(startAt))

		targetColl := target.Database(cancelTestDB).Collection(cancelTestColl)
		count, err := targetColl.CountDocuments(ctx, bson.D{{Key: "_id", Value: cancelTestMarker}})
		require.NoError(t, err)
		require.Zero(t, count, "a canceled run drained its abandoned write to the target")

		require.ErrorContains(t, r.Pause(ctx), "already paused")

		// And: resuming replays from the inclusive floor and the write lands once.
		resumeCtx, cancelResume := context.WithCancel(ctx)
		defer cancelResume()
		require.NoError(t, r.Resume(resumeCtx))
		resumedDone := r.Done()

		awaitSignal(ctx, t, gate.succeeded, "the resumed run never replayed the marker write")
		count, err = targetColl.CountDocuments(ctx, bson.D{{Key: "_id", Value: cancelTestMarker}})
		require.NoError(t, err)
		require.Equal(t, int64(1), count, "resume did not replay the abandoned write exactly once")

		cancelResume()
		awaitSignal(ctx, t, resumedDone, "the resumed run did not stop after cancellation")
		resumedStatus := r.Status()
		require.True(t, resumedStatus.IsPaused())
		require.NoError(t, resumedStatus.Err)
	})
}
