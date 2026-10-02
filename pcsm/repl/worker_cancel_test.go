package repl //nolint

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
)

// blockingGate hands out bulkWriters whose Do parks until the write's context
// is canceled or the gate is released, and counts every Do call. Writers made
// by one gate share its channels so a test can observe any of them. It is
// opt-in: the shared mockBulkWriter keeps completing Do immediately.
type blockingGate struct {
	started chan struct{} // one send per Do that parked
	release chan struct{}
	doCalls atomic.Int32
}

func newBlockingGate() *blockingGate {
	return &blockingGate{started: make(chan struct{}, 16), release: make(chan struct{})}
}

func (g *blockingGate) writer() *blockingBulkWriter {
	return &blockingBulkWriter{gate: g, fullAt: 1}
}

func (g *blockingGate) releaseAll() { close(g.release) }

func (g *blockingGate) awaitParked(t *testing.T) {
	t.Helper()

	select {
	case <-g.started:
	case <-time.After(barrierTimeout):
		t.Fatal("no bulk write was parked")
	}
}

type blockingBulkWriter struct {
	gate   *blockingGate
	count  int
	fullAt int
}

func (b *blockingBulkWriter) Full() bool               { return b.fullAt > 0 && b.count >= b.fullAt }
func (b *blockingBulkWriter) Empty() bool              { return b.count == 0 }
func (b *blockingBulkWriter) WouldOverflow(_ int) bool { return false }

func (b *blockingBulkWriter) Do(ctx context.Context, _ *mongo.Client) (int, error) {
	b.gate.doCalls.Add(1)

	n := b.count
	b.count = 0

	b.gate.started <- struct{}{}

	select {
	case <-ctx.Done():
		return 0, ctx.Err() //nolint:wrapcheck // mirrors the driver, which returns the context error unwrapped
	case <-b.gate.release:
		return n, nil
	}
}

func (b *blockingBulkWriter) Insert(_ catalog.Namespace, _ *InsertEvent)   { b.count++ }
func (b *blockingBulkWriter) Update(_ catalog.Namespace, _ *UpdateEvent)   { b.count++ }
func (b *blockingBulkWriter) Replace(_ catalog.Namespace, _ *ReplaceEvent) { b.count++ }
func (b *blockingBulkWriter) Delete(_ catalog.Namespace, _ *DeleteEvent)   { b.count++ }

func awaitWorkerExit(t *testing.T, w *worker) {
	t.Helper()

	select {
	case <-w.done:
	case <-time.After(barrierTimeout):
		t.Fatal("worker did not exit after cancellation")
	}
}

// TestCancel_AbandonsParkedAndQueuedBulks pins the fence at the pool level:
// canceling the pool context ends a parked bulk write, never executes the
// bulk queued behind it, records the abandonment in writerErr, and reports
// nothing on the pool error channel.
func TestCancel_AbandonsParkedAndQueuedBulks(t *testing.T) {
	t.Parallel()

	gate := newBlockingGate()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := makeTestPoolLiveWithParent(t, ctx, []bulkWriter{gate.writer()},
		func() bulkWriter { return gate.writer() })
	w := pool.workers[0]

	// Given: the first bulk is parked in Do and a sealed bulk is queued behind it.
	w.routedEventCh <- makeInsertEventWithTS("parked", bson.Timestamp{T: 10, I: 1})
	gate.awaitParked(t)

	w.pendingBulkCh <- &pendingBulk{
		writer: gate.writer(), checkpoint: bson.Timestamp{T: 20, I: 1}, events: 1,
	}

	// When: the run is canceled.
	cancel()

	// Then: the worker exits without draining.
	awaitWorkerExit(t, w)

	assert.Equal(t, int32(1), gate.doCalls.Load(), "the queued bulk must never be executed")
	assert.Len(t, w.pendingBulkCh, 1, "the queued bulk must stay abandoned in the queue")
	require.Error(t, w.writerErr)
	assert.Contains(t, w.writerErr.Error(), "abandoned")

	select {
	case err := <-pool.Err():
		t.Fatalf("cancellation was reported as a worker failure: %v", err)
	default:
	}
}

// TestCancel_BarrierOverAbandonedBulkReportsError verifies that a barrier that
// overlaps cancellation never reports a successful drain: whichever arm the
// worker takes first, the parked bulk is abandoned and the barrier fails.
func TestCancel_BarrierOverAbandonedBulkReportsError(t *testing.T) {
	t.Parallel()

	gate := newBlockingGate()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := makeTestPoolLiveWithParent(t, ctx, []bulkWriter{gate.writer()},
		func() bulkWriter { return gate.writer() })
	w := pool.workers[0]

	// Given: a bulk is parked in Do and a barrier is requested.
	w.routedEventCh <- makeInsertEventWithTS("parked", bson.Timestamp{T: 10, I: 1})
	gate.awaitParked(t)

	barrierErr := make(chan error, 1)
	go func() { barrierErr <- pool.Barrier() }()

	// When: the run is canceled while the barrier waits for the writer.
	cancel()

	// Then: the barrier reports a failure, never a successful drain.
	select {
	case err := <-barrierErr:
		require.Error(t, err, "a barrier over an abandoned bulk must not report success")
	case <-time.After(barrierTimeout):
		t.Fatal("barrier did not return after cancellation")
	}

	awaitWorkerExit(t, w)
	assert.Equal(t, int32(1), gate.doCalls.Load())
}

// TestRoute_ReturnsWhenWorkerExited verifies that routing to a worker that
// has exited returns once its queue is full instead of blocking the
// dispatcher forever.
func TestRoute_ReturnsWhenWorkerExited(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := makeTestPoolLiveWithParent(t, ctx, []bulkWriter{&mockBulkWriter{}},
		func() bulkWriter { return &mockBulkWriter{} })
	w := pool.workers[0]

	// Given: the worker has exited.
	cancel()
	awaitWorkerExit(t, w)

	// When: more events than its queue holds are routed to it.
	routed := make(chan struct{})
	go func() {
		defer close(routed)

		for i := range cap(w.routedEventCh) + 1 {
			re := makeInsertEventWithTS(fmt.Sprintf("doc-%d", i), bson.Timestamp{T: 1, I: 1})
			pool.Route(re.change, re.ns)
		}
	}()

	// Then: Route returns instead of blocking on the dead worker's full queue.
	select {
	case <-routed:
	case <-time.After(barrierTimeout):
		t.Fatal("Route blocked on an exited worker")
	}
}

// TestCancel_CheckpointFloorStaysAtAbandonedWork verifies the inclusive resume
// floor after cancellation: an abandoned bulk never advances it.
func TestCancel_CheckpointFloorStaysAtAbandonedWork(t *testing.T) {
	t.Parallel()

	t.Run("no commit keeps the first routed timestamp", func(t *testing.T) {
		t.Parallel()

		gate := newBlockingGate()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pool := makeTestPoolLiveWithParent(t, ctx, []bulkWriter{gate.writer()},
			func() bulkWriter { return gate.writer() })

		first := bson.Timestamp{T: 100, I: 1}
		re := makeInsertEventWithTS("parked", first)
		pool.Route(re.change, re.ns)
		gate.awaitParked(t)

		cancel()
		awaitWorkerExit(t, pool.workers[0])

		assert.Equal(t, first, pool.Checkpoint())
	})

	t.Run("a commit then an abandoned bulk keeps the commit", func(t *testing.T) {
		t.Parallel()

		gate := newBlockingGate()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pool := makeTestPoolLiveWithParent(t, ctx, []bulkWriter{&mockBulkWriter{fullAt: 1}},
			func() bulkWriter { return gate.writer() })

		committed := bson.Timestamp{T: 100, I: 1}
		abandoned := bson.Timestamp{T: 200, I: 1}

		first := makeInsertEventWithTS("committed", committed)
		pool.Route(first.change, first.ns)

		second := makeInsertEventWithTS("parked", abandoned)
		pool.Route(second.change, second.ns)
		// The writer is sequential: a parked second Do proves the first committed.
		gate.awaitParked(t)

		cancel()
		awaitWorkerExit(t, pool.workers[0])

		assert.Equal(t, committed, pool.Checkpoint())
	})
}

// TestRelease_DrainsParkedBulk verifies the other half of the contract: with
// the pool context alive (an operator pause), a parked bulk still drains once
// the target answers, and the barrier succeeds.
func TestRelease_DrainsParkedBulk(t *testing.T) {
	t.Parallel()

	gate := newBlockingGate()
	pool := makeTestPoolLiveWithParent(t, context.Background(), []bulkWriter{gate.writer()},
		func() bulkWriter { return gate.writer() })
	w := pool.workers[0]

	// Given: a bulk is parked in Do and another event waits in the queue.
	parked := bson.Timestamp{T: 10, I: 1}
	drained := bson.Timestamp{T: 20, I: 1}
	w.routedEventCh <- makeInsertEventWithTS("parked", parked)
	gate.awaitParked(t)
	w.routedEventCh <- makeInsertEventWithTS("drained", drained)

	barrierErr := make(chan error, 1)
	go func() { barrierErr <- pool.Barrier() }()

	// When: the parked write is released rather than canceled.
	gate.releaseAll()

	// Then: the barrier drains both bulks.
	select {
	case err := <-barrierErr:
		require.NoError(t, err)
	case <-time.After(barrierTimeout):
		t.Fatal("barrier did not complete after the parked write was released")
	}

	assert.Equal(t, int32(2), gate.doCalls.Load())
	assert.Equal(t, int64(2), pool.TotalEventsApplied())

	committed := w.lastCommittedTS.Load()
	require.NotNil(t, committed)
	assert.Equal(t, drained, *committed)

	pool.ReleaseBarrier()
}
