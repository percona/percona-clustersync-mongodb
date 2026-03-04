package repl //nolint

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/errors"
)

// makeInsertEventWithTS creates a valid routedEvent with an insert ChangeEvent
// and a specific ClusterTime. Used for checkpoint verification tests.
func makeInsertEventWithTS(id string, ts bson.Timestamp) *routedEvent {
	event := makeInsertEvent(id)
	event.change.ClusterTime = ts

	return event
}

// TestSubmit_EmptyBulkIsNoop verifies that submit() on an empty bulk is a
// no-op: returns true without queuing anything to bulkQueue.
func TestSubmit_EmptyBulkIsNoop(t *testing.T) {
	t.Parallel()

	w := &worker{
		currentBulkWrite: &mockBulkWriter{},
		pendingBulkCh:    make(chan *pendingBulk, 1),
		writerDone:       make(chan struct{}),
	}

	ok := w.submit()

	assert.True(t, ok, "submit on empty bulk should succeed")
	assert.Empty(t, w.pendingBulkCh, "nothing should be queued")
}

// TestSubmit_SealsBulkAndQueues verifies that submit() seals the current
// bulkWriter into a pendingBulk with the correct checkpoint timestamp,
// queues it, and replaces w.bulkWrite with a fresh writer.
func TestSubmit_SealsBulkAndQueues(t *testing.T) {
	t.Parallel()

	mock := &mockBulkWriter{count: 3}
	ts := bson.Timestamp{T: 10, I: 1}

	w := &worker{
		currentBulkWrite: mock,
		pendingTS:        ts,
		pendingBulkCh:    make(chan *pendingBulk, 1),
		writerDone:       make(chan struct{}),
		newBulkWriter:    func() bulkWriter { return &mockBulkWriter{} },
	}

	ok := w.submit()
	require.True(t, ok, "submit should succeed")

	// Verify the sealed bulk was queued with correct fields.
	require.Len(t, w.pendingBulkCh, 1, "one bulk should be queued")

	pb := <-w.pendingBulkCh
	assert.Equal(t, mock, pb.writer, "queued writer should be the original mock")
	assert.Equal(t, ts, pb.checkpoint, "checkpoint should match pendingTS at seal time")

	// Verify a fresh bulkWriter replaced the old one.
	assert.True(t, w.currentBulkWrite.Empty(), "new bulkWriter should be empty")
	assert.NotEqual(t, mock, w.currentBulkWrite, "bulkWrite should be a new instance")
}

// TestSubmit_ReturnsFalseWhenWriterDead verifies that submit() returns false
// when the writer goroutine has already exited (writerDone closed).
func TestSubmit_ReturnsFalseWhenWriterDead(t *testing.T) {
	t.Parallel()

	w := &worker{
		currentBulkWrite: &mockBulkWriter{count: 1}, // non-empty so submit tries to queue
		pendingBulkCh:    make(chan *pendingBulk, 1),
		writerDone:       make(chan struct{}),
	}

	close(w.writerDone) // simulate dead writer

	ok := w.submit()
	assert.False(t, ok, "submit should fail when writer is dead")
}

// TestQueueBulk_BlocksWhenFull verifies that queueBulk blocks when the
// bulkQueue channel is at capacity, and unblocks once a slot is freed.
func TestQueueBulk_BlocksWhenFull(t *testing.T) {
	t.Parallel()

	w := &worker{
		pendingBulkCh: make(chan *pendingBulk, 1),
		writerDone:    make(chan struct{}),
	}

	// Fill the queue to capacity.
	w.pendingBulkCh <- &pendingBulk{}

	// Next queueBulk should block.
	result := make(chan bool, 1)

	go func() {
		result <- w.queueBulk(&pendingBulk{})
	}()

	select {
	case <-result:
		t.Fatal("queueBulk should block when queue is full")
	case <-time.After(50 * time.Millisecond):
		// Expected: blocked.
	}

	// Drain one item to unblock.
	<-w.pendingBulkCh

	select {
	case ok := <-result:
		assert.True(t, ok, "queueBulk should succeed after space freed")
	case <-time.After(barrierTimeout):
		t.Fatal("queueBulk did not unblock after draining queue")
	}
}

// TestQueueBulk_UnblocksOnWriterDone verifies that queueBulk returns false
// when the writer goroutine dies (writerDone closed) while blocked on a
// full queue.
func TestQueueBulk_UnblocksOnWriterDone(t *testing.T) {
	t.Parallel()

	w := &worker{
		pendingBulkCh: make(chan *pendingBulk, 1),
		writerDone:    make(chan struct{}),
	}

	// Fill the queue to capacity.
	w.pendingBulkCh <- &pendingBulk{}

	// queueBulk blocks on full queue.
	result := make(chan bool, 1)

	go func() {
		result <- w.queueBulk(&pendingBulk{})
	}()

	select {
	case <-result:
		t.Fatal("queueBulk should block when queue is full")
	case <-time.After(50 * time.Millisecond):
		// Expected: blocked.
	}

	// Close writerDone to simulate writer death.
	close(w.writerDone)

	select {
	case ok := <-result:
		assert.False(t, ok, "queueBulk should return false when writer dies")
	case <-time.After(barrierTimeout):
		t.Fatal("queueBulk did not unblock after writerDone closed")
	}
}

// TestAsyncPipeline_EventsApplied verifies the end-to-end async pipeline:
// events routed to workers are written by the writer goroutine and counted
// in eventsApplied.
func TestAsyncPipeline_EventsApplied(t *testing.T) {
	t.Parallel()

	pool := makeTestPoolLive(t, []bulkWriter{
		&mockBulkWriter{},
		&mockBulkWriter{},
	})

	const eventsPerWorker = 5

	for i := range eventsPerWorker {
		pool.workers[0].routedEventCh <- makeInsertEvent(fmt.Sprintf("w0-doc-%d", i))
		pool.workers[1].routedEventCh <- makeInsertEvent(fmt.Sprintf("w1-doc-%d", i))
	}

	err := pool.Barrier()
	require.NoError(t, err)

	assert.Equal(t, int64(eventsPerWorker*2), pool.TotalEventsApplied(),
		"all routed events should be applied by the async writer")

	pool.ReleaseBarrier()
}

// TestAsyncPipeline_CheckpointAdvances verifies that the committed timestamp
// (lastTS) correctly reflects the checkpoint captured when the bulk was
// sealed, not the live pendingTS. Tests across two barrier+resume cycles
// with different timestamps.
func TestAsyncPipeline_CheckpointAdvances(t *testing.T) {
	t.Parallel()

	pool := makeTestPoolLive(t, []bulkWriter{
		&mockBulkWriter{},
	})

	w := pool.workers[0]

	// Phase 1: send events with ts={T:10}.
	ts1 := bson.Timestamp{T: 10, I: 1}
	w.routedEventCh <- makeInsertEventWithTS("doc-1", ts1)

	err := pool.Barrier()
	require.NoError(t, err)

	committed := w.lastTS.Load()
	require.NotNil(t, committed, "lastTS should be set after barrier")
	assert.Equal(t, ts1, *committed, "lastTS should match phase 1 checkpoint")

	pool.ReleaseBarrier()

	// Phase 2: send events with ts={T:20}.
	ts2 := bson.Timestamp{T: 20, I: 1}
	w.routedEventCh <- makeInsertEventWithTS("doc-2", ts2)

	err = pool.Barrier()
	require.NoError(t, err)

	committed = w.lastTS.Load()
	require.NotNil(t, committed, "lastTS should be set after second barrier")
	assert.Equal(t, ts2, *committed, "lastTS should advance to phase 2 checkpoint")

	pool.ReleaseBarrier()
}

// TestAsyncPipeline_WriterErrorStopsWorker verifies that when the writer
// goroutine encounters a Do() error, it closes writerDone, and the main
// loop detects this via the <-w.writerDone select case and exits.
// This tests the error path WITHOUT a barrier (unlike the existing barrier
// error tests).
func TestAsyncPipeline_WriterErrorStopsWorker(t *testing.T) {
	t.Parallel()

	pool := makeTestPoolLive(t, []bulkWriter{
		&mockBulkWriter{doErr: errors.New("write concern error")},
	})

	w := pool.workers[0]

	// Send one event. The worker's ticker will fire (flushInterval=1s),
	// triggering submit(). The writer goroutine calls Do() → error →
	// closes writerDone → main loop exits.
	w.routedEventCh <- makeInsertEvent("doc-0")

	// Wait for the worker to report its error.
	select {
	case err := <-pool.Err():
		assert.Contains(t, err.Error(), "bulk write",
			"error should originate from the writer goroutine")
	case <-time.After(barrierTimeout):
		t.Fatal("timed out waiting for writer error to propagate")
	}

	// Worker should have exited (done channel closed).
	select {
	case <-w.done:
		// Worker exited as expected.
	case <-time.After(barrierTimeout):
		t.Fatal("worker did not exit after writer error")
	}
}

// TestAsyncPipeline_BarrierDrainsQueue verifies that a barrier correctly
// drains all pending events through the async pipeline. Multiple events
// per worker are sent to exercise the full drain sequence:
// drainRoutedEvents → submit → close(bulkQueue) → writer finishes all.
func TestAsyncPipeline_BarrierDrainsQueue(t *testing.T) {
	t.Parallel()

	pool := makeTestPoolLive(t, []bulkWriter{
		&mockBulkWriter{},
		&mockBulkWriter{},
	})

	const eventsPerWorker = 10

	for i := range eventsPerWorker {
		pool.workers[0].routedEventCh <- makeInsertEvent(fmt.Sprintf("w0-%d", i))
		pool.workers[1].routedEventCh <- makeInsertEvent(fmt.Sprintf("w1-%d", i))
	}

	err := pool.Barrier()
	require.NoError(t, err)

	// Verify each worker applied exactly the events it received.
	assert.Equal(t, int64(eventsPerWorker), pool.workers[0].eventsApplied.Load(),
		"worker 0 should have applied all its events")
	assert.Equal(t, int64(eventsPerWorker), pool.workers[1].eventsApplied.Load(),
		"worker 1 should have applied all its events")

	pool.ReleaseBarrier()
}

// TestAsyncPipeline_BarrierResumeProcessesNewEvents verifies that after a
// barrier+resume cycle, the worker's async writer pipeline is fully
// restarted (new bulkQueue, writerDone, bulkWriter, writer goroutine)
// and can process new events.
func TestAsyncPipeline_BarrierResumeProcessesNewEvents(t *testing.T) {
	t.Parallel()

	pool := makeTestPoolLive(t, []bulkWriter{
		&mockBulkWriter{},
	})

	w := pool.workers[0]

	// Phase 1: send events, barrier, resume.
	const phase1Events = 3
	for i := range phase1Events {
		w.routedEventCh <- makeInsertEvent(fmt.Sprintf("p1-%d", i))
	}

	err := pool.Barrier()
	require.NoError(t, err)
	assert.Equal(t, int64(phase1Events), w.eventsApplied.Load())

	pool.ReleaseBarrier()

	// Phase 2: send more events after resume, barrier again.
	const phase2Events = 5
	for i := range phase2Events {
		w.routedEventCh <- makeInsertEvent(fmt.Sprintf("p2-%d", i))
	}

	err = pool.Barrier()
	require.NoError(t, err)
	assert.Equal(t, int64(phase1Events+phase2Events), w.eventsApplied.Load(),
		"eventsApplied should include events from both phases")

	pool.ReleaseBarrier()
}
