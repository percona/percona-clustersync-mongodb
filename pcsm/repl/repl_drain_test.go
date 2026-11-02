package repl //nolint:testpackage // Exercises the run-owned drain and controlled bulk writer seams.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/errors"
)

func TestStopRun_StatusAndCheckpointDuringFinalDrain(t *testing.T) {
	t.Parallel()

	t.Run("held final drain", func(t *testing.T) {
		t.Parallel()

		gate := newBlockingGate()
		release := sync.OnceFunc(gate.releaseAll)
		defer release()
		// A partial bulk is only sealed by Stop's final barrier.
		bw := gate.writer()
		bw.fullAt = 0
		pool := makeTestPoolLiveWithParent(t, t.Context(), []bulkWriter{bw},
			func() bulkWriter { return gate.writer() })
		r := &Repl{
			pool: pool, startTime: time.Now(), pausing: true,
			pauseCh: make(chan struct{}), doneCh: make(chan struct{}),
			eventsApplied: 7, checkpointOpTime: bson.Timestamp{T: 10},
		}
		done := r.Done()
		event := makeInsertEventWithTS("final", bson.Timestamp{T: 20})
		pool.Route(event.change, event.ns)

		go r.stopRun(t.Context(), pool)
		gate.awaitParked(t)

		type snapshot struct {
			status Status
			cp     *Checkpoint
		}
		observed := make(chan snapshot, 1)
		go func() { observed <- snapshot{r.Status(), r.Checkpoint()} }()
		select {
		case got := <-observed:
			assert.True(t, got.status.Pausing)
			assert.Zero(t, got.status.PauseTime)
			assert.Equal(t, int64(7), got.status.EventsApplied)
			assert.Equal(t, int64(7), got.cp.EventsApplied)
		case <-time.After(barrierTimeout):
			assert.Fail(t, "status/checkpoint blocked behind final drain")
		}
		select {
		case <-done:
			assert.Fail(t, "Done closed before final write completed")
		default:
		}

		release()
		select {
		case <-done:
		case <-time.After(barrierTimeout):
			require.FailNow(t, "final drain did not finish")
		}

		status := r.Status()
		require.NoError(t, status.Err)
		assert.False(t, status.Pausing)
		assert.NotZero(t, status.PauseTime)
		assert.Equal(t, int64(8), status.EventsApplied, "final write must count exactly once")
		assert.Equal(t, int64(8), r.Checkpoint().EventsApplied)
		assert.Equal(t, event.change.ClusterTime, r.Checkpoint().CheckpointOpTime)
	})
}

func TestStopRun_FinalBarrierFailure(t *testing.T) {
	t.Parallel()

	writeErr := errors.New("final bulk failed")
	originalErr := errors.New("earlier replication failure")
	for _, prior := range []error{nil, originalErr} {
		name := "new failure"
		if prior != nil {
			name = "preserve prior failure"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			pool := makeTestPoolLive(t, []bulkWriter{&mockBulkWriter{doErr: writeErr}})
			r := &Repl{
				pool: pool, startTime: time.Now(), pausing: true, err: prior,
				doneCh: make(chan struct{}), checkpointOpTime: bson.Timestamp{T: 10},
			}
			event := makeInsertEventWithTS("failed", bson.Timestamp{T: 20})
			pool.Route(event.change, event.ns)

			r.stopRun(t.Context(), pool)

			select {
			case <-r.Done():
			default:
				require.FailNow(t, "Done was not closed after terminal bookkeeping")
			}
			wantErr := writeErr
			if prior != nil {
				wantErr = prior
			}
			require.ErrorIs(t, r.Status().Err, wantErr)
			assert.Equal(t, int64(0), r.Status().EventsApplied)
			assert.Equal(t, event.change.ClusterTime, r.Checkpoint().CheckpointOpTime,
				"failed first event remains an inclusive replay floor")
			assert.Contains(t, r.Checkpoint().Error, wantErr.Error())
		})
	}
}

func TestStopRun_CancellationPreservesRecordedWriterFailure(t *testing.T) {
	t.Parallel()

	writeErr := errors.New("writer failed before cancellation")
	for _, failed := range []bool{false, true} {
		name := "abandonment only"
		if failed {
			name = "recorded failure"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			gate := newBlockingGate()
			var bw bulkWriter = gate.writer()
			if failed {
				bw = &mockBulkWriter{doErr: writeErr, fullAt: 1}
			}
			pool := makeTestPoolLiveWithParent(t, ctx, []bulkWriter{bw},
				func() bulkWriter { return gate.writer() })
			r := &Repl{
				pool: pool, startTime: time.Now(), pausing: true,
				doneCh: make(chan struct{}), checkpointOpTime: bson.Timestamp{T: 10},
			}
			event := makeInsertEventWithTS("interrupted", bson.Timestamp{T: 20})
			pool.Route(event.change, event.ns)
			if failed {
				awaitWorkerExit(t, pool.workers[0])
				// The dispatcher may have consumed the notification as
				// cancellation arrived. Stop must retain the writer's cause.
				require.ErrorIs(t, <-pool.Err(), writeErr)
			} else {
				gate.awaitParked(t)
			}
			cancel()

			r.stopRun(ctx, pool)

			if failed {
				require.ErrorIs(t, r.Status().Err, writeErr)
			} else {
				require.NoError(t, r.Status().Err)
			}
			assert.Equal(t, int64(0), r.Status().EventsApplied)
			assert.Equal(t, event.change.ClusterTime, r.Checkpoint().CheckpointOpTime)
		})
	}
}

type cancelingBulkWriter struct {
	mockBulkWriter

	cancel context.CancelFunc
}

func (b *cancelingBulkWriter) Do(context.Context, *mongo.Client) (int, error) {
	b.cancel()

	return 0, b.doErr
}

func TestStopRun_WriteReturningAfterCancellationIsAbandoned(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	writeErr := errors.New("interrupted write without a wrapped context error")
	bw := &cancelingBulkWriter{cancel: cancel}
	bw.doErr = writeErr
	pool := makeTestPoolLiveWithParent(t, ctx, []bulkWriter{bw},
		func() bulkWriter { return &mockBulkWriter{} })
	r := &Repl{
		pool: pool, startTime: time.Now(), doneCh: make(chan struct{}),
		checkpointOpTime: bson.Timestamp{T: 10},
	}
	event := makeInsertEventWithTS("failed", bson.Timestamp{T: 20})
	pool.Route(event.change, event.ns)

	r.stopRun(ctx, pool)

	require.NoError(t, r.Status().Err)
	assert.Equal(t, event.change.ClusterTime, r.Checkpoint().CheckpointOpTime)
}

type cancelOnOverflowWriter struct {
	mockBulkWriter

	abandon func()
}

func (b *cancelOnOverflowWriter) WouldOverflow(int) bool {
	if b.count == 0 {
		return false
	}

	b.abandon()

	return true
}

func TestStopRun_OverflowAfterWriterAbandonmentIsNotFailure(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	gate := newBlockingGate()
	var pool *workerPool
	sealed := 0
	pool = makeTestPoolLiveWithParent(t, ctx, []bulkWriter{gate.writer()}, func() bulkWriter {
		sealed++
		if sealed <= config.WorkerBulkQueueSize {
			return &mockBulkWriter{fullAt: 1}
		}

		return &cancelOnOverflowWriter{abandon: func() {
			cancel()
			<-pool.workers[0].writerDone
		}}
	})
	r := &Repl{
		pool: pool, startTime: time.Now(), doneCh: make(chan struct{}),
		checkpointOpTime: bson.Timestamp{T: 10},
	}
	first := makeInsertEventWithTS("parked", bson.Timestamp{T: 20})
	pool.Route(first.change, first.ns)
	gate.awaitParked(t)

	// Fill the sealed-bulk queue, then overflow a partial bulk. Cancellation
	// happens inside WouldOverflow, so the worker cannot select ctx.Done first.
	// The full queue forces enqueueBulk to observe the abandoned writer.
	for range config.WorkerBulkQueueSize + 2 {
		event := makeInsertEventWithTS("overflow", bson.Timestamp{T: 21})
		pool.Route(event.change, event.ns)
	}
	awaitWorkerExit(t, pool.workers[0])
	require.ErrorIs(t, ctx.Err(), context.Canceled)

	r.stopRun(ctx, pool)

	require.NoError(t, r.Status().Err)
	assert.Equal(t, first.change.ClusterTime, r.Checkpoint().CheckpointOpTime)
	assert.Zero(t, r.Status().EventsApplied)
}
