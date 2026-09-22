package repl //nolint:testpackage // Exercises the run-owned worker progress tracker.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/util"
)

func TestTrackPoolProgress(t *testing.T) {
	t.Parallel()

	start := bson.Timestamp{T: 100, I: 1}
	first := bson.Timestamp{T: 101, I: 1}
	second := bson.Timestamp{T: 102, I: 1}
	newer := bson.Timestamp{T: 200, I: 1}

	tests := []struct {
		name     string
		routed   []bson.Timestamp
		advance  bson.Timestamp
		want     bson.Timestamp
		wantPool bson.Timestamp
		wantIdle bool
	}{
		{
			name:     "committed floor advances while pool is busy",
			routed:   []bson.Timestamp{first, second},
			want:     first,
			wantPool: first,
			wantIdle: false,
		},
		{
			name:     "unrouted pool is a no-op",
			want:     start,
			wantIdle: true,
		},
		{
			name:     "older committed floor cannot regress frontiers",
			routed:   []bson.Timestamp{first},
			advance:  newer,
			want:     newer,
			wantPool: first,
			wantIdle: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pool := makeTestPool(1)
			r := &Repl{pool: pool, lastReplicatedOpTime: start, checkpointOpTime: start}
			ticks := make(chan time.Time)
			stop := make(chan struct{})
			stopped := make(chan struct{})
			t.Cleanup(func() {
				close(stop)
				select {
				case <-stopped:
				case <-time.After(barrierTimeout):
					require.FailNow(t, "progress tracker did not stop")
				}
			})
			go func() {
				defer close(stopped)
				r.trackPoolProgress(pool, ticks, stop)
			}()

			for _, ts := range tt.routed {
				event := makeInsertEventWithTS("document", ts)
				pool.Route(event.change, event.ns)
			}
			if len(tt.routed) > 0 {
				// Commit only the first event through the real writer path.
				commitRoutedEvent(t, pool.workers[0])
			}
			require.Equal(t, tt.wantPool, pool.Checkpoint())
			require.Equal(t, tt.wantIdle, pool.Idle())

			r.lock.Lock()
			r.advanceCheckpoint(tt.advance)
			r.lock.Unlock()

			// The second unbuffered send is received only after the first
			// checkpoint update finishes. No dispatcher activity is needed.
			err := util.CtxWithTimeout(t.Context(), barrierTimeout, func(ctx context.Context) error {
				for range 2 {
					select {
					case ticks <- time.Time{}:
					case <-ctx.Done():
						return errors.Wrap(ctx.Err(), "progress tracker did not receive tick")
					}
				}

				return nil
			})
			require.NoError(t, err)

			status := r.Status()
			assert.Equal(t, tt.want, status.CheckpointOpTime)
			assert.Equal(t, tt.want, status.LastReplicatedOpTime)
			assert.Equal(t, tt.wantIdle, pool.Idle(), "tracking must not drain pending events")
		})
	}
}

// Promoted from the PCSM-383 review reproduction. Worker 0 drains after one
// event at 101.1 and never receives another; worker 1 keeps committing its
// backlog. The resume floor must stay at 101.1 (worker 0 still bounds it),
// but reporting must follow worker 1's commits.
func TestTrackPoolProgress_DrainedWorkerDoesNotFreezeReporting(t *testing.T) {
	t.Parallel()

	pool := makeTestPool(2)
	start := bson.Timestamp{T: 100, I: 1}
	cold := bson.Timestamp{T: 101, I: 1}
	r := &Repl{pool: pool, lastReplicatedOpTime: start, checkpointOpTime: start}
	coldID := docIDForWorker(t, 0, 2)
	hotID := docIDForWorker(t, 1, 2)

	event := makeInsertEventWithTS(coldID, cold)
	pool.Route(event.change, event.ns)
	commitRoutedEvent(t, pool.workers[0])

	for _, seconds := range []uint32{201, 202, 203} {
		event := makeInsertEventWithTS(hotID, bson.Timestamp{T: seconds, I: 1})
		pool.Route(event.change, event.ns)
	}
	commitRoutedEvent(t, pool.workers[1])
	r.advancePoolCheckpoint(pool)
	before := r.Status()
	require.Equal(t, int64(2), before.EventsApplied)
	require.False(t, pool.Idle())
	assert.Equal(t, cold, before.CheckpointOpTime, "drained worker 0 still bounds the resume floor")
	assert.Equal(t, bson.Timestamp{T: 201, I: 1}, before.LastReplicatedOpTime,
		"reporting follows the busy worker's first commit")

	commitRoutedEvent(t, pool.workers[1])
	r.advancePoolCheckpoint(pool)
	after := r.Status()
	require.Equal(t, int64(3), after.EventsApplied)
	require.False(t, pool.Idle())
	assert.Equal(t, cold, after.CheckpointOpTime, "reporting-scoped fix: the resume floor is unchanged")
	assert.Equal(t, bson.Timestamp{T: 202, I: 1}, after.LastReplicatedOpTime,
		"second busy commit moves reporting; the pending 203.1 bounds it")

	commitRoutedEvent(t, pool.workers[1])
	r.advancePoolCheckpoint(pool)
	drained := r.Status()
	require.True(t, pool.Idle())
	assert.Equal(t, bson.Timestamp{T: 203, I: 1}, drained.LastReplicatedOpTime,
		"all drained: the newest committed event is the frontier")
	assert.Equal(t, cold, drained.CheckpointOpTime,
		"the drained worker keeps pinning Checkpoint's minimum; lifting it is the linked ticket's scope")
}

func TestReportedFrontier(t *testing.T) {
	t.Parallel()

	type routed struct {
		worker int
		ts     bson.Timestamp
		commit bool // commit this event through the real writer path
		fail   bool // the bulk for this event fails
	}

	tests := []struct {
		name   string
		events []routed
		want   bson.Timestamp
	}{
		{
			name: "never routed pool reports nothing",
			want: bson.Timestamp{},
		},
		{
			name: "slower worker with outstanding work bounds a faster one",
			events: []routed{
				{worker: 0, ts: bson.Timestamp{T: 101, I: 1}, commit: true},
				{worker: 0, ts: bson.Timestamp{T: 150, I: 1}},
				{worker: 1, ts: bson.Timestamp{T: 201, I: 1}, commit: true},
				{worker: 1, ts: bson.Timestamp{T: 202, I: 1}},
			},
			want: bson.Timestamp{T: 101, I: 1},
		},
		{
			name: "never committed worker holds the frontier at its first event",
			events: []routed{
				{worker: 0, ts: bson.Timestamp{T: 101, I: 1}},
				{worker: 1, ts: bson.Timestamp{T: 201, I: 1}, commit: true},
			},
			want: bson.Timestamp{T: 101, I: 1},
		},
		{
			name: "failed write is outstanding, not drained",
			events: []routed{
				{worker: 0, ts: bson.Timestamp{T: 101, I: 1}, commit: true},
				{worker: 0, ts: bson.Timestamp{T: 102, I: 1}, fail: true},
				{worker: 1, ts: bson.Timestamp{T: 201, I: 1}, commit: true},
			},
			want: bson.Timestamp{T: 101, I: 1},
		},
		{
			name: "distinct events sharing a timestamp keep the worker busy",
			events: []routed{
				{worker: 0, ts: bson.Timestamp{T: 101, I: 1}, commit: true},
				{worker: 0, ts: bson.Timestamp{T: 101, I: 1}},
				{worker: 1, ts: bson.Timestamp{T: 201, I: 1}, commit: true},
			},
			want: bson.Timestamp{T: 101, I: 1},
		},
		{
			name: "all drained reports the newest commit",
			events: []routed{
				{worker: 0, ts: bson.Timestamp{T: 101, I: 1}, commit: true},
				{worker: 1, ts: bson.Timestamp{T: 201, I: 1}, commit: true},
			},
			want: bson.Timestamp{T: 201, I: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pool := makeTestPool(2)
			for _, ev := range tt.events {
				event := makeInsertEventWithTS(docIDForWorker(t, ev.worker, 2), ev.ts)
				pool.Route(event.change, event.ns)
				switch {
				case ev.commit:
					commitRoutedEvent(t, pool.workers[ev.worker])
				case ev.fail:
					failRoutedEvent(t, pool.workers[ev.worker])
				}
			}

			assert.Equal(t, tt.want, pool.ReportedFrontier())
			assert.False(t, pool.ReportedFrontier().Before(pool.Checkpoint()),
				"reporting never trails the resume floor")
		})
	}
}

// failRoutedEvent drains one routed event into a bulk whose write fails, so
// the worker counts it as routed but never committed.
func failRoutedEvent(t *testing.T, w *worker) {
	t.Helper()

	w.errCh = make(chan error, 1)
	w.currentBulkWrite = &mockBulkWriter{doErr: assert.AnError}
	w.newBulkWriter = func() bulkWriter { return &mockBulkWriter{} }
	w.pendingBulkCh = make(chan *pendingBulk, 1)
	w.writerDone = make(chan struct{})

	require.NoError(t, w.addToCurrentBulk(<-w.routedEventCh))
	require.True(t, w.enqueueBulk())
	close(w.pendingBulkCh)
	w.runWriter(t.Context())
	require.Error(t, w.writerErr)
}

// A reconnect reopens inclusively from the checkpoint. Newer routed writes can
// commit and lift the floor past an already-applied DDL before the redelivered
// copy reaches the dispatcher; re-applying a drop there would discard those
// writes. The guard must drop the redelivered DDL on every topology.
func TestShouldSkipReplay_RedeliveredDropBehindNewerCommits(t *testing.T) {
	t.Parallel()

	for _, sharded := range []bool{false, true} {
		t.Run(map[bool]string{false: "replica set", true: "sharded"}[sharded], func(t *testing.T) {
			t.Parallel()

			drop := bson.Timestamp{T: 100, I: 1}
			pool := makeTestPool(1)
			r := &Repl{pool: pool, sourceIsSharded: sharded, lastReplicatedOpTime: drop, checkpointOpTime: drop}

			// The drop was applied and the run continued: a newer write
			// routed after it commits and the tracker lifts the floor.
			event := makeInsertEventWithTS("after-drop", bson.Timestamp{T: 105, I: 1})
			pool.Route(event.change, event.ns)
			commitRoutedEvent(t, pool.workers[0])
			r.advancePoolCheckpoint(pool)
			require.Equal(t, bson.Timestamp{T: 105, I: 1}, r.Status().CheckpointOpTime)

			redelivered := &ChangeEvent{OperationType: Drop, ClusterTime: drop}
			assert.True(t, r.shouldSkipReplay(redelivered), "redelivered drop behind newer commits must be skipped")

			atFloor := &ChangeEvent{OperationType: Drop, ClusterTime: bson.Timestamp{T: 105, I: 1}}
			assert.False(t, r.shouldSkipReplay(atFloor), "an event at the inclusive floor is not a replay")
		})
	}
}
