package repl //nolint:testpackage // Exercises the run-owned worker progress tracker.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/mdb"
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

	coldEvent := makeInsertEventWithTS(coldID, cold)
	pool.Route(coldEvent.change, coldEvent.ns)
	commitRoutedEvent(t, pool.workers[0])

	for _, seconds := range []uint32{201, 202, 203} {
		hotEvent := makeInsertEventWithTS(hotID, bson.Timestamp{T: seconds, I: 1})
		pool.Route(hotEvent.change, hotEvent.ns)
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

// A reconnect reopens inclusively from the checkpoint. Newer routed writes can
// commit and lift the floor past an already-applied DDL before the redelivered
// copy reaches the dispatcher; re-applying a drop there would discard those
// writes. The guard must drop the redelivered DDL on every topology.
func TestShouldSkipReplay_RedeliveredDropBehindNewerCommits(t *testing.T) {
	t.Parallel()

	for _, sharded := range []bool{false, true} {
		name := "replica set"
		if sharded {
			name = "sharded"
		}

		t.Run(name, func(t *testing.T) {
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

func TestIndexCheckpoint_DispatchPredecessors(t *testing.T) {
	t.Parallel()

	for _, name := range []string{
		"first event", "inherited token", "routed", "filtered", "internal", "replayed", "invalidate",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), barrierTimeout)
			defer cancel()
			ts := bson.Timestamp{T: 100, I: 1}
			done := make(chan struct{})
			close(done)
			r := &Repl{
				pool: makeTestPoolLiveWithParent(t, ctx, []bulkWriter{&mockBulkWriter{}}, func() bulkWriter {
					return &mockBulkWriter{}
				}),
				catalog: &indexCheckpointCatalog{}, options: &Options{}, startTime: time.Unix(1, 0),
				checkpointOpTime: ts, doneCh: done, pauseCh: make(chan struct{}, 1),
				nsFilter: func(_, coll string) bool { return coll != "excluded" },
			}
			queue := changeStreamQueue{
				events: make(chan *ChangeEvent, 5),
				resume: make(chan *options.ChangeStreamOptionsBuilder, 1),
			}
			var want bson.Raw
			switch name {
			case "first event":
			case "inherited token":
				want = checkpointToken(t, "saved-exclusive-predecessor")
				r.checkpointStartAfter = want
			default:
				prior := makeInsertEventWithTS("prior", ts).change
				prior.ID = checkpointToken(t, name)
				want = prior.ID
				switch name {
				case "filtered":
					prior.Namespace.Collection = "excluded"
				case "internal":
					prior.Namespace.Database = config.PCSMDatabase
				case "replayed":
					prior.ClusterTime = bson.Timestamp{T: 99}
				case "invalidate":
					prior.OperationType = Invalidate
					r.sourceIsSharded = true
					r.sourceVer = mdb.ServerVersion{7, 0, 0, 0}
					r.expectMovePrimaryInvalidate = true
				}
				queue.events <- prior
			}
			queue.events <- &ChangeEvent{
				OperationType: advanceTimePseudoEvent, ClusterTime: bson.Timestamp{T: 200},
				ID: checkpointToken(t, "read-ahead-tick-is-not-a-predecessor"),
			}
			first := checkpointIndexEvent(t, CreateIndexes, ts)
			queue.events <- first
			queue.events <- checkpointIndexEvent(t, DropIndexes, ts)
			close(queue.events)
			var snapshots []*Checkpoint
			r.options.Checkpoint = func(context.Context) error {
				snapshots = append(snapshots, r.Checkpoint())

				return nil
			}

			r.dispatchChanges(ctx, queue)

			require.NoError(t, r.Status().Err)
			require.Len(t, snapshots, 2)
			assert.Equal(t, ts, snapshots[0].CheckpointOpTime)
			assert.Equal(t, want, snapshots[0].StartAfter)
			assert.Equal(t, first.ID, snapshots[1].StartAfter, "same-timestamp boundary replaces predecessor")
			assert.Equal(t, ts, snapshots[1].CheckpointOpTime)
			assert.Equal(t, first.ID, r.Checkpoint().StartAfter, "post-apply snapshot retains exact boundary")
		})
	}
}

func TestIndexCheckpoint_ReconnectAfterInvalidate(t *testing.T) {
	t.Parallel()

	for _, indexBoundary := range []bool{false, true} {
		t.Run(fmt.Sprintf("index boundary %t", indexBoundary), func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), barrierTimeout)
			defer cancel()
			ts := bson.Timestamp{T: 100, I: 1}
			old := checkpointToken(t, "older-boundary")
			invalid := checkpointToken(t, "invalidate")
			r := &Repl{
				pool: makeTestPoolLiveWithParent(t, ctx, []bulkWriter{&mockBulkWriter{}}, func() bulkWriter {
					return &mockBulkWriter{}
				}),
				catalog: &indexCheckpointCatalog{}, options: &Options{Checkpoint: func(context.Context) error { return nil }},
				nsFilter:        func(string, string) bool { return true },
				sourceIsSharded: true, sourceVer: mdb.ServerVersion{8, 0, 0, 0},
				expectMovePrimaryInvalidate: true, checkpointOpTime: ts, checkpointStartAfter: old,
			}
			queue := changeStreamQueue{
				events: make(chan *ChangeEvent, 4),
				resume: make(chan *options.ChangeStreamOptionsBuilder, 1),
			}
			queue.events <- &ChangeEvent{OperationType: Invalidate, ID: invalid, ClusterTime: ts}
			want := invalid
			if indexBoundary {
				prior := makeInsertEventWithTS("after-invalidate", ts).change
				prior.ID = checkpointToken(t, "after-invalidate")
				queue.events <- prior
				queue.events <- checkpointIndexEvent(t, CreateIndexes, ts)
				want = prior.ID
			}
			queue.events <- nil
			close(queue.events)

			r.dispatchChanges(ctx, queue)

			opts := resolvedResumeOptions(t, awaitResumeOptions(t, queue.resume))
			assert.Equal(t, want, opts.StartAfter, "a later exact boundary supersedes the invalidate token")
			assert.Nil(t, opts.StartAtOperationTime)
		})
	}
}
