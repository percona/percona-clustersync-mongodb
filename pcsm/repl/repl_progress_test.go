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
