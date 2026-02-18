package pcsm //nolint:testpackage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/pcsm/clone"
	"github.com/percona/percona-clustersync-mongodb/pcsm/repl"
)

func TestNew(t *testing.T) {
	t.Parallel()

	p := New(t.Context(), nil, nil)

	assert.Equal(t, State(StateIdle), p.state, "initial state should be StateIdle")
	assert.Nil(t, p.source, "source should be nil when passed nil")
	assert.Nil(t, p.target, "target should be nil when passed nil")
	assert.NotNil(t, p.onStateChanged, "onStateChanged should be initialized to non-nil")

	// Verify onStateChanged is callable (no-op function)
	assert.NotPanics(t, func() {
		p.onStateChanged(StateRunning)
	}, "onStateChanged should be callable without panic")
}

func TestCheckpoint(t *testing.T) {
	t.Parallel()

	t.Run("returns nil when idle", func(t *testing.T) {
		t.Parallel()

		p := &PCSM{
			state:          StateIdle,
			onStateChanged: func(State) {},
		}

		data, err := p.Checkpoint(context.Background())

		require.NoError(t, err)
		assert.Nil(t, data)
	})

	t.Run("returns valid BSON when paused", func(t *testing.T) {
		t.Parallel()

		p := &PCSM{
			state:          StatePaused,
			onStateChanged: func(State) {},
			catalog:        catalog.NewCatalog(nil),
			clone:          &mockCloner{doneCh: make(chan struct{})},
			repl:           &mockReplicator{doneCh: make(chan struct{})},
		}

		data, err := p.Checkpoint(context.Background())

		require.NoError(t, err)
		require.NotNil(t, data)

		// Verify it's valid BSON by unmarshaling
		var cp checkpoint
		err = bson.Unmarshal(data, &cp)
		require.NoError(t, err)
		assert.Equal(t, State(StatePaused), cp.State)
	})

	t.Run("includes namespace filters", func(t *testing.T) {
		t.Parallel()

		p := &PCSM{
			state:          StatePaused,
			onStateChanged: func(State) {},
			nsInclude:      []string{"db1.*", "db2.coll"},
			nsExclude:      []string{"db1.excluded"},
			catalog:        catalog.NewCatalog(nil),
			clone:          &mockCloner{doneCh: make(chan struct{})},
			repl:           &mockReplicator{doneCh: make(chan struct{})},
		}

		data, err := p.Checkpoint(context.Background())

		require.NoError(t, err)

		var cp checkpoint
		err = bson.Unmarshal(data, &cp)
		require.NoError(t, err)
		assert.Equal(t, []string{"db1.*", "db2.coll"}, cp.NSInclude)
		assert.Equal(t, []string{"db1.excluded"}, cp.NSExclude)
	})

	t.Run("includes error when present", func(t *testing.T) {
		t.Parallel()

		p := &PCSM{
			state:          StateFailed,
			onStateChanged: func(State) {},
			err:            errors.New("test error"),
			catalog:        catalog.NewCatalog(nil),
			clone:          &mockCloner{doneCh: make(chan struct{})},
			repl:           &mockReplicator{doneCh: make(chan struct{})},
		}

		data, err := p.Checkpoint(context.Background())

		require.NoError(t, err)

		var cp checkpoint
		err = bson.Unmarshal(data, &cp)
		require.NoError(t, err)
		assert.Equal(t, "test error", cp.Error)
	})

	t.Run("includes clone checkpoint", func(t *testing.T) {
		t.Parallel()

		cloneCP := &clone.Checkpoint{
			TotalSize:  1000,
			CopiedSize: 500,
		}

		p := &PCSM{
			state:          StatePaused,
			onStateChanged: func(State) {},
			catalog:        catalog.NewCatalog(nil),
			clone:          &mockCloner{doneCh: make(chan struct{}), checkpoint: cloneCP},
			repl:           &mockReplicator{doneCh: make(chan struct{})},
		}

		data, err := p.Checkpoint(context.Background())

		require.NoError(t, err)

		var cp checkpoint
		err = bson.Unmarshal(data, &cp)
		require.NoError(t, err)
		require.NotNil(t, cp.Clone)
		assert.Equal(t, uint64(1000), cp.Clone.TotalSize)
		assert.Equal(t, uint64(500), cp.Clone.CopiedSize)
	})

	t.Run("includes repl checkpoint", func(t *testing.T) {
		t.Parallel()

		replCP := &repl.Checkpoint{
			EventsRead:    100,
			EventsApplied: 50,
		}

		p := &PCSM{
			state:          StatePaused,
			onStateChanged: func(State) {},
			catalog:        catalog.NewCatalog(nil),
			clone:          &mockCloner{doneCh: make(chan struct{})},
			repl:           &mockReplicator{doneCh: make(chan struct{}), checkpoint: replCP},
		}

		data, err := p.Checkpoint(context.Background())

		require.NoError(t, err)

		var cp checkpoint
		err = bson.Unmarshal(data, &cp)
		require.NoError(t, err)
		require.NotNil(t, cp.Repl)
		assert.Equal(t, int64(100), cp.Repl.EventsRead)
		assert.Equal(t, int64(50), cp.Repl.EventsApplied)
	})

	// Note: Catalog checkpoint test is skipped because catalog.Checkpoint() returns nil
	// when there are no databases, and we can't populate the catalog without MongoDB.
	// This is covered by E2E tests.
}

func TestRecover(t *testing.T) {
	t.Parallel()

	t.Run("fails when not idle - running", func(t *testing.T) {
		t.Parallel()

		p := &PCSM{
			state:          StateRunning,
			onStateChanged: func(State) {},
		}

		err := p.Recover(context.Background(), []byte{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot recover")
	})

	t.Run("fails when not idle - paused", func(t *testing.T) {
		t.Parallel()

		p := &PCSM{
			state:          StatePaused,
			onStateChanged: func(State) {},
		}

		err := p.Recover(context.Background(), []byte{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot recover")
	})

	t.Run("fails with malformed BSON", func(t *testing.T) {
		t.Parallel()

		p := &PCSM{
			state:          StateIdle,
			onStateChanged: func(State) {},
		}

		err := p.Recover(context.Background(), []byte{0xFF, 0xFF, 0xFF})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("succeeds with idle checkpoint", func(t *testing.T) {
		t.Parallel()

		cp := checkpoint{State: StateIdle}
		data, err := bson.Marshal(cp)
		require.NoError(t, err)

		p := &PCSM{
			state:          StateIdle,
			onStateChanged: func(State) {},
		}

		err = p.Recover(context.Background(), data)

		require.NoError(t, err)
		assert.Equal(t, State(StateIdle), p.state)
	})

	t.Run("recovers paused state", func(t *testing.T) {
		t.Parallel()

		cp := checkpoint{
			State:     StatePaused,
			NSInclude: []string{"db1.*"},
			NSExclude: []string{"db1.excluded"},
		}
		data, err := bson.Marshal(cp)
		require.NoError(t, err)

		p := &PCSM{
			state:          StateIdle,
			onStateChanged: func(State) {},
		}

		err = p.Recover(context.Background(), data)

		require.NoError(t, err)
		assert.Equal(t, State(StatePaused), p.state)
		assert.Equal(t, []string{"db1.*"}, p.nsInclude)
		assert.Equal(t, []string{"db1.excluded"}, p.nsExclude)
		assert.NotNil(t, p.catalog)
		assert.NotNil(t, p.clone)
		assert.NotNil(t, p.repl)
	})

	t.Run("restores error from checkpoint", func(t *testing.T) {
		t.Parallel()

		cp := checkpoint{
			State: StateFailed,
			Error: "previous error",
		}
		data, err := bson.Marshal(cp)
		require.NoError(t, err)

		p := &PCSM{
			state:          StateIdle,
			onStateChanged: func(State) {},
		}

		err = p.Recover(context.Background(), data)

		require.NoError(t, err)
		assert.Equal(t, State(StateFailed), p.state)
		require.Error(t, p.err)
		assert.Equal(t, "previous error", p.err.Error())
	})

	// Note: Recovery from StateRunning triggers doResume() which spawns run() goroutine.
	// This requires real MongoDB clients to function properly. The goroutine will exit
	// gracefully when the mocks don't behave like real clients. This path is tested via E2E.
}

func TestSetOnStateChanged(t *testing.T) {
	t.Parallel()

	t.Run("sets callback function", func(t *testing.T) {
		t.Parallel()

		p := &PCSM{
			state:          StateIdle,
			onStateChanged: func(State) {},
		}

		called := false
		var receivedState State

		p.SetOnStateChanged(func(s State) {
			called = true
			receivedState = s
		})

		// Manually invoke to verify it was set
		p.lock.Lock()
		callback := p.onStateChanged
		p.lock.Unlock()

		callback(StateRunning)

		assert.True(t, called, "callback should have been called")
		assert.Equal(t, State(StateRunning), receivedState)
	})

	t.Run("replaces nil with no-op", func(t *testing.T) {
		t.Parallel()

		p := &PCSM{
			state:          StateIdle,
			onStateChanged: func(State) {},
		}

		p.SetOnStateChanged(nil)

		p.lock.Lock()
		callback := p.onStateChanged
		p.lock.Unlock()

		assert.NotNil(t, callback, "callback should not be nil after setting nil")
		assert.NotPanics(t, func() {
			callback(StateRunning)
		}, "no-op callback should not panic")
	})
}

func TestStatus(t *testing.T) {
	t.Parallel()

	t.Run("returns idle status when idle", func(t *testing.T) {
		t.Parallel()

		p := &PCSM{
			state:          StateIdle,
			onStateChanged: func(State) {},
		}

		status := p.Status(context.Background())

		assert.Equal(t, State(StateIdle), status.State)
		require.NoError(t, status.Error)
		assert.False(t, status.InitialSyncCompleted)
	})

	// Note: Tests below use StateFailed because Status() calls topo.ClusterTime()
	// for non-Idle/non-Failed states, which requires a real MongoDB client.
	// Using StateFailed allows testing status fields without calling ClusterTime().

	t.Run("includes clone status", func(t *testing.T) {
		t.Parallel()

		cloneStatus := clone.Status{
			EstimatedTotalSizeBytes: 1000,
			CopiedSizeBytes:         500,
			StartTime:               time.Now(),
		}

		p := &PCSM{
			state:          StateFailed,
			onStateChanged: func(State) {},
			clone:          &mockCloner{doneCh: make(chan struct{}), status: cloneStatus},
			repl:           &mockReplicator{doneCh: make(chan struct{})},
		}

		status := p.Status(context.Background())

		assert.Equal(t, uint64(1000), status.Clone.EstimatedTotalSizeBytes)
		assert.Equal(t, uint64(500), status.Clone.CopiedSizeBytes)
	})

	t.Run("includes repl status", func(t *testing.T) {
		t.Parallel()

		now := time.Now()

		p := &PCSM{
			state:          StateFailed,
			onStateChanged: func(State) {},
			clone:          &mockCloner{doneCh: make(chan struct{})},
			repl: &mockReplicator{
				doneCh:     make(chan struct{}),
				startTime:  now,
				lastOpTime: bson.Timestamp{T: 100, I: 1},
			},
		}

		status := p.Status(context.Background())

		assert.Equal(t, now, status.Repl.StartTime)
		assert.Equal(t, bson.Timestamp{T: 100, I: 1}, status.Repl.LastReplicatedOpTime)
	})

	t.Run("error from pcsm", func(t *testing.T) {
		t.Parallel()

		p := &PCSM{
			state:          StateFailed,
			onStateChanged: func(State) {},
			err:            errors.New("pcsm error"),
			clone:          &mockCloner{doneCh: make(chan struct{})},
			repl:           &mockReplicator{doneCh: make(chan struct{})},
		}

		status := p.Status(context.Background())

		require.Error(t, status.Error)
		assert.Equal(t, "pcsm error", status.Error.Error())
	})

	t.Run("error from repl wraps", func(t *testing.T) {
		t.Parallel()

		p := &PCSM{
			state:          StateFailed,
			onStateChanged: func(State) {},
			clone:          &mockCloner{doneCh: make(chan struct{})},
			repl: &mockReplicator{
				doneCh: make(chan struct{}),
				err:    errors.New("repl error"),
			},
		}

		status := p.Status(context.Background())

		require.Error(t, status.Error)
		assert.Contains(t, status.Error.Error(), "Change Replication")
		assert.Contains(t, status.Error.Error(), "repl error")
	})

	t.Run("error from clone wraps", func(t *testing.T) {
		t.Parallel()

		p := &PCSM{
			state:          StateFailed,
			onStateChanged: func(State) {},
			clone: &mockCloner{
				doneCh: make(chan struct{}),
				status: clone.Status{Err: errors.New("clone error")},
			},
			repl: &mockReplicator{doneCh: make(chan struct{})},
		}

		status := p.Status(context.Background())

		require.Error(t, status.Error)
		assert.Contains(t, status.Error.Error(), "Clone")
		assert.Contains(t, status.Error.Error(), "clone error")
	})

	t.Run("pcsm error takes priority", func(t *testing.T) {
		t.Parallel()

		p := &PCSM{
			state:          StateFailed,
			onStateChanged: func(State) {},
			err:            errors.New("pcsm error"),
			clone: &mockCloner{
				doneCh: make(chan struct{}),
				status: clone.Status{Err: errors.New("clone error")},
			},
			repl: &mockReplicator{
				doneCh: make(chan struct{}),
				err:    errors.New("repl error"),
			},
		}

		status := p.Status(context.Background())

		require.Error(t, status.Error)
		assert.Equal(t, "pcsm error", status.Error.Error())
	})

	t.Run("initial sync completed", func(t *testing.T) {
		t.Parallel()

		p := &PCSM{
			state:          StateFailed,
			onStateChanged: func(State) {},
			clone: &mockCloner{
				doneCh: make(chan struct{}),
				status: clone.Status{
					FinishTS: bson.Timestamp{T: 100, I: 1},
				},
			},
			repl: &mockReplicator{
				doneCh:     make(chan struct{}),
				startTime:  time.Now(),
				lastOpTime: bson.Timestamp{T: 200, I: 1}, // After clone finish
			},
		}

		status := p.Status(context.Background())

		assert.True(t, status.InitialSyncCompleted)
	})

	t.Run("initial sync not completed", func(t *testing.T) {
		t.Parallel()

		p := &PCSM{
			state:          StateFailed,
			onStateChanged: func(State) {},
			clone: &mockCloner{
				doneCh: make(chan struct{}),
				status: clone.Status{
					FinishTS: bson.Timestamp{T: 200, I: 1},
				},
			},
			repl: &mockReplicator{
				doneCh:     make(chan struct{}),
				startTime:  time.Now(),
				lastOpTime: bson.Timestamp{T: 100, I: 1}, // Before clone finish
			},
		}

		status := p.Status(context.Background())

		assert.False(t, status.InitialSyncCompleted)
	})

	t.Run("initial sync not completed when repl not started", func(t *testing.T) {
		t.Parallel()

		p := &PCSM{
			state:          StateFailed,
			onStateChanged: func(State) {},
			clone: &mockCloner{
				doneCh: make(chan struct{}),
				status: clone.Status{
					FinishTS: bson.Timestamp{T: 100, I: 1},
				},
			},
			repl: &mockReplicator{
				doneCh: make(chan struct{}),
				// startTime is zero - repl not started
			},
		}

		status := p.Status(context.Background())

		assert.False(t, status.InitialSyncCompleted)
	})

	// Note: TotalLagTimeSeconds calculation requires topo.ClusterTime() with real MongoDB.
	// This is tested via E2E tests.
}
