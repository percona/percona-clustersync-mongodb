package pcsm //nolint:testpackage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/pcsm/clone"
	"github.com/percona/percona-clustersync-mongodb/pcsm/repl"
)

// MockCloner is a test double for the Cloner interface.
type MockCloner struct {
	doneCh chan struct{}
}

func (m *MockCloner) Start(context.Context) error     { return nil }
func (m *MockCloner) Done() <-chan struct{}           { return m.doneCh }
func (m *MockCloner) Status() clone.Status            { return clone.Status{} }
func (m *MockCloner) Checkpoint() *clone.Checkpoint   { return nil }
func (m *MockCloner) Recover(*clone.Checkpoint) error { return nil }
func (m *MockCloner) ResetError()                     {}

// MockReplicator is a test double for the Replicator interface.
type MockReplicator struct {
	doneCh    chan struct{}
	startTime time.Time
	pauseTime time.Time
}

func (m *MockReplicator) Start(context.Context, bson.Timestamp) error { return nil }
func (m *MockReplicator) Pause(context.Context) error                 { return nil }
func (m *MockReplicator) Resume(context.Context) error                { return nil }
func (m *MockReplicator) Done() <-chan struct{}                       { return m.doneCh }
func (m *MockReplicator) Status() repl.Status {
	return repl.Status{StartTime: m.startTime, PauseTime: m.pauseTime}
}
func (m *MockReplicator) Checkpoint() *repl.Checkpoint   { return nil }
func (m *MockReplicator) Recover(*repl.Checkpoint) error { return nil }
func (m *MockReplicator) ResetError()                    {}

func TestStart_StateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initialState  State
		errorContains string
	}{
		{
			name:          "fails from running state",
			initialState:  StateRunning,
			errorContains: "already running",
		},
		{
			name:          "fails from paused state",
			initialState:  StatePaused,
			errorContains: "paused",
		},
		{
			name:          "fails from failed state",
			initialState:  StateFailed,
			errorContains: "already running",
		},
		{
			name:          "fails from finalizing state",
			initialState:  StateFinalizing,
			errorContains: "already running",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &PCSM{
				state:          tt.initialState,
				onStateChanged: func(State) {},
			}

			err := p.Start(context.Background(), nil)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errorContains)
			assert.Equal(t, tt.initialState, p.state)
		})
	}
}

func TestPause_StateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initialState  State
		setupRepl     bool
		errorContains string
	}{
		{
			name:          "fails from idle state",
			initialState:  StateIdle,
			errorContains: "not running",
		},
		{
			name:          "fails from paused state",
			initialState:  StatePaused,
			errorContains: "not running",
		},
		{
			name:          "fails from failed state",
			initialState:  StateFailed,
			errorContains: "not running",
		},
		{
			name:          "fails from finalizing state",
			initialState:  StateFinalizing,
			errorContains: "not running",
		},
		{
			name:          "fails from finalized state",
			initialState:  StateFinalized,
			errorContains: "not running",
		},
		{
			name:          "fails from running when repl not actually running",
			initialState:  StateRunning,
			setupRepl:     true,
			errorContains: "Change Replication is not running",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &PCSM{
				state:          tt.initialState,
				onStateChanged: func(State) {},
			}

			if tt.setupRepl {
				p.repl = &MockReplicator{
					doneCh: make(chan struct{}),
				}
			}

			err := p.Pause(context.Background())

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errorContains)
		})
	}
}

func TestResume_StateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initialState  State
		fromFailure   bool
		errorContains string
	}{
		{
			name:          "fails from idle state",
			initialState:  StateIdle,
			errorContains: "cannot resume",
		},
		{
			name:          "fails from running state",
			initialState:  StateRunning,
			errorContains: "cannot resume",
		},
		{
			name:          "fails from failed state without fromFailure flag",
			initialState:  StateFailed,
			errorContains: "cannot resume",
		},
		{
			name:          "fails from finalizing state",
			initialState:  StateFinalizing,
			errorContains: "cannot resume",
		},
		{
			name:          "fails from finalized state",
			initialState:  StateFinalized,
			errorContains: "cannot resume",
		},
		{
			name:          "fails from idle even with fromFailure flag",
			initialState:  StateIdle,
			fromFailure:   true,
			errorContains: "cannot resume",
		},
		{
			name:          "fails from running even with fromFailure flag",
			initialState:  StateRunning,
			fromFailure:   true,
			errorContains: "cannot resume",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &PCSM{
				state:          tt.initialState,
				onStateChanged: func(State) {},
			}

			if tt.initialState == StatePaused || tt.initialState == StateFailed {
				p.repl = &MockReplicator{
					doneCh: make(chan struct{}),
				}
			}

			err := p.Resume(context.Background(), ResumeOptions{
				ResumeFromFailure: tt.fromFailure,
			})

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errorContains)
		})
	}
}

func TestFinalize_FailsFromFailedStateWithoutIgnoreHistoryLost(t *testing.T) {
	t.Parallel()

	p := &PCSM{
		state:          StateFailed,
		onStateChanged: func(State) {},
		err:            repl.ErrOplogHistoryLost,
		clone:          &MockCloner{doneCh: make(chan struct{})},
		repl:           &MockReplicator{doneCh: make(chan struct{})},
	}

	err := p.Finalize(context.Background(), FinalizeOptions{
		IgnoreHistoryLost: false,
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed state")
}

func TestResumeFromPaused_FailsWhenReplNotStarted(t *testing.T) {
	t.Parallel()

	p := &PCSM{
		state:          StatePaused,
		onStateChanged: func(State) {},
		repl:           &MockReplicator{doneCh: make(chan struct{})},
	}

	err := p.Resume(context.Background(), ResumeOptions{
		ResumeFromFailure: false,
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "replication is not started")
}

func TestResumeFromFailed_FailsWhenReplNotPaused(t *testing.T) {
	t.Parallel()

	p := &PCSM{
		state:          StateFailed,
		onStateChanged: func(State) {},
		repl: &MockReplicator{
			doneCh:    make(chan struct{}),
			startTime: time.Now(),
		},
	}

	err := p.Resume(context.Background(), ResumeOptions{
		ResumeFromFailure: true,
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "replication is not paused")
}
