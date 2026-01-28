package pcsm //nolint:testpackage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
				p.repl = &Repl{
					pauseCh: make(chan struct{}),
					doneCh:  make(chan struct{}),
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
				p.repl = &Repl{
					pauseCh: make(chan struct{}),
					doneCh:  make(chan struct{}),
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

func TestResumeFromPaused_FailsWhenReplNotStarted(t *testing.T) {
	t.Parallel()

	p := &PCSM{
		state:          StatePaused,
		onStateChanged: func(State) {},
		repl: &Repl{
			pauseCh: make(chan struct{}),
			doneCh:  make(chan struct{}),
		},
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
		repl: &Repl{
			pauseCh:   make(chan struct{}),
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
