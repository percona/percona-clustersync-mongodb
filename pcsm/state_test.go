package pcsm //nolint:testpackage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartCommand_StateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initialState  State
		expectError   bool
		errorContains string
	}{
		{
			name:          "fails from running state",
			initialState:  StateRunning,
			expectError:   true,
			errorContains: "already running",
		},
		{
			name:          "fails from paused state",
			initialState:  StatePaused,
			expectError:   true,
			errorContains: "paused",
		},
		{
			name:          "fails from failed state",
			initialState:  StateFailed,
			expectError:   true,
			errorContains: "already running",
		},
		{
			name:          "fails from finalizing state",
			initialState:  StateFinalizing,
			expectError:   true,
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

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
				assert.Equal(t, tt.initialState, p.state)
			}
		})
	}
}

func TestPauseCommand_StateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initialState  State
		setupRepl     bool
		replRunning   bool
		expectError   bool
		errorContains string
	}{
		{
			name:          "fails from idle state",
			initialState:  StateIdle,
			expectError:   true,
			errorContains: "not running",
		},
		{
			name:          "fails from paused state",
			initialState:  StatePaused,
			expectError:   true,
			errorContains: "not running",
		},
		{
			name:          "fails from failed state",
			initialState:  StateFailed,
			expectError:   true,
			errorContains: "not running",
		},
		{
			name:          "fails from finalizing state",
			initialState:  StateFinalizing,
			expectError:   true,
			errorContains: "not running",
		},
		{
			name:          "fails from finalized state",
			initialState:  StateFinalized,
			expectError:   true,
			errorContains: "not running",
		},
		{
			name:          "fails from running when repl not actually running",
			initialState:  StateRunning,
			setupRepl:     true,
			replRunning:   false,
			expectError:   true,
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
					pauseC:  make(chan struct{}),
					doneSig: make(chan struct{}),
				}
				if tt.replRunning {
					p.repl.startTime = time.Now()
				}
			}

			err := p.Pause(context.Background())

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
			}
		})
	}
}

func TestResumeCommand_StateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initialState  State
		fromFailure   bool
		expectError   bool
		errorContains string
	}{
		{
			name:          "fails from idle state",
			initialState:  StateIdle,
			fromFailure:   false,
			expectError:   true,
			errorContains: "cannot resume",
		},
		{
			name:          "fails from running state",
			initialState:  StateRunning,
			fromFailure:   false,
			expectError:   true,
			errorContains: "cannot resume",
		},
		{
			name:          "fails from failed state without fromFailure flag",
			initialState:  StateFailed,
			fromFailure:   false,
			expectError:   true,
			errorContains: "cannot resume",
		},
		{
			name:          "fails from finalizing state",
			initialState:  StateFinalizing,
			fromFailure:   false,
			expectError:   true,
			errorContains: "cannot resume",
		},
		{
			name:          "fails from finalized state",
			initialState:  StateFinalized,
			fromFailure:   false,
			expectError:   true,
			errorContains: "cannot resume",
		},
		{
			name:          "fails from idle even with fromFailure flag",
			initialState:  StateIdle,
			fromFailure:   true,
			expectError:   true,
			errorContains: "cannot resume",
		},
		{
			name:          "fails from running even with fromFailure flag",
			initialState:  StateRunning,
			fromFailure:   true,
			expectError:   true,
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
					pauseC:  make(chan struct{}),
					doneSig: make(chan struct{}),
				}
			}

			err := p.Resume(context.Background(), ResumeOptions{
				ResumeFromFailure: tt.fromFailure,
			})

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
			}
		})
	}
}

func TestFinalizeCommand_FailedStateValidation(t *testing.T) {
	t.Parallel()

	t.Run("fails from failed state without ignoreHistoryLost", func(t *testing.T) {
		t.Parallel()

		p := &PCSM{
			state:          StateFailed,
			onStateChanged: func(State) {},
			err:            ErrOplogHistoryLost,
			clone: &Clone{
				doneSig: make(chan struct{}),
			},
			repl: &Repl{
				pauseC:  make(chan struct{}),
				doneSig: make(chan struct{}),
			},
		}

		err := p.Finalize(context.Background(), FinalizeOptions{
			IgnoreHistoryLost: false,
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed state")
	})
}

func TestResumeFromPaused_ValidatesReplState(t *testing.T) {
	t.Parallel()

	t.Run("fails when repl not started and not resuming from failure", func(t *testing.T) {
		t.Parallel()

		p := &PCSM{
			state:          StatePaused,
			onStateChanged: func(State) {},
			repl: &Repl{
				pauseC:  make(chan struct{}),
				doneSig: make(chan struct{}),
			},
		}

		err := p.Resume(context.Background(), ResumeOptions{
			ResumeFromFailure: false,
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "replication is not started")
	})
}

func TestResumeFromFailed_ValidatesReplState(t *testing.T) {
	t.Parallel()

	t.Run("fails when repl not paused but resuming from failure", func(t *testing.T) {
		t.Parallel()

		p := &PCSM{
			state:          StateFailed,
			onStateChanged: func(State) {},
			repl: &Repl{
				pauseC:    make(chan struct{}),
				doneSig:   make(chan struct{}),
				startTime: time.Now(),
			},
		}

		err := p.Resume(context.Background(), ResumeOptions{
			ResumeFromFailure: true,
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "replication is not paused")
	})
}
