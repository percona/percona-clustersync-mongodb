package pcsm //nolint:testpackage // Pins admission against the unexported execution owner.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/percona/percona-clustersync-mongodb/pcsm/repl"
	"github.com/percona/percona-clustersync-mongodb/util"
)

type admissionReplicator struct {
	*mockReplicator

	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (r *admissionReplicator) Status() repl.Status {
	status := r.mockReplicator.Status()
	r.once.Do(func() {
		close(r.entered)
		<-r.release
	})

	return status
}

func TestExecution_RefusesAdmissionAfterTeardown(t *testing.T) {
	t.Parallel()

	for _, operation := range []string{"resume", "finalize"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()

			require.NoError(t, util.CtxWithTimeout(t.Context(), 5*time.Second, func(ctx context.Context) error {
				p := recoveredFinalizingPipeline(ctx, t)
				mock, ok := p.repl.(*mockReplicator)
				require.True(t, ok)
				r := &admissionReplicator{
					mockReplicator: mock,
					entered:        make(chan struct{}),
					release:        make(chan struct{}),
				}
				close(r.doneCh)
				p.repl = r
				if operation == "resume" {
					p.state = StatePaused
				}
				before := p.state
				epoch, cancel := context.WithCancel(ctx)
				defer cancel()
				release := sync.OnceFunc(func() { close(r.release) })
				defer release()
				result := make(chan error, 1)

				// Status blocks after the epoch check but before execution registration.
				go func() {
					request := WithEpoch(ctx, epoch)
					if operation == "resume" {
						result <- p.Resume(request, ResumeOptions{})
					} else {
						result <- p.Finalize(request)
					}
				}()
				awaitEvent(ctx, t, r.entered, "request did not reach admission window")

				cancel()
				require.NoError(t, p.Stop(ctx), "no execution has been registered yet")
				release()
				err := awaitStop(ctx, t, result)
				require.NoError(t, p.Stop(ctx), "join any incorrectly admitted execution before asserting")

				require.ErrorIs(t, err, ErrNotActive)
				state, _ := pipelineState(p)
				require.Equal(t, before, state)
				require.Nil(t, p.runExec)
				require.Nil(t, p.finalizeExec)

				return nil
			}))
		})
	}
}

func TestStop_JoinsAdmittedExecutionBeforeLaunch(t *testing.T) {
	t.Parallel()

	require.NoError(t, util.CtxWithTimeout(t.Context(), 5*time.Second, func(ctx context.Context) error {
		cln, rpl := newRunMocks()
		close(cln.doneCh)
		p := &PCSM{
			lifecycleCtx: ctx, clone: cln, repl: rpl,
			state: StateRunning, onStateChanged: func(State) {},
		}

		p.lock.Lock()
		execCtx, execution, err := p.admitExecution(ctx, &p.runExec)
		p.lock.Unlock()
		require.NoError(t, err)

		result := stopAsync(ctx, p)
		awaitEvent(ctx, t, execCtx.Done(), "Stop did not cancel the admitted execution")
		select {
		case err := <-result:
			require.FailNow(t, "Stop returned before the execution launched and joined", "%v", err)
		default:
		}

		p.lock.Lock()
		p.startRun(execCtx, execution)
		p.lock.Unlock()
		require.NoError(t, awaitStop(ctx, t, result))
		require.ErrorIs(t, cln.lastCtx().Err(), context.Canceled)
		require.Nil(t, p.runExec)

		return nil
	}))
}
