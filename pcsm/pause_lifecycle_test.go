package pcsm //nolint:testpackage // Exercises the execution owner and its monitor joins.

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/util"
)

func runningDrainPipeline(t *testing.T, cln Cloner) (*PCSM, *drainingReplicator, <-chan struct{}) {
	t.Helper()
	r := &drainingReplicator{
		doneObserved: make(chan struct{}),
		tailObserved: make(chan struct{}),
		releaseTail:  make(chan struct{}),
	}
	r.doneCh = make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-r.doneCh:
		default:
			close(r.doneCh)
		}
	})
	r.startCalled = make(chan struct{}, 1)
	close(r.releaseTail)
	p := &PCSM{
		lifecycleCtx: t.Context(), state: StatePaused, clone: cln, repl: r,
		catalog:        catalog.NewCatalog(nil, nil, mdb.ServerVersion{}),
		onStateChanged: func(State) {},
	}
	if c, ok := cln.(interface{ attachRepl(r *drainingReplicator) }); ok {
		c.attachRepl(r)
	}
	require.NoError(t, p.Resume(t.Context(), ResumeOptions{}))
	<-r.doneObserved
	<-r.startCalled

	return p, r, p.runDone
}

func TestPause_PublishesPausedAfterMonitorCleanup(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		cln := &exitingMonitorCloner{
			observed: make(chan struct{}), release: make(chan struct{}),
		}
		cln.status = finishedLifecycleCloner().status
		cln.status.FinishTS = bson.Timestamp{T: 2}
		release := sync.OnceFunc(func() { close(cln.release) })
		defer release()
		p, r, done := runningDrainPipeline(t, cln)
		<-cln.observed

		require.NoError(t, p.Pause(t.Context()))
		require.NoError(t, p.Pause(t.Context()))
		state, _ := pipelineState(p)
		require.Equal(t, State(StatePausing), state)
		r.settled.Store(true)
		close(r.doneCh)
		synctest.Wait()

		state, _ = pipelineState(p)
		require.Equal(t, State(StatePausing), state, "monitor still owns the execution")
		select {
		case <-done:
			require.FailNow(t, "run completed before monitor cleanup")
		default:
		}

		release()
		<-done
		state, err := pipelineState(p)
		require.Equal(t, State(StatePaused), state)
		require.NoError(t, err)
	})
}

func TestResume_PendingPauseCancellationThenSuccess(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		p, r, done := runningDrainPipeline(t, finishedLifecycleCloner())
		require.NoError(t, p.Pause(t.Context()))
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- p.Resume(ctx, ResumeOptions{}) }()
		synctest.Wait()
		select {
		case err := <-result:
			require.FailNow(t, "resume returned before pause completed", "%v", err)
		default:
		}
		cancel()
		require.ErrorIs(t, <-result, context.Canceled)
		state, _ := pipelineState(p)
		require.Equal(t, State(StatePausing), state)

		r.settled.Store(true)
		close(r.doneCh)
		<-done
		r.settled.Store(false)
		r.pausing.Store(false)
		r.doneCh = make(chan struct{})
		require.NoError(t, p.Resume(t.Context(), ResumeOptions{}))
		<-r.startCalled
		p.lock.Lock()
		resumedDone := p.runDone
		p.lock.Unlock()
		state, _ = pipelineState(p)
		require.Equal(t, State(StateRunning), state)
		close(r.doneCh)
		<-resumedDone
	})
}

func TestResume_JoinsReportedFailureBeforeRunStops(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		p, r, done := runningDrainPipeline(t, finishedLifecycleCloner())
		r.drainError = errors.New("oplog history is lost")
		r.failureReported.Store(true)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- p.Resume(ctx, ResumeOptions{ResumeFromFailure: true}) }()
		synctest.Wait()
		select {
		case err := <-result:
			require.FailNow(t, "failure recovery returned before execution stopped", "%v", err)
		default:
		}

		cancel()
		require.ErrorIs(t, <-result, context.Canceled)
		r.settled.Store(true)
		close(r.doneCh)
		<-done
		state, err := pipelineState(p)
		require.Equal(t, State(StateFailed), state)
		require.ErrorIs(t, err, r.drainError)
	})
}

func TestPauseOnInitialSync_DoesNotJoinOwnExecution(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := &drainingReplicator{}
		p := &PCSM{
			state: StateRunning, pauseOnInitialSync: true,
			clone: finishedLifecycleCloner(), repl: r,
			runDone: make(chan struct{}), onStateChanged: func(State) {},
		}
		monitorDone := make(chan struct{})
		go func() {
			p.monitorInitialSync(t.Context())
			close(monitorDone)
		}()
		synctest.Wait()
		r.lastOp.Store(2)
		// The monitor's own ticker drives automatic pause in virtual time.
		<-monitorDone
		state, _ := pipelineState(p)
		require.Equal(t, State(StatePausing), state)
		require.True(t, r.pausing.Load())
	})
}

func TestFinalize_JoinsPauseAndMonitors(t *testing.T) {
	t.Parallel()
	for _, overlap := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "overlapping"}[overlap], func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				cln := &exitingMonitorCloner{
					observed: make(chan struct{}), release: make(chan struct{}),
				}
				cln.status = finishedLifecycleCloner().status
				cln.status.FinishTS = bson.Timestamp{T: 2}
				release := sync.OnceFunc(func() { close(cln.release) })
				defer release()
				p, r, done := runningDrainPipeline(t, cln)
				<-cln.observed
				r.lastOp.Store(3)
				if overlap {
					require.NoError(t, p.Pause(t.Context()))
				}
				fin := &gatedFinalizer{entered: make(chan struct{})}
				p.finalizer = fin
				result := make(chan error, 1)
				go func() { result <- p.Finalize(t.Context()) }()
				synctest.Wait()
				require.True(t, r.pausing.Load())
				state, _ := pipelineState(p)
				require.Equal(t, State(StatePausing), state)

				r.settled.Store(true)
				close(r.doneCh)
				synctest.Wait()
				select {
				case err := <-result:
					require.FailNow(t, "finalize returned before monitor cleanup", "%v", err)
				default:
				}
				require.Zero(t, fin.calls.Load())
				release()
				<-done
				require.NoError(t, <-result)
				<-fin.entered
				err := p.Stop(t.Context())
				require.NoError(t, err)
			})
		})
	}
}

func TestFinalize_PauseTimeoutHasNoContinuation(t *testing.T) {
	t.Parallel()
	for _, overlap := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "overlapping"}[overlap], func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				p, r, done := runningDrainPipeline(t, finishedLifecycleCloner())
				r.lastOp.Store(2)
				if overlap {
					require.NoError(t, p.Pause(t.Context()))
				}
				fin := &gatedFinalizer{entered: make(chan struct{})}
				p.finalizer = fin
				result := make(chan error, 1)
				go func() { result <- util.CtxWithTimeout(t.Context(), time.Nanosecond, p.Finalize) }()
				require.ErrorIs(t, <-result, context.DeadlineExceeded)
				state, _ := pipelineState(p)
				require.Equal(t, State(StatePausing), state)
				r.settled.Store(true)
				close(r.doneCh)
				<-done
				synctest.Wait()
				require.Zero(t, fin.calls.Load(), "expired waiter must not continue finalization")
				state, _ = pipelineState(p)
				require.Equal(t, State(StatePaused), state)
				require.NoError(t, p.Finalize(t.Context()))
				<-fin.entered
				err := p.Stop(t.Context())
				require.NoError(t, err)
			})
		})
	}
}

// Gate the waiter's select before it observes runDone, so Resume can
// deterministically win the lifecycle lock after the old execution exits.
type gatedJoinContext struct {
	entered chan struct{}
	release chan struct{}
}

func (*gatedJoinContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (*gatedJoinContext) Err() error                  { return nil }
func (*gatedJoinContext) Value(any) any               { return nil }

func (c *gatedJoinContext) Done() <-chan struct{} {
	close(c.entered)
	<-c.release

	return nil
}

func TestFinalize_RejectsStalePauseAfterConcurrentResume(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		p, r, done := runningDrainPipeline(t, finishedLifecycleCloner())
		r.lastOp.Store(2)
		ctx := &gatedJoinContext{
			entered: make(chan struct{}), release: make(chan struct{}),
		}
		release := sync.OnceFunc(func() { close(ctx.release) })
		defer release()
		fin := &gatedFinalizer{entered: make(chan struct{})}
		p.finalizer = fin
		defer func() {
			err := p.Stop(t.Context())
			require.NoError(t, err)
		}()
		result := make(chan error, 1)
		go func() { result <- p.Finalize(ctx) }()
		<-ctx.entered
		r.settled.Store(true)
		close(r.doneCh)
		<-done

		r.settled.Store(false)
		r.pausing.Store(false)
		r.doneCh = make(chan struct{})
		require.NoError(t, p.Resume(t.Context(), ResumeOptions{}))
		<-r.startCalled
		p.lock.Lock()
		newDone := p.runDone
		p.lock.Unlock()
		require.NoError(t, p.Pause(t.Context()))
		r.settled.Store(true)
		close(r.doneCh)
		<-newDone

		release()
		require.ErrorContains(t, <-result, "execution changed")
		require.Zero(t, fin.calls.Load())
		state, _ := pipelineState(p)
		require.Equal(t, State(StatePaused), state)
	})
}

func TestFinalize_ConcurrentPauseWaitersOwnOneFinalizer(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		p, r, done := runningDrainPipeline(t, finishedLifecycleCloner())
		r.lastOp.Store(2)
		fin := &gatedFinalizer{entered: make(chan struct{})}
		p.finalizer = fin
		defer func() {
			err := p.Stop(t.Context())
			require.NoError(t, err)
		}()
		result := make(chan error, 2)
		for range 2 {
			go func() { result <- p.Finalize(t.Context()) }()
		}
		synctest.Wait()
		require.Zero(t, fin.calls.Load())
		r.settled.Store(true)
		close(r.doneCh)
		<-done
		first, second := <-result, <-result
		if first == nil {
			require.ErrorContains(t, second, "finalization is already in progress")
		} else {
			require.ErrorContains(t, first, "finalization is already in progress")
			require.NoError(t, second)
		}
		<-fin.entered
		require.EqualValues(t, 1, fin.calls.Load())
	})
}

func TestFinalize_RechecksFailureAfterPreviousRun(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		p, r, release := pausedPipelineWithExitingRun(t, finishedLifecycleCloner())
		defer release()
		r.lastOp.Store(2)
		fin := &gatedFinalizer{entered: make(chan struct{})}
		p.finalizer = fin
		result := make(chan error, 1)
		go func() { result <- p.Finalize(WithEpoch(t.Context(), t.Context())) }()
		synctest.Wait()
		select {
		case err := <-result:
			require.FailNow(t, "finalize returned before run reported its failure", "%v", err)
		default:
		}
		release()
		require.ErrorIs(t, <-result, r.drainError)
		state, err := pipelineState(p)
		require.Equal(t, State(StateFailed), state)
		require.ErrorIs(t, err, r.drainError)
		require.Zero(t, fin.calls.Load())
	})
}
