package pcsm //nolint:testpackage // Drives Suspend against the unexported run and finalize lifecycle.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/pcsm/clone"
	"github.com/percona/percona-clustersync-mongodb/util"
)

type suspendResult struct {
	suspended bool
	err       error
}

func awaitEvent[T any](ctx context.Context, t *testing.T, ch <-chan T, msg string) {
	t.Helper()

	select {
	case <-ch:
	case <-ctx.Done():
		require.FailNow(t, msg, ctx.Err().Error())
	}
}

func awaitSuspend(ctx context.Context, t *testing.T, res <-chan suspendResult) suspendResult {
	t.Helper()

	select {
	case r := <-res:
		return r
	case <-ctx.Done():
		require.FailNow(t, "Suspend did not return", ctx.Err().Error())

		return suspendResult{}
	}
}

func suspendAsync(ctx context.Context, p *PCSM) <-chan suspendResult {
	res := make(chan suspendResult, 1)

	go func() {
		suspended, err := p.Suspend(ctx)
		res <- suspendResult{suspended: suspended, err: err}
	}()

	return res
}

func newSuspendMocks() (*mockCloner, *mockReplicator) {
	cln := &mockCloner{doneCh: make(chan struct{}), startCalled: make(chan struct{}, 1)}
	rpl := &mockReplicator{
		doneCh: make(chan struct{}), startCalled: make(chan struct{}, 1), pauseCalled: make(chan struct{}, 1),
	}

	return cln, rpl
}

func finishedCloneStatus() clone.Status {
	return clone.Status{StartTime: time.Unix(1, 0), FinishTime: time.Unix(2, 0), FinishTS: bson.Timestamp{T: 1}}
}

// startRunning launches run the way Start and doResume do, without their
// state-change notification, and returns the run's completion channel.
func startRunning(p *PCSM) <-chan struct{} {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.state = StateRunning
	p.startRun(p.lifecycleCtx)

	return p.runDone
}

func pipelineState(p *PCSM) (State, bool, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	return p.state, p.suspended, p.err
}

func TestSuspend_IdleIsNoop(t *testing.T) {
	t.Parallel()

	p := &PCSM{lifecycleCtx: t.Context(), state: StateIdle, onStateChanged: func(State) {}}

	suspended, err := p.Suspend(t.Context())
	require.NoError(t, err)
	assert.False(t, suspended)

	state, flagged, _ := pipelineState(p)
	assert.Equal(t, State(StateIdle), state)
	assert.False(t, flagged)
}

// TestSuspend_DuringCloneResumesInMemory pins the clone phase: Suspend cancels
// the clone's context and settles paused+suspended without starting
// replication; Resume then continues the clone through clone.Resume and hands
// over to replication.
func TestSuspend_DuringCloneResumesInMemory(t *testing.T) {
	t.Parallel()

	require.NoError(t, util.CtxWithTimeout(t.Context(), 5*time.Second, func(ctx context.Context) error {
		cln, rpl := newSuspendMocks()
		p := &PCSM{lifecycleCtx: ctx, source: unreachableSource(t), clone: cln, repl: rpl, onStateChanged: func(State) {}}

		// Given: the run is inside the clone.
		done := startRunning(p)
		awaitEvent(ctx, t, cln.startCalled, "run did not start the clone")
		assert.False(t, cln.resumed())

		// When: the instance is suspended; the clone ends on cancellation alone.
		res := suspendAsync(ctx, p)
		awaitEvent(ctx, t, cln.lastCtx().Done(), "suspension did not cancel the clone's context")
		close(cln.doneCh)
		awaitEvent(ctx, t, done, "run did not exit after the clone stopped")

		// Then: paused and suspended, nothing failed, replication never started.
		r := awaitSuspend(ctx, t, res)
		require.NoError(t, r.err)
		assert.True(t, r.suspended)

		state, flagged, perr := pipelineState(p)
		assert.Equal(t, State(StatePaused), state)
		assert.True(t, flagged)
		require.NoError(t, perr)
		assert.Nil(t, rpl.lastCtx(), "replication must not start after a suspension")

		// And: a same-term resume continues the clone rather than starting over.
		cln.doneCh = make(chan struct{})
		cln.status.StartTime = time.Unix(1, 0)
		require.NoError(t, p.Resume(ctx, ResumeOptions{}))

		p.lock.Lock()
		resumedDone := p.runDone
		p.lock.Unlock()

		awaitEvent(ctx, t, cln.startCalled, "resumed run did not continue the clone")
		assert.True(t, cln.resumed(), "a suspended clone must be resumed, not started again")

		cln.status = finishedCloneStatus()
		close(cln.doneCh)
		awaitEvent(ctx, t, rpl.startCalled, "replication did not start after the resumed clone")

		_, flagged, _ = pipelineState(p)
		assert.False(t, flagged, "resume must consume the suspension")

		close(rpl.doneCh)
		awaitEvent(ctx, t, resumedDone, "resumed run did not exit")

		return nil
	}))
}

// TestSuspend_DuringReplPausesAtFloor pins the replication phase: Suspend
// cancels replication's context and settles paused+suspended with no error.
func TestSuspend_DuringReplPausesAtFloor(t *testing.T) {
	t.Parallel()

	require.NoError(t, util.CtxWithTimeout(t.Context(), 5*time.Second, func(ctx context.Context) error {
		cln, rpl := newSuspendMocks()
		cln.status = finishedCloneStatus()
		p := &PCSM{lifecycleCtx: ctx, source: unreachableSource(t), clone: cln, repl: rpl, onStateChanged: func(State) {}}

		// Given: replication is running.
		done := startRunning(p)
		awaitEvent(ctx, t, rpl.startCalled, "run did not start replication")

		// When: the instance is suspended; replication ends on cancellation alone.
		res := suspendAsync(ctx, p)
		awaitEvent(ctx, t, rpl.lastCtx().Done(), "suspension did not cancel replication's context")
		close(rpl.doneCh)
		awaitEvent(ctx, t, done, "run did not exit after replication stopped")

		// Then: paused and suspended, nothing failed.
		r := awaitSuspend(ctx, t, res)
		require.NoError(t, r.err)
		assert.True(t, r.suspended)

		state, flagged, perr := pipelineState(p)
		assert.Equal(t, State(StatePaused), state)
		assert.True(t, flagged)
		require.NoError(t, perr)

		return nil
	}))
}

// TestSuspend_OperatorPauseIsNotASuspension pins that a pause requested by
// the operator keeps its meaning: Suspend ends the drain but reports no
// suspension, so a re-promotion will not resume what the operator stopped.
func TestSuspend_OperatorPauseIsNotASuspension(t *testing.T) {
	t.Parallel()

	require.NoError(t, util.CtxWithTimeout(t.Context(), 5*time.Second, func(ctx context.Context) error {
		cln, rpl := newSuspendMocks()
		cln.status = finishedCloneStatus()
		rpl.startTime = time.Unix(1, 0)
		p := &PCSM{lifecycleCtx: ctx, source: unreachableSource(t), clone: cln, repl: rpl, onStateChanged: func(State) {}}

		// Given: the operator paused the pipeline and the drain is still running.
		done := startRunning(p)
		awaitEvent(ctx, t, rpl.startCalled, "run did not resume replication")
		require.NoError(t, p.Pause(ctx))
		awaitEvent(ctx, t, rpl.pauseCalled, "pause did not reach the replicator")

		// When: the instance is suspended during the drain.
		res := suspendAsync(ctx, p)
		awaitEvent(ctx, t, rpl.lastCtx().Done(), "suspension did not cancel the drain")
		close(rpl.doneCh)
		awaitEvent(ctx, t, done, "run did not exit after the drain was canceled")

		// Then: the operator's pause stands and is not reported as a suspension.
		r := awaitSuspend(ctx, t, res)
		require.NoError(t, r.err)
		assert.False(t, r.suspended)

		state, flagged, perr := pipelineState(p)
		assert.Equal(t, State(StatePaused), state)
		assert.False(t, flagged)
		require.NoError(t, perr)

		return nil
	}))
}

// TestSuspend_GenuineFailureRacingSuspensionStaysFailed pins invariant 5: a
// component error recorded while the suspension cancels it is a failure, and
// the suspension must not erase it.
func TestSuspend_GenuineFailureRacingSuspensionStaysFailed(t *testing.T) {
	t.Parallel()

	require.NoError(t, util.CtxWithTimeout(t.Context(), 5*time.Second, func(ctx context.Context) error {
		cln, rpl := newSuspendMocks()
		p := &PCSM{lifecycleCtx: ctx, source: unreachableSource(t), clone: cln, repl: rpl, onStateChanged: func(State) {}}

		done := startRunning(p)
		awaitEvent(ctx, t, cln.startCalled, "run did not start the clone")

		// When: the clone records a real error while the suspension cancels it.
		res := suspendAsync(ctx, p)
		awaitEvent(ctx, t, cln.lastCtx().Done(), "suspension did not cancel the clone's context")
		cloneErr := errors.New("insert failed")
		cln.status.Err = cloneErr
		close(cln.doneCh)
		awaitEvent(ctx, t, done, "run did not exit")

		// Then: the failure is kept and no suspension is reported.
		r := awaitSuspend(ctx, t, res)
		require.NoError(t, r.err)
		assert.False(t, r.suspended)

		state, flagged, perr := pipelineState(p)
		assert.Equal(t, State(StateFailed), state)
		assert.False(t, flagged)
		require.ErrorIs(t, perr, cloneErr)

		return nil
	}))
}

func TestSuspend_FailedIsUntouched(t *testing.T) {
	t.Parallel()

	failure := errors.New("earlier failure")
	p := &PCSM{lifecycleCtx: t.Context(), state: StateFailed, err: failure, onStateChanged: func(State) {}}

	suspended, err := p.Suspend(t.Context())
	require.NoError(t, err)
	assert.False(t, suspended)

	state, flagged, perr := pipelineState(p)
	assert.Equal(t, State(StateFailed), state)
	assert.False(t, flagged)
	require.ErrorIs(t, perr, failure)
}

// TestSuspend_CanceledJoinKeepsOwnership pins that a suspension whose join
// is cut short settles nothing and keeps the execution handle, and that a
// later suspension settles the run that has meanwhile exited.
func TestSuspend_CanceledJoinKeepsOwnership(t *testing.T) {
	t.Parallel()

	require.NoError(t, util.CtxWithTimeout(t.Context(), 5*time.Second, func(ctx context.Context) error {
		cln, rpl := newSuspendMocks()
		p := &PCSM{lifecycleCtx: ctx, source: unreachableSource(t), clone: cln, repl: rpl, onStateChanged: func(State) {}}

		done := startRunning(p)
		awaitEvent(ctx, t, cln.startCalled, "run did not start the clone")

		// When: the join context is already dead and the clone has not stopped.
		joinCtx, cancelJoin := context.WithCancel(ctx)
		cancelJoin()

		suspended, err := p.Suspend(joinCtx)

		// Then: an error, nothing settled, the execution still owned.
		require.ErrorIs(t, err, context.Canceled)
		assert.False(t, suspended)

		state, flagged, _ := pipelineState(p)
		assert.Equal(t, State(StateRunning), state)
		assert.False(t, flagged)

		p.execMu.Lock()
		owned := p.runExec != nil
		p.execMu.Unlock()
		assert.True(t, owned, "a cut-short suspension must keep the execution handle")

		// And: the cancellation did reach the clone; once the run has exited on
		// its own, a later suspension settles it.
		awaitEvent(ctx, t, cln.lastCtx().Done(), "the clone's context was not canceled")
		close(cln.doneCh)
		awaitEvent(ctx, t, done, "run did not exit")

		suspended, err = p.Suspend(ctx)
		require.NoError(t, err)
		assert.True(t, suspended)

		state, flagged, _ = pipelineState(p)
		assert.Equal(t, State(StatePaused), state)
		assert.True(t, flagged)

		return nil
	}))
}

// gatedFinalizer blocks its first Finalize until its context is canceled and
// returns at once afterwards, so a test can interrupt one finalization and
// let the next one through.
type gatedFinalizer struct {
	calls   atomic.Int32
	entered chan struct{}
}

func (f *gatedFinalizer) Finalize(ctx context.Context) []catalog.UnsuccessfulIndex {
	if f.calls.Add(1) == 1 {
		close(f.entered)
		<-ctx.Done()
	}

	return nil
}

// TestSuspend_InterruptedFinalizeIsNotCompletedAndRunsAgain pins the
// finalizing phase: a finalizer ended by the suspension leaves the state
// finalizing with Completed false, and an explicit /finalize runs it again.
func TestSuspend_InterruptedFinalizeIsNotCompletedAndRunsAgain(t *testing.T) {
	t.Parallel()

	require.NoError(t, util.CtxWithTimeout(t.Context(), 5*time.Second, func(ctx context.Context) error {
		fin := &gatedFinalizer{entered: make(chan struct{})}
		finalized := make(chan struct{}, 1)
		p := recoveredFinalizingPipeline(t)
		p.finalizer = fin
		p.onStateChanged = func(s State) {
			if s == StateFinalized {
				signalCalled(finalized)
			}
		}

		// Given: catalog finalization is live.
		require.NoError(t, p.Finalize(ctx))
		awaitEvent(ctx, t, fin.entered, "finalizer did not start")

		// When: the instance is suspended.
		suspended, err := p.Suspend(ctx)
		require.NoError(t, err)
		assert.True(t, suspended)

		// Then: still finalizing, not completed, no live finalizer.
		p.lock.Lock()
		state, flagged, active := p.state, p.suspended, p.finalizeActive
		completed := p.finalizeStatus != nil && p.finalizeStatus.Completed
		p.lock.Unlock()

		assert.Equal(t, State(StateFinalizing), state)
		assert.True(t, flagged)
		assert.False(t, active)
		assert.False(t, completed, "an interrupted finalization must not report completion")

		// And: an explicit /finalize runs it again to completion.
		require.NoError(t, p.Finalize(ctx))
		awaitEvent(ctx, t, finalized, "second finalization did not complete")

		p.lock.Lock()
		state = p.state
		completed = p.finalizeStatus != nil && p.finalizeStatus.Completed
		p.lock.Unlock()

		assert.Equal(t, State(StateFinalized), state)
		assert.True(t, completed)
		assert.Equal(t, int32(2), fin.calls.Load())

		return nil
	}))
}

// TestFinalize_RefusesWhenEpochDiesDuringDrain pins C7: a /finalize whose
// epoch ends while it waits for replication to drain starts no catalog work.
func TestFinalize_RefusesWhenEpochDiesDuringDrain(t *testing.T) {
	t.Parallel()

	require.NoError(t, util.CtxWithTimeout(t.Context(), 5*time.Second, func(ctx context.Context) error {
		cln, rpl := newSuspendMocks()
		cln.status = finishedCloneStatus()
		rpl.startTime = time.Unix(1, 0)
		rpl.lastOpTime = bson.Timestamp{T: 2}
		p := &PCSM{
			lifecycleCtx: ctx, source: unreachableSource(t), state: StateRunning,
			clone: cln, repl: rpl, onStateChanged: func(State) {},
		}

		epoch, endEpoch := context.WithCancel(ctx)
		defer endEpoch()

		// Given: /finalize is draining replication under its epoch.
		result := make(chan error, 1)
		go func() { result <- p.Finalize(WithEpoch(ctx, epoch)) }()
		awaitEvent(ctx, t, rpl.pauseCalled, "finalize did not pause replication")

		// When: the epoch ends before the drain completes.
		endEpoch()
		close(rpl.doneCh)

		// Then: refused; no finalizer, no finalize status, state left to Suspend.
		select {
		case err := <-result:
			require.ErrorIs(t, err, ErrNotActive)
		case <-ctx.Done():
			require.FailNow(t, "Finalize did not return", ctx.Err().Error())
		}

		p.lock.Lock()
		state, active, status := p.state, p.finalizeActive, p.finalizeStatus
		p.lock.Unlock()

		assert.Equal(t, State(StateRunning), state)
		assert.False(t, active)
		assert.Nil(t, status)

		return nil
	}))
}

// TestLaunch_RefusesDeadEpochBeforeMutatingState pins C1: a request admitted
// under an epoch that has since ended is refused before any state changes.
func TestLaunch_RefusesDeadEpochBeforeMutatingState(t *testing.T) {
	t.Parallel()

	dead, endEpoch := context.WithCancel(context.Background())
	endEpoch()
	ctx := WithEpoch(context.Background(), dead)

	t.Run("start", func(t *testing.T) {
		t.Parallel()

		p := &PCSM{lifecycleCtx: t.Context(), state: StateIdle, onStateChanged: func(State) {}}

		require.ErrorIs(t, p.Start(ctx, &StartOptions{}), ErrNotActive)

		state, _, _ := pipelineState(p)
		assert.Equal(t, State(StateIdle), state)
		assert.Nil(t, p.clone)
		assert.Nil(t, p.repl)
	})

	t.Run("resume keeps the suspension", func(t *testing.T) {
		t.Parallel()

		cln, rpl := newSuspendMocks()
		cln.status = finishedCloneStatus()
		rpl.startTime = time.Unix(1, 0)
		rpl.pauseTime = time.Unix(2, 0)
		p := &PCSM{
			lifecycleCtx: t.Context(), state: StatePaused, suspended: true,
			clone: cln, repl: rpl, onStateChanged: func(State) {},
		}

		require.ErrorIs(t, p.Resume(ctx, ResumeOptions{}), ErrNotActive)

		state, flagged, _ := pipelineState(p)
		assert.Equal(t, State(StatePaused), state)
		assert.True(t, flagged, "a refused launch must not consume the suspension")
		assert.Nil(t, rpl.lastCtx())
	})

	t.Run("recover", func(t *testing.T) {
		t.Parallel()

		_, rpl := newSuspendMocks()
		p := &PCSM{lifecycleCtx: t.Context(), state: StatePaused, repl: rpl, onStateChanged: func(State) {}}
		data, err := bson.Marshal(checkpoint{State: StateRunning})
		require.NoError(t, err)

		require.ErrorIs(t, p.Recover(ctx, data), ErrNotActive)

		state, _, _ := pipelineState(p)
		assert.Equal(t, State(StatePaused), state)
		require.Same(t, rpl, p.repl)
	})

	t.Run("finalize", func(t *testing.T) {
		t.Parallel()

		p := recoveredFinalizingPipeline(t)

		require.ErrorIs(t, p.Finalize(ctx), ErrNotActive)

		p.lock.Lock()
		active := p.finalizeActive
		p.lock.Unlock()
		assert.False(t, active)
	})
}

// TestRun_PersistsCompletedCloneOnce pins the post-clone checkpoint: one
// running-state notification is issued after the clone completes and before
// replication starts, and none after.
func TestRun_PersistsCompletedCloneOnce(t *testing.T) {
	t.Parallel()

	require.NoError(t, util.CtxWithTimeout(t.Context(), 5*time.Second, func(ctx context.Context) error {
		cln, rpl := newSuspendMocks()
		notified := make(chan State, 4)
		p := &PCSM{
			lifecycleCtx: ctx, source: unreachableSource(t), clone: cln, repl: rpl,
			onStateChanged: func(s State) { notified <- s },
		}

		// Given: the clone runs and completes.
		done := startRunning(p)
		awaitEvent(ctx, t, cln.startCalled, "run did not start the clone")
		cln.status = finishedCloneStatus()
		close(cln.doneCh)

		// Then: a running checkpoint is issued and replication starts.
		awaitEvent(ctx, t, rpl.startCalled, "replication did not start")

		select {
		case s := <-notified:
			assert.Equal(t, State(StateRunning), s)
		case <-ctx.Done():
			require.FailNow(t, "no checkpoint after the clone completed", ctx.Err().Error())
		}

		close(rpl.doneCh)
		awaitEvent(ctx, t, done, "run did not exit")

		select {
		case s := <-notified:
			require.FailNowf(t, "unexpected extra state change", "%s", s)
		default:
		}

		return nil
	}))
}
