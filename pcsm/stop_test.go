package pcsm //nolint:testpackage // Drives Stop against the unexported run and finalize lifecycle.

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

func awaitEvent[T any](ctx context.Context, t *testing.T, ch <-chan T, msg string) {
	t.Helper()

	select {
	case <-ch:
	case <-ctx.Done():
		require.FailNow(t, msg, ctx.Err().Error())
	}
}

func awaitStop(ctx context.Context, t *testing.T, res <-chan error) error {
	t.Helper()

	select {
	case err := <-res:
		return err
	case <-ctx.Done():
		require.FailNow(t, "Stop did not return", ctx.Err().Error())

		return nil
	}
}

func stopAsync(ctx context.Context, p *PCSM) <-chan error {
	res := make(chan error, 1)

	go func() { res <- p.Stop(ctx) }()

	return res
}

func newRunMocks() (*mockCloner, *mockReplicator) {
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

func pipelineState(p *PCSM) (State, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	return p.state, p.err
}

func TestStop_IdleIsNoop(t *testing.T) {
	t.Parallel()

	p := &PCSM{lifecycleCtx: t.Context(), state: StateIdle, onStateChanged: func(State) {}}

	require.NoError(t, p.Stop(t.Context()))

	state, _ := pipelineState(p)
	assert.Equal(t, State(StateIdle), state)
}

// TestStop_DuringCloneCancelsAndJoins pins the clone phase: Stop cancels the
// clone's context, waits for the run to exit and starts no replication. The
// clone ended by that cancellation is not a failure; the pipeline is
// discarded with its tenure, the next one restores the interrupted clone as a
// failed state, and a new /start re-clones.
func TestStop_DuringCloneCancelsAndJoins(t *testing.T) {
	t.Parallel()

	require.NoError(t, util.CtxWithTimeout(t.Context(), 5*time.Second, func(ctx context.Context) error {
		cln, rpl := newRunMocks()
		p := &PCSM{lifecycleCtx: ctx, source: unreachableSource(t), clone: cln, repl: rpl, onStateChanged: func(State) {}}

		// Given: the run is inside the clone.
		done := startRunning(p)
		awaitEvent(ctx, t, cln.startCalled, "run did not start the clone")

		// When: the tenure ends; the clone ends on that cancellation, recording it.
		res := stopAsync(ctx, p)
		awaitEvent(ctx, t, cln.lastCtx().Done(), "Stop did not cancel the clone's context")
		cln.status.Err = cln.lastCtx().Err()
		close(cln.doneCh)
		awaitEvent(ctx, t, done, "run did not exit after the clone stopped")

		// Then: joined, nothing failed, replication never started.
		require.NoError(t, awaitStop(ctx, t, res))

		state, perr := pipelineState(p)
		assert.Equal(t, State(StateRunning), state, "Stop settles nothing; the pipeline is discarded")
		require.NoError(t, perr, "a clone ended by the tenure's cancellation is not a failure")
		assert.Nil(t, rpl.lastCtx(), "replication must not start after the tenure ended")

		p.execMu.Lock()
		owned := p.runExec != nil
		p.execMu.Unlock()
		assert.False(t, owned, "a joined execution releases its handle")

		return nil
	}))
}

// TestStop_DuringReplCancelsAndJoins pins the replication phase: Stop cancels
// replication's context and waits for the run to exit.
func TestStop_DuringReplCancelsAndJoins(t *testing.T) {
	t.Parallel()

	require.NoError(t, util.CtxWithTimeout(t.Context(), 5*time.Second, func(ctx context.Context) error {
		cln, rpl := newRunMocks()
		cln.status = finishedCloneStatus()
		p := &PCSM{lifecycleCtx: ctx, source: unreachableSource(t), clone: cln, repl: rpl, onStateChanged: func(State) {}}

		// Given: replication is running.
		done := startRunning(p)
		awaitEvent(ctx, t, rpl.startCalled, "run did not start replication")

		// When: the tenure ends; replication ends on cancellation alone.
		res := stopAsync(ctx, p)
		awaitEvent(ctx, t, rpl.lastCtx().Done(), "Stop did not cancel replication's context")
		close(rpl.doneCh)
		awaitEvent(ctx, t, done, "run did not exit after replication stopped")

		// Then: joined, nothing failed.
		require.NoError(t, awaitStop(ctx, t, res))

		state, perr := pipelineState(p)
		assert.Equal(t, State(StateRunning), state)
		require.NoError(t, perr)

		return nil
	}))
}

// TestRun_GenuineCloneFailureIsRecorded pins that a clone error recorded while
// the run is live fails the pipeline, so only a canceled tenure's clone error
// is dropped.
func TestRun_GenuineCloneFailureIsRecorded(t *testing.T) {
	t.Parallel()

	require.NoError(t, util.CtxWithTimeout(t.Context(), 5*time.Second, func(ctx context.Context) error {
		cln, rpl := newRunMocks()
		p := &PCSM{lifecycleCtx: ctx, source: unreachableSource(t), clone: cln, repl: rpl, onStateChanged: func(State) {}}

		done := startRunning(p)
		awaitEvent(ctx, t, cln.startCalled, "run did not start the clone")

		cloneErr := errors.New("insert failed")
		cln.status.Err = cloneErr
		close(cln.doneCh)
		awaitEvent(ctx, t, done, "run did not exit")

		state, perr := pipelineState(p)
		assert.Equal(t, State(StateFailed), state)
		require.ErrorIs(t, perr, cloneErr)
		assert.Nil(t, rpl.lastCtx())

		return nil
	}))
}

// TestStop_CanceledJoinKeepsOwnership pins that a Stop whose join is cut short
// keeps the execution handle, so a later Stop still joins the run.
func TestStop_CanceledJoinKeepsOwnership(t *testing.T) {
	t.Parallel()

	require.NoError(t, util.CtxWithTimeout(t.Context(), 5*time.Second, func(ctx context.Context) error {
		cln, rpl := newRunMocks()
		p := &PCSM{lifecycleCtx: ctx, source: unreachableSource(t), clone: cln, repl: rpl, onStateChanged: func(State) {}}

		done := startRunning(p)
		awaitEvent(ctx, t, cln.startCalled, "run did not start the clone")

		// When: the join context is already dead and the clone has not stopped.
		joinCtx, cancelJoin := context.WithCancel(ctx)
		cancelJoin()

		err := p.Stop(joinCtx)

		// Then: an error, the execution still owned.
		require.ErrorIs(t, err, context.Canceled)

		p.execMu.Lock()
		owned := p.runExec != nil
		p.execMu.Unlock()
		assert.True(t, owned, "a cut-short Stop must keep the execution handle")

		// And: the cancellation did reach the clone; once the run has exited on
		// its own, a later Stop finds nothing left to join.
		awaitEvent(ctx, t, cln.lastCtx().Done(), "the clone's context was not canceled")
		cln.status.Err = cln.lastCtx().Err()
		close(cln.doneCh)
		awaitEvent(ctx, t, done, "run did not exit")

		require.NoError(t, p.Stop(ctx))

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

// TestStop_InterruptedFinalizeIsNotCompletedAndRunsAgain pins the finalizing
// phase: a finalizer ended by Stop leaves the state finalizing with Completed
// false, that state is what the checkpoint carries, and an explicit /finalize
// on the pipeline the next tenure restores from it runs finalization again.
func TestStop_InterruptedFinalizeIsNotCompletedAndRunsAgain(t *testing.T) {
	t.Parallel()

	require.NoError(t, util.CtxWithTimeout(t.Context(), 5*time.Second, func(ctx context.Context) error {
		fin := &gatedFinalizer{entered: make(chan struct{})}
		p := recoveredFinalizingPipeline(t)
		p.finalizer = fin

		// Given: catalog finalization is live.
		require.NoError(t, p.Finalize(ctx))
		awaitEvent(ctx, t, fin.entered, "finalizer did not start")

		// When: the tenure ends.
		require.NoError(t, p.Stop(ctx))

		// Then: still finalizing, not completed, no live finalizer.
		p.lock.Lock()
		state, active := p.state, p.finalizeActive
		completed := p.finalizeStatus != nil && p.finalizeStatus.Completed
		p.lock.Unlock()

		assert.Equal(t, State(StateFinalizing), state)
		assert.False(t, active)
		assert.False(t, completed, "an interrupted finalization must not report completion")

		// And: the next tenure restores that state and an explicit /finalize
		// runs finalization again to completion.
		data, err := p.Checkpoint(ctx)
		require.NoError(t, err)

		next := recoveredFinalizingPipeline(t)
		require.NoError(t, next.Recover(ctx, data))
		next.finalizer = fin
		finalized := make(chan struct{}, 1)
		next.onStateChanged = func(s State) {
			if s == StateFinalized {
				signalCalled(finalized)
			}
		}

		require.NoError(t, next.Finalize(ctx))
		awaitEvent(ctx, t, finalized, "second finalization did not complete")

		next.lock.Lock()
		state = next.state
		completed = next.finalizeStatus != nil && next.finalizeStatus.Completed
		next.lock.Unlock()

		assert.Equal(t, State(StateFinalized), state)
		assert.True(t, completed)
		assert.Equal(t, int32(2), fin.calls.Load())

		return nil
	}))
}

// TestFinalize_RefusesWhenEpochDiesDuringDrain pins that a /finalize whose
// epoch ends while it waits for replication to drain starts no catalog work.
func TestFinalize_RefusesWhenEpochDiesDuringDrain(t *testing.T) {
	t.Parallel()

	require.NoError(t, util.CtxWithTimeout(t.Context(), 5*time.Second, func(ctx context.Context) error {
		cln, rpl := newRunMocks()
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

		// Then: refused; no finalizer, no finalize status, state untouched.
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

// TestLaunch_RefusesDeadEpochBeforeMutatingState pins that a request admitted
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

		state, _ := pipelineState(p)
		assert.Equal(t, State(StateIdle), state)
		assert.Nil(t, p.clone)
		assert.Nil(t, p.repl)
	})

	t.Run("resume", func(t *testing.T) {
		t.Parallel()

		cln, rpl := newRunMocks()
		cln.status = finishedCloneStatus()
		rpl.startTime = time.Unix(1, 0)
		rpl.pauseTime = time.Unix(2, 0)
		p := &PCSM{
			lifecycleCtx: t.Context(), state: StatePaused,
			clone: cln, repl: rpl, onStateChanged: func(State) {},
		}

		require.ErrorIs(t, p.Resume(ctx, ResumeOptions{}), ErrNotActive)

		state, _ := pipelineState(p)
		assert.Equal(t, State(StatePaused), state)
		assert.Nil(t, rpl.lastCtx())
	})

	t.Run("recover", func(t *testing.T) {
		t.Parallel()

		_, rpl := newRunMocks()
		p := &PCSM{lifecycleCtx: t.Context(), state: StatePaused, repl: rpl, onStateChanged: func(State) {}}
		data, err := bson.Marshal(checkpoint{State: StateRunning})
		require.NoError(t, err)

		require.ErrorIs(t, p.Recover(ctx, data), ErrNotActive)

		state, _ := pipelineState(p)
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
		cln, rpl := newRunMocks()
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
