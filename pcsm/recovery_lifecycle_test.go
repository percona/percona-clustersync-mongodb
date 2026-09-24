package pcsm //nolint:testpackage // Drives the unexported run/monitor lifecycle directly.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/pcsm/clone"
	"github.com/percona/percona-clustersync-mongodb/pcsm/repl"
)

type drainingReplicator struct {
	mockReplicator

	pausing      atomic.Bool
	settled      atomic.Bool
	tailCaptured atomic.Bool
	doneObserved chan struct{}
	tailObserved chan struct{}
	releaseTail  chan struct{}
	drainError   error
}

func (r *drainingReplicator) Pause(context.Context) error {
	r.pausing.Store(true)

	return nil
}

func (r *drainingReplicator) Done() <-chan struct{} {
	close(r.doneObserved)

	return r.doneCh
}

func (r *drainingReplicator) Status() repl.Status {
	status := repl.Status{
		StartTime:            time.Unix(1, 0),
		LastReplicatedOpTime: bson.Timestamp{T: 1},
		Pausing:              r.pausing.Load(),
	}
	if r.settled.Load() {
		status.PauseTime = time.Unix(2, 0)
		status.Err = r.drainError
		if r.tailCaptured.CompareAndSwap(false, true) {
			close(r.tailObserved)
			<-r.releaseTail
		}
	}

	return status
}

func TestRecoverIsNotOverwrittenByPreviousRun(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		// Given: demotion drained the replicator, but PCSM.run is still exiting.
		p, _, release := pausedPipelineWithExitingRun(t, finishedLifecycleCloner())
		defer release()

		// When: recovery is requested before the old run returns.
		data, err := bson.Marshal(checkpoint{State: StatePaused})
		require.NoError(t, err)
		recovered := make(chan error, 1)
		go func() { recovered <- p.Recover(t.Context(), data) }()
		synctest.Wait()
		select {
		case early := <-recovered:
			t.Fatalf("recovery returned before the previous run exited: %v", early)
		default:
		}

		release()
		require.NoError(t, <-recovered)
		synctest.Wait()

		// Then: the old run's error cannot overwrite the recovered state.
		p.lock.Lock()
		defer p.lock.Unlock()
		require.Equal(t, State(StatePaused), p.state)
		require.NoError(t, p.err)
	})
}

func TestRecoverCancellationPreservesPreviousRun(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		// Given: the old run still owns the paused pipeline.
		p, old, release := pausedPipelineWithExitingRun(t, finishedLifecycleCloner())
		defer release()
		data, err := bson.Marshal(checkpoint{State: StatePaused})
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		recovered := make(chan error, 1)
		go func() { recovered <- p.Recover(ctx, data) }()
		synctest.Wait()

		// When: recovery is canceled while joining the old run.
		cancel()
		require.ErrorIs(t, <-recovered, context.Canceled)

		// Then: cancellation neither replaces components nor abandons ownership.
		p.lock.Lock()
		require.Same(t, old, p.repl)
		require.Equal(t, State(StatePaused), p.state)
		p.lock.Unlock()
		release()
		synctest.Wait()
		p.lock.Lock()
		require.Equal(t, State(StateFailed), p.state)
		require.ErrorIs(t, p.err, old.drainError)
		p.lock.Unlock()
		require.NoError(t, p.Recover(t.Context(), data))
	})
}

func TestResumeRechecksFailureAfterPreviousRun(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		// Given: the paused pipeline has an unreported drain error.
		p, old, release := pausedPipelineWithExitingRun(t, finishedLifecycleCloner())
		defer release()

		// When: an ordinary resume overlaps the old run's failure reporting.
		resumed := make(chan error, 1)
		go func() { resumed <- p.Resume(t.Context(), ResumeOptions{}) }()
		synctest.Wait()
		select {
		case early := <-resumed:
			t.Fatalf("resume returned before the previous run exited: %v", early)
		default:
		}
		release()

		// Then: resume must validate the settled state, not clear the old error.
		require.ErrorContains(t, <-resumed, "cannot resume")
		p.lock.Lock()
		defer p.lock.Unlock()
		require.Equal(t, State(StateFailed), p.state)
		require.ErrorIs(t, p.err, old.drainError)
	})
}

type exitingMonitorCloner struct {
	mockCloner

	statusCalls atomic.Int64
	observed    chan struct{}
	release     chan struct{}
}

func (c *exitingMonitorCloner) Status() clone.Status {
	// run reads the status first; monitorInitialSync reads it second.
	if c.statusCalls.Add(1) == 2 {
		close(c.observed)
		<-c.release
	}

	return c.status
}

func TestRecoverJoinsPreviousRunMonitors(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		// Given: the initial-sync monitor still holds the old component.
		cln := &exitingMonitorCloner{
			mockCloner: *finishedLifecycleCloner(),
			observed:   make(chan struct{}),
			release:    make(chan struct{}),
		}
		cln.status.FinishTS = bson.Timestamp{T: 2}
		releaseMonitor := sync.OnceFunc(func() { close(cln.release) })
		defer releaseMonitor()
		p, _, releaseRun := pausedPipelineWithExitingRun(t, cln)
		defer releaseRun()
		<-cln.observed
		releaseRun()
		synctest.Wait()

		// When: run has reported its error, but its monitor has not exited.
		data, err := bson.Marshal(checkpoint{State: StatePaused})
		require.NoError(t, err)
		recovered := make(chan error, 1)
		go func() { recovered <- p.Recover(t.Context(), data) }()
		synctest.Wait()
		select {
		case early := <-recovered:
			t.Fatalf("recovery returned before the previous monitor exited: %v", early)
		default:
		}

		// Then: replacement waits for the monitor as well as run.
		releaseMonitor()
		require.NoError(t, <-recovered)
		p.lock.Lock()
		defer p.lock.Unlock()
		require.Equal(t, State(StatePaused), p.state)
		require.NoError(t, p.err)
		require.NotSame(t, cln, p.clone)
	})
}

func finishedLifecycleCloner() *mockCloner {
	return &mockCloner{status: clone.Status{
		FinishTime: time.Unix(1, 0), FinishTS: bson.Timestamp{T: 1},
	}}
}

func pausedPipelineWithExitingRun(t *testing.T, cln Cloner) (*PCSM, *drainingReplicator, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	runCtx, stopRun := context.WithCancel(ctx)
	t.Cleanup(stopRun)
	old := &drainingReplicator{
		doneObserved: make(chan struct{}),
		tailObserved: make(chan struct{}),
		releaseTail:  make(chan struct{}),
		drainError:   errors.New("previous run failed during drain"),
	}
	old.doneCh = make(chan struct{})
	release := sync.OnceFunc(func() { close(old.releaseTail) })
	t.Cleanup(release)
	p := &PCSM{
		lifecycleCtx:   runCtx,
		state:          StatePaused,
		clone:          cln,
		repl:           old,
		onStateChanged: func(State) {},
	}
	require.NoError(t, p.Resume(ctx, ResumeOptions{}))
	await := func(signal <-chan struct{}) {
		t.Helper()
		select {
		case <-signal:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	await(old.doneObserved)
	require.NoError(t, p.Pause(ctx))
	stopRun()
	old.pausing.Store(false)
	old.settled.Store(true)
	close(old.doneCh)
	await(old.tailObserved)

	return p, old, release
}
