package pcsm //nolint:testpackage // Exercises the checkpoint/lifecycle lock boundary.

import (
	"context"
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

func checkpointDrainPipeline(ctx context.Context) (*PCSM, *mockReplicator) {
	cln, rpl := newRunMocks()
	cln.status = finishedCloneStatus()
	rpl.startTime = time.Unix(1, 0)
	rpl.lastOpTime = bson.Timestamp{T: 2}
	rpl.doneCalled = make(chan struct{}, 1)

	return &PCSM{
		lifecycleCtx: ctx, state: StateRunning,
		clone: cln, repl: rpl,
		catalog:        catalog.NewCatalog(nil, nil, mdb.ServerVersion{}),
		onStateChanged: func(State) {},
	}, rpl
}

func TestFinalizeDrainsPendingCheckpointWithExclusiveReservation(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		p, rpl := checkpointDrainPipeline(ctx)
		catalogEntered := make(chan struct{}, 2)
		releaseCatalog := make(chan struct{})
		p.finalizer = &blockingCatalogFinalizer{entered: catalogEntered, release: releaseCatalog}
		finalized := make(chan struct{}, 1)
		p.onStateChanged = func(s State) {
			if s == StateFinalized {
				finalized <- struct{}{}
			}
		}
		first := make(chan error, 1)
		go func() { first <- p.Finalize(ctx) }()
		<-rpl.doneCalled
		synctest.Wait()

		require.EqualError(t, p.Finalize(ctx), "finalization is already in progress")
		require.NoError(t, p.Pause(ctx))
		require.ErrorContains(t, p.Resume(ctx, ResumeOptions{}), "finalization is already in progress")

		// The dispatcher cannot finish its drain until the writer acquires
		// the pipeline lock and captures the protected boundary.
		checkpointed := make(chan error, 1)
		go func() {
			_, err := p.Checkpoint(ctx)
			checkpointed <- err
			close(rpl.doneCh)
		}()
		require.NoError(t, <-checkpointed)
		require.NoError(t, <-first)
		<-catalogEntered
		require.EqualError(t, p.Finalize(ctx), "finalization is already in progress")
		synctest.Wait()
		require.Empty(t, catalogEntered, "only one catalog finalizer may start")
		close(releaseCatalog)
		<-finalized
		require.NoError(t, p.Stop(ctx))
	})
}

func TestFinalizeRequestDeadlineDoesNotAbandonAcceptedDrain(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		p, rpl := checkpointDrainPipeline(ctx)
		result := make(chan error, 1)
		finalized := make(chan struct{})
		p.onStateChanged = func(state State) {
			if state == StateFinalized {
				close(finalized)
			}
		}
		require.NoError(t, util.CtxWithTimeout(ctx, 5*time.Second, func(caller context.Context) error {
			go func() { result <- p.Finalize(caller) }()
			<-rpl.doneCalled
			<-caller.Done()
			require.ErrorIs(t, caller.Err(), context.DeadlineExceeded)

			return nil
		}))
		synctest.Wait()
		require.Empty(t, result, "accepted finalization must outlive the request deadline")
		p.lock.Lock()
		require.True(t, p.finalizeActive)
		p.lock.Unlock()
		close(rpl.doneCh)
		require.NoError(t, <-result)
		<-finalized
		require.NoError(t, p.Stop(ctx))
	})
}

func TestFinalizeEpochCancellationReleasesReservation(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		epoch, endEpoch := context.WithCancel(ctx)
		defer endEpoch()
		p, rpl := checkpointDrainPipeline(epoch)
		result := make(chan error, 1)
		go func() { result <- p.Finalize(WithEpoch(ctx, epoch)) }()
		<-rpl.doneCalled
		endEpoch()
		require.ErrorIs(t, <-result, ErrNotActive)
		p.lock.Lock()
		require.False(t, p.finalizeActive)
		require.Nil(t, p.finalizeStatus)
		p.lock.Unlock()
		_, err := p.Checkpoint(ctx)
		require.NoError(t, err)
		close(rpl.doneCh)
		require.NoError(t, p.Stop(ctx))
	})
}

func TestFinalizeRevalidatesFailureAfterDrain(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		p, rpl := checkpointDrainPipeline(ctx)
		result := make(chan error, 1)
		go func() { result <- p.Finalize(ctx) }()
		<-rpl.doneCalled
		synctest.Wait()

		failure := errors.New("replication failed during drain")
		p.setFailed(failure)
		close(rpl.doneCh)
		require.ErrorIs(t, <-result, failure)
		p.lock.Lock()
		require.False(t, p.finalizeActive)
		require.Nil(t, p.finalizeStatus)
		p.lock.Unlock()
	})
}

func TestFinalizePauseErrorReleasesReservation(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		p, rpl := checkpointDrainPipeline(t.Context())
		rpl.pauseErr = errors.New("pause failed")
		require.ErrorIs(t, p.Finalize(t.Context()), rpl.pauseErr)
		p.lock.Lock()
		require.False(t, p.finalizeActive)
		p.lock.Unlock()
	})
}
