package pcsm //nolint:testpackage // Exercises unexported recovered lifecycle state directly.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/pcsm/clone"
)

const finalizeProbeTimeout = 5 * time.Second

// unreachableSource returns a client that fails server selection immediately.
// Tests need StateFinalizing, so client is supplied and status treats the lookup
// failure as "source cluster is lost" and carries on.
func unreachableSource(t *testing.T) *mongo.Client {
	t.Helper()

	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://127.0.0.1:1").
		SetServerSelectionTimeout(10 * time.Millisecond).
		SetConnectTimeout(10 * time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })

	return client
}

// recoveredFinalizingPipeline reproduces the state HA standby is left in
// after being promoted while the previous ACTIVE was finalizing.
//
// Recover restores StateFinalizing and rebuilds a replicator with an open
// Done channel, but it does not restore an in-process finalizer goroutine.
// The empty catalog lets resumed finalization complete without MongoDB clients.
func recoveredFinalizingPipeline(t *testing.T) *PCSM {
	t.Helper()

	return &PCSM{
		lifecycleCtx: t.Context(),
		source:       unreachableSource(t),
		state:        StateFinalizing,
		catalog:      catalog.NewCatalog(nil, nil, mdb.ServerVersion{}),
		clone: &mockCloner{status: clone.Status{
			StartTime:  time.Unix(1, 0),
			FinishTime: time.Unix(2, 0),
			FinishTS:   bson.Timestamp{T: 1},
		}},
		repl: &mockReplicator{
			doneCh:     make(chan struct{}),
			startTime:  time.Unix(1, 0),
			pauseTime:  time.Unix(2, 0),
			lastOpTime: bson.Timestamp{T: 100},
		},
		onStateChanged: func(State) {},
	}
}

// The normal StateRunning path still waits for a replicator that has run and
// closed Done before starting catalog finalization.
func TestFinalizeCompletesWhenReplicatorHasRun(t *testing.T) {
	t.Parallel()

	p := recoveredFinalizingPipeline(t)
	p.state = StateRunning
	done := make(chan struct{})
	close(done)
	p.repl.(*mockReplicator).doneCh = done //nolint:forcetypeassert // fixture-owned type

	finalized := make(chan error, 1)
	go func() { finalized <- p.Finalize(t.Context()) }()

	select {
	case err := <-finalized:
		require.NoError(t, err)
	case <-time.After(finalizeProbeTimeout):
		t.Fatal("Finalize did not return even though the replicator had finished")
	}
}

// Finalization is not resumed automatically on promotion, so re-issuing
// finalize is the recovery action from a restored "finalizing" state.
func TestFinalizeAfterPromotionTerminates(t *testing.T) {
	t.Parallel()

	p := recoveredFinalizingPipeline(t)

	finalized := make(chan error, 1)
	go func() { finalized <- p.Finalize(t.Context()) }()

	select {
	case err := <-finalized:
		require.NoError(t, err)
	case <-time.After(finalizeProbeTimeout):
		t.Fatal("Finalize never returned: it blocks on <-repl.Done() for a " +
			"replicator that was recovered but never run, so that channel can " +
			"never close and a promoted instance cannot leave 'finalizing'")
	}
}

func TestFinalizeAfterPromotionFromPausedTerminates(t *testing.T) {
	t.Parallel()

	p := recoveredFinalizingPipeline(t)
	p.state = StatePaused

	finalized := make(chan error, 1)
	go func() { finalized <- p.Finalize(t.Context()) }()

	select {
	case err := <-finalized:
		require.NoError(t, err)
	case <-time.After(finalizeProbeTimeout):
		t.Fatal("Finalize did not return for a recovered paused replicator")
	}

	reported := make(chan *Status, 1)
	go func() { reported <- p.Status(t.Context()) }()

	select {
	case status := <-reported:
		require.Contains(t, []State{StateFinalizing, StateFinalized}, status.State)
	case <-time.After(finalizeProbeTimeout):
		t.Fatal("Status did not return after finalizing a recovered paused pipeline")
	}
}

func TestFinalizeAgainAfterRecoveredFinalizationTerminates(t *testing.T) {
	t.Parallel()

	p := recoveredFinalizingPipeline(t)
	finalizedState := make(chan struct{}, 2)
	p.SetOnStateChanged(func(state State) {
		if state == StateFinalized {
			finalizedState <- struct{}{}
		}
	})

	firstFinalize := make(chan error, 1)
	go func() { firstFinalize <- p.Finalize(t.Context()) }()

	select {
	case err := <-firstFinalize:
		require.NoError(t, err)
	case <-time.After(finalizeProbeTimeout):
		t.Fatal("first Finalize did not return after promotion")
	}

	select {
	case <-finalizedState:
	case <-time.After(finalizeProbeTimeout):
		t.Fatal("recovered finalization did not report StateFinalized")
	}

	secondFinalize := make(chan error, 1)
	go func() { secondFinalize <- p.Finalize(t.Context()) }()

	select {
	case err := <-secondFinalize:
		require.NoError(t, err)
	case <-time.After(finalizeProbeTimeout):
		t.Fatal("second Finalize did not return after recovered finalization completed")
	}

	reported := make(chan *Status, 1)
	go func() { reported <- p.Status(t.Context()) }()

	select {
	case status := <-reported:
		require.Contains(t, []State{StateFinalizing, StateFinalized}, status.State)
	case <-time.After(finalizeProbeTimeout):
		t.Fatal("Status did not return after second Finalize")
	}
}

// Re-issued finalize must return before status is queried. The empty catalog
// can finish immediately, so either finalizing or finalized is valid here.
func TestStatusStaysResponsiveDuringFinalizeAfterPromotion(t *testing.T) {
	t.Parallel()

	p := recoveredFinalizingPipeline(t)

	finalized := make(chan error, 1)
	go func() {
		finalized <- p.Finalize(t.Context())
	}()

	select {
	case err := <-finalized:
		require.NoError(t, err)
	case <-time.After(finalizeProbeTimeout):
		t.Fatal("Finalize did not return after promotion")
	}

	reported := make(chan *Status, 1)
	go func() { reported <- p.Status(t.Context()) }()

	select {
	case status := <-reported:
		require.Contains(t, []State{StateFinalizing, StateFinalized}, status.State)
		require.NotNil(t, status.FinalizeStatus)
	case <-time.After(finalizeProbeTimeout):
		t.Fatal("Status did not return after resumed finalization")
	}
}

func TestFinalizeRejectsSecondCallWhileFinalizerActive(t *testing.T) {
	t.Parallel()

	p := recoveredFinalizingPipeline(t)
	p.finalizeStatus = &FinalizeStatus{StartedAt: time.Now()}
	// startFinalize is the only production writer of this process-local flag.
	// Set it directly to hold the active window without racing the empty catalog.
	p.finalizeActive = true

	finalized := make(chan error, 1)
	go func() { finalized <- p.Finalize(t.Context()) }()

	select {
	case err := <-finalized:
		require.EqualError(t, err, "finalization is already in progress")
	case <-time.After(finalizeProbeTimeout):
		t.Fatal("second Finalize did not return while finalizer was active")
	}

	reported := make(chan *Status, 1)
	go func() { reported <- p.Status(t.Context()) }()

	select {
	case status := <-reported:
		require.Equal(t, State(StateFinalizing), status.State)
		require.NotNil(t, status.FinalizeStatus)
	case <-time.After(finalizeProbeTimeout):
		t.Fatal("Status did not return while finalizer was active")
	}
}
