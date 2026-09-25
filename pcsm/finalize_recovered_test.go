package pcsm //nolint:testpackage

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
// Recover assigns p.state from the checkpoint, so the promoted instance
// restores StateFinalizing, and it rebuilds the replicator through
// repl.NewRepl, which always allocates an open doneCh. Nothing restarts the
// finalize goroutine and nothing ever runs that replicator, so its Done
// channel stays open for the lifetime of the process.
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

// Control for the two tests below: the same pipeline finalizes normally once
// its replicator has actually run and closed Done. This isolates the failures
// to the never-closed channel rather than to the fixture.
func TestFinalizeCompletesWhenReplicatorHasRun(t *testing.T) {
	t.Parallel()

	p := recoveredFinalizingPipeline(t)
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

// Finalization is not resumed on promotion, so re-issuing finalize is
// only recovery action available from a restored "finalizing" state
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

// Finalize holds the lifecycle lock across that receive and Status needs the
// same lock, so the promoted instance also stops answering /status. /metrics
// is served off a different path and keeps returning 200, so a liveness probe
// still sees a healthy instance.
func TestStatusStaysResponsiveDuringFinalizeAfterPromotion(t *testing.T) {
	t.Parallel()

	p := recoveredFinalizingPipeline(t)

	blocked := make(chan struct{})
	go func() {
		close(blocked)
		_ = p.Finalize(t.Context())
	}()
	<-blocked
	time.Sleep(500 * time.Millisecond)

	reported := make(chan *Status, 1)
	go func() { reported <- p.Status(t.Context()) }()

	select {
	case status := <-reported:
		require.Equal(t, State(StateFinalizing), status.State)
	case <-time.After(finalizeProbeTimeout):
		t.Fatal("Status never returned: it waits on the lifecycle lock that " +
			"Finalize holds while blocked, so the instance stops reporting its " +
			"state even though /metrics still answers")
	}
}
