package main //nolint:testpackage // Exercise the tenure writer and finalization together.

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/xoptions"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/ha"
	"github.com/percona/percona-clustersync-mongodb/pcsm"
)

func TestFinalizationStatusWaitsForCheckpoint(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		ack  bool
	}{
		{name: "acknowledged", ack: true},
		{name: "fenced"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				checkFinalizationCheckpoint(t, tc.ack)
			})
		})
	}
}

func checkFinalizationCheckpoint(t *testing.T, ack bool) {
	t.Helper()

	issued := make(chan struct{}, 1)
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	initial := claimed(bson.D{
		{"_id", recoveryID},
		{"term", int64(1)},
		{"data", bson.D{{"state", pcsm.StateFinalizing}}},
	})
	deployment := drivertest.NewMockDeployment()
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		if e.CommandName != "findAndModify" {
			return
		}
		state, _ := e.Command.Lookup("update", "$set", "data", "state").StringValueOK()
		if state != pcsm.StateFinalized {
			deployment.AddResponses(initial)

			return
		}

		select {
		case issued <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-ctx.Done():
		}
		if ack {
			deployment.AddResponses(initial)
		} else {
			deployment.AddResponses(noDocument())
		}
	}}
	opts := options.Client().SetMonitor(monitor)
	require.NoError(t, xoptions.SetInternalClientOptions(opts, "deployment", deployment))
	target, err := mongo.Connect(opts)
	require.NoError(t, err)
	source := mockedTarget(t, nil, bson.D{{"ok", 1}}, bson.D{{"ok", 1}})
	t.Cleanup(func() {
		require.NoError(t, source.Disconnect(t.Context()))
		require.NoError(t, target.Disconnect(t.Context()))
	})
	s := &server{
		cfg:           &config.Config{RecoveryCheckpointInterval: time.Hour},
		sourceCluster: source,
		targetCluster: target,
		membership:    &ha.Membership{},
	}
	s.observeOwnership(ctx, ha.Ownership{Held: true, Term: 2})
	synctest.Wait()
	tn := currentTenure(s)
	require.NotNil(t, tn)
	require.True(t, tn.isReady())

	require.NoError(t, tn.pipeline.Finalize(pcsm.WithEpoch(ctx, tn.ctx)),
		"finalize should still acknowledge scheduling")
	synctest.Wait()
	require.Len(t, issued, 1, "the finalized snapshot must be pending")

	before := tn.pipeline.Status(ctx)
	require.Equal(t, pcsm.State(pcsm.StateFinalizing), before.State, "finalized leaked before checkpoint acknowledgment")
	require.NotNil(t, before.FinalizeStatus)
	require.False(t, before.FinalizeStatus.Completed)

	close(release)
	synctest.Wait()
	after := tn.pipeline.Status(ctx)
	require.NotNil(t, after.FinalizeStatus)
	if ack {
		require.Same(t, tn, currentTenure(s))
		require.Equal(t, pcsm.State(pcsm.StateFinalized), after.State)
		require.True(t, after.FinalizeStatus.Completed)
	} else {
		require.Nil(t, currentTenure(s), "checkpoint loss did not finish tearing down the tenure")
		require.Equal(t, pcsm.State(pcsm.StateFinalizing), after.State)
		require.False(t, after.FinalizeStatus.Completed)
	}
}
