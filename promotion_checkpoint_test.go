package main //nolint:testpackage // Exercise server promotion and its checkpoint loop together.

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
	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm"
)

func TestPromotionCheckpointUsesNewTerm(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		// Given: a coalesced demotion leaves the prior checkpoint loop alive.
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		data, err := bson.Marshal(bson.D{{"state", pcsm.StateFailed}})
		require.NoError(t, err)
		record := bson.D{{"_id", recoveryID}, {"term", int64(2)}, {"data", bson.Raw(data)}}
		deployment := drivertest.NewMockDeployment(
			bson.D{{"ok", 1}, {"cursor", bson.D{
				{"id", int64(0)},
				{"ns", config.PCSMDatabase + "." + config.RecoveryCollection},
				{"firstBatch", bson.A{record}},
			}}},
			bson.D{{"ok", 1}, {"value", record}},
			bson.D{{"ok", 1}, {"value", record}},
		)
		checkpoints := make(chan bson.Raw, 1)
		opts := options.Client().SetMonitor(&event.CommandMonitor{
			Started: func(_ context.Context, e *event.CommandStartedEvent) {
				if e.CommandName == "findAndModify" {
					checkpoints <- append(bson.Raw(nil), e.Command...)
				}
			},
		})
		require.NoError(t, xoptions.SetInternalClientOptions(opts, "deployment", deployment))
		target, err := mongo.Connect(opts)
		require.NoError(t, err)

		pipeline := pcsm.New(ctx, nil, target, mdb.ServerVersion{}, false, false)
		require.NoError(t, pipeline.Recover(ctx, data))
		membership := &ha.Membership{}
		membership.SetRole(ha.RoleActive, 3)
		oldCtx, oldCancel := context.WithCancel(ctx)
		s := &server{
			cfg:              &config.Config{RecoveryCheckpointInterval: time.Second},
			targetCluster:    target,
			pcsm:             pipeline,
			membership:       membership,
			activeTerm:       1,
			checkpointCancel: oldCancel,
		}
		oldDone := make(chan struct{})
		go func() {
			defer close(oldDone)
			RunCheckpointing(oldCtx, target, pipeline, 1, "old-active",
				time.Second, func() { s.onDemote(ctx, 1) })
		}()
		t.Cleanup(func() {
			cancel()
			synctest.Wait()
			<-oldDone
			require.NoError(t, target.Disconnect(t.Context()))
		})
		synctest.Wait()

		// When: the next observed role change is ACTIVE in a newer term.
		s.onPromote(ctx, 3)
		require.Equal(t, ha.Term(3), s.activeTerm, "restore must have succeeded")

		// Then: periodic checkpoints continue with the new ownership term.
		for range 2 {
			select {
			case command := <-checkpoints:
				terms, decodeErr := command.Lookup("query", "$expr", "$lte").Array().Values()
				require.NoError(t, decodeErr)
				require.Equal(t, int64(3), terms[1].Int64())
			case <-time.After(2 * time.Second):
				t.Fatal("no periodic checkpoint after promotion")
			}
		}
	})
}
