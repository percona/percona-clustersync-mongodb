//go:build integration

package main //nolint:testpackage // Exercise the lease loop, tenure and real replication together.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/ha"
	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm"
)

func TestHALeaseTTLTargetWriteOutageKeepsTenure(t *testing.T) {
	t.Parallel()

	_, source := checkpointFaultMongo(t)
	targetURI, control := checkpointFaultMongo(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	renewalFailed := make(chan struct{})
	notifyFailure := sync.OnceFunc(func() { close(renewalFailed) })
	target, err := mongo.Connect(options.Client().ApplyURI(targetURI).
		SetTimeout(time.Minute).SetMonitor(&event.CommandMonitor{
		Failed: func(_ context.Context, e *event.CommandFailedEvent) {
			if e.CommandName == "findAndModify" {
				notifyFailure()
			}
		},
	}))
	require.NoError(t, err)
	defer func() { require.NoError(t, target.Disconnect(t.Context())) }()
	membership, err := ha.JoinMembership(ctx, target, ha.MembershipOptions{
		InstanceID: "outage-owner", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, membership.Stop(t.Context())) }()
	version, err := mdb.Version(ctx, source)
	require.NoError(t, err)
	s := &server{
		cfg:           &config.Config{RecoveryCheckpointInterval: time.Second},
		sourceCluster: source,
		targetCluster: target,
		sourceVersion: version,
		membership:    membership,
	}
	defer closeTenure(ctx, s)
	var transitions atomic.Int64
	observe := s.ownershipObserver(ctx)
	tracked := func(o ha.Ownership) {
		before, _ := membership.CurrentRole()
		observe(o)
		after, _ := membership.CurrentRole()
		if before != after {
			transitions.Add(1)
		}
	}
	membership.FirstLeaseTick(ctx, tracked)
	tn := awaitTenure(t, ctx, s)
	initialTransitions := transitions.Load()
	require.Equal(t, int64(1), initialTransitions)
	leaseCtx, cancelLease := context.WithCancel(ctx)
	leaseDone := make(chan struct{})
	go func() {
		defer close(leaseDone)
		membership.RunLease(leaseCtx, tracked)
	}()
	defer func() {
		cancelLease()
		awaitCheckpointSignal(t, ctx, leaseDone)
	}()

	sourceColl := source.Database("ttl_outage").Collection("docs")
	targetColl := control.Database("ttl_outage").Collection("docs")
	_, err = sourceColl.InsertOne(ctx, bson.D{{"_id", "cloned"}})
	require.NoError(t, err)
	require.NoError(t, tn.pipeline.Start(pcsm.WithEpoch(ctx, tn.ctx), &pcsm.StartOptions{}))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		require.True(c, tn.pipeline.Status(ctx).InitialSyncCompleted)
	}, 30*time.Second, 100*time.Millisecond)

	// Reuse the root checkpoint fault harness: target writes cannot get a
	// response for 15s, while the control connection can disable the fault.
	// This models a stalled target, not a topology/server-selection failure.
	require.NoError(t, control.Database("admin").RunCommand(ctx, bson.D{
		{"configureFailPoint", "failCommand"},
		{"mode", "alwaysOn"},
		{"data", bson.D{
			{"failCommands", bson.A{"findAndModify", "update", "insert", "bulkWrite"}},
			{"blockConnection", true},
			{"blockTimeMS", 15000},
		}},
	}).Err())
	heal := func() {
		require.NoError(t, control.Database("admin").RunCommand(t.Context(), bson.D{
			{"configureFailPoint", "failCommand"}, {"mode", "off"},
		}).Err())
	}
	defer heal()
	outage := time.NewTimer(15 * time.Second)
	defer outage.Stop()
	_, err = sourceColl.InsertOne(ctx, bson.D{{"_id", "during-outage"}})
	require.NoError(t, err)

	select {
	case <-outage.C:
	case <-tn.ctx.Done():
		t.Fatal("the 60s lease tenure ended during a 15s outage")
	case <-ctx.Done():
		t.Fatal("outage test exceeded its deadline")
	}
	awaitCheckpointSignal(t, ctx, renewalFailed)
	require.Same(t, tn, currentTenure(s))
	require.NoError(t, tn.ctx.Err())
	role, term := membership.CurrentRole()
	require.Equal(t, ha.RoleActive, role)
	require.Equal(t, tn.term, term)
	require.Equal(t, initialTransitions, transitions.Load(), "no role transitions during outage")
	heal()

	_, err = sourceColl.InsertOne(ctx, bson.D{{"_id", "after-heal"}})
	require.NoError(t, err)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		n, err := targetColl.CountDocuments(ctx, bson.D{{"_id", bson.D{
			{"$in", bson.A{"during-outage", "after-heal"}},
		}}})
		require.NoError(c, err)
		require.Equal(c, int64(2), n)
	}, 45*time.Second, 100*time.Millisecond)
	require.Same(t, tn, currentTenure(s))
	require.NoError(t, tn.ctx.Err())
	require.Equal(t, initialTransitions, transitions.Load())
}
