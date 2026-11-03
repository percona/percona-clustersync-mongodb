//go:build integration

package main //nolint:testpackage // Exercise the checkpoint claim against the real document.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/ha"
	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm"
)

// A tenure whose claim finds the checkpoint owned by a newer term ends
// without restoring anything, and the lease the instance still holds is
// advanced above the tenure's term for the tenure that follows.
//
//nolint:paralleltest // The recovery integration suite shares one checkpoint document.
func TestTenureFencedByNewerCheckpointEnds(t *testing.T) {
	target := recoveryTestClient(t)
	defer func() { require.NoError(t, target.Disconnect(t.Context())) }()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	leaseColl := target.Database(config.PCSMDatabase).Collection(config.LeaseCollection)
	require.NoError(t, leaseColl.Drop(ctx))
	require.NoError(t, recoveryColl(target).Drop(ctx))

	// Given: a checkpoint owned by term 5.
	other := pcsm.New(ctx, target, target, mdb.ServerVersion{}, false, false)
	failed, err := bson.Marshal(bson.D{{"state", pcsm.StateFailed}})
	require.NoError(t, err)
	require.NoError(t, other.Recover(ctx, failed))
	require.ErrorIs(t, Restore(ctx, target, other, 5, "pcsm-other"), errNoCheckpoint)
	require.NoError(t, DoCheckpoint(ctx, target, other, 5, "pcsm-other"))

	// And: this instance takes the lease, at term 1. Its checkpoint claims go
	// through a client that holds the first one until the tenure is captured.
	monitor, release := holdFirstClaim(t, nil)
	claiming, err := mongo.Connect(options.Client().ApplyURI(recoveryMongo(t)).SetMonitor(monitor))
	require.NoError(t, err)
	defer func() { require.NoError(t, claiming.Disconnect(t.Context())) }()

	membership, err := ha.JoinMembership(ctx, target, ha.MembershipOptions{InstanceID: "pcsm-fenced", Group: "g"})
	require.NoError(t, err)
	defer func() { require.NoError(t, membership.Stop(t.Context())) }()
	s := &server{
		cfg:           &config.Config{RecoveryCheckpointInterval: time.Hour},
		sourceCluster: claiming,
		targetCluster: claiming,
		membership:    membership,
	}
	observe := s.ownershipObserver(ctx)
	defer closeTenure(ctx, s)
	defer release()

	// When: the first lease tick opens a tenure at term 1, whose claim is
	// fenced by the stored term.
	membership.FirstLeaseTick(ctx, observe)
	fenced := currentTenure(s)
	require.NotNil(t, fenced, "ownership at term 1 must open a tenure")
	require.Equal(t, ha.Term(1), fenced.term)
	release()

	select {
	case <-fenced.ctx.Done():
	case <-ctx.Done():
		require.FailNow(t, "the fenced tenure did not end", "%v", ctx.Err())
	}

	// Then: the tenure is over without a restore, and the instance is STANDBY.
	require.Nil(t, currentTenure(s), "a fenced tenure must end")
	require.Equal(t, pcsm.State(pcsm.StateIdle), fenced.pipeline.Status(ctx).State)
	role, _ := membership.CurrentRole()
	require.Equal(t, ha.RoleStandby, role)
	require.Equal(t, ha.Term(2), s.minTerm)
	require.Equal(t, int64(5), readCheckpoint(t, ctx, target).Term, "a fenced claim must not touch the document")

	// And: the next lease attempt, which still owns the lease at term 1,
	// advances it for the next tenure and opens nothing.
	membership.FirstLeaseTick(ctx, observe)
	require.Nil(t, currentTenure(s), "ownership below minTerm must not open a tenure")

	var lease ha.Lease
	require.NoError(t, leaseColl.FindOne(ctx, bson.D{{"_id", ha.LeaseID}}).Decode(&lease))
	require.Equal(t, "pcsm-fenced", lease.InstanceID)
	require.Equal(t, ha.Term(2), lease.Term, "the lease term must move above the fenced tenure's")
}

// The claim orders a write of the ended tenure's term that is still on the
// wire. The write is the newest snapshot of its term, since the writer that
// issued it never issued another: landing before the claim, it is the
// snapshot the next tenure restores; landing after it, it is rejected. Either
// way the next tenure's restore and the stored checkpoint agree.
//
//nolint:paralleltest // The recovery integration suite shares one checkpoint document.
func TestClaimOrdersLateWriteOfEndedTerm(t *testing.T) {
	target := recoveryTestClient(t)
	defer func() { require.NoError(t, target.Disconnect(t.Context())) }()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	snapshotOf := func(state pcsm.State) bson.Raw {
		t.Helper()

		pipeline := pcsm.New(ctx, target, target, mdb.ServerVersion{}, false, false)
		data, err := bson.Marshal(bson.D{{"state", state}})
		require.NoError(t, err)
		require.NoError(t, pipeline.Recover(ctx, data))

		snapshot, err := pipeline.Checkpoint(ctx)
		require.NoError(t, err)

		return snapshot
	}
	storedState := func() string {
		return readCheckpoint(t, ctx, target).Data.Lookup("state").StringValue()
	}

	tests := []struct {
		name        string
		landsBefore bool
		wantState   pcsm.State
	}{
		{name: "lands before the claim and is restored", landsBefore: true, wantState: pcsm.StateFinalized},
		{name: "lands after the claim and is rejected", landsBefore: false, wantState: pcsm.StateFinalizing},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, recoveryColl(target).Drop(ctx))

			// Given: the tenure at term 1 claimed the checkpoint and saved a
			// finalizing snapshot; its next write, the finalized snapshot, is
			// on the wire when the tenure ends.
			saved := staticRecoverable{data: snapshotOf(pcsm.StateFinalizing)}
			require.ErrorIs(t, Restore(ctx, target, saved, 1, "pcsm-old"), errNoCheckpoint)
			require.NoError(t, DoCheckpoint(ctx, target, saved, 1, "pcsm-old"))
			late := staticRecoverable{data: snapshotOf(pcsm.StateFinalized)}

			if tt.landsBefore {
				require.NoError(t, DoCheckpoint(ctx, target, late, 1, "pcsm-old"))
			}

			// When: the tenure at term 2 claims the checkpoint and restores it.
			next := pcsm.New(ctx, target, target, mdb.ServerVersion{}, false, false)
			require.NoError(t, Restore(ctx, target, next, 2, "pcsm-new"))

			if !tt.landsBefore {
				require.ErrorIs(t, DoCheckpoint(ctx, target, late, 1, "pcsm-old"), errCheckpointFenced,
					"a write of the ended term landing after the claim must be rejected")
			}

			// Then: the stored checkpoint is what the tenure at term 2 restored.
			require.Equal(t, tt.wantState, next.Status(ctx).State)
			require.Equal(t, string(tt.wantState), storedState())
			cp := readCheckpoint(t, ctx, target)
			require.Equal(t, int64(2), cp.Term)
			require.Equal(t, "pcsm-new", cp.InstanceID)
		})
	}
}
