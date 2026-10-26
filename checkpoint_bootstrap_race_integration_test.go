//go:build integration

package main //nolint:testpackage // Exercise the checkpoint bootstrap and demotion together.

import (
	"context"
	"sync"
	"testing"
	"time"

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

// Two writers of one term bootstrap the first checkpoint at once (once the
// periodic loop and the state-change callback; today any two processes that
// share a term after a lease reset). Neither finds a document, so both
// bootstrap it, and the loser reads its duplicate key as a takeover by a newer
// instance. Both writers hold the same term, so neither has been deposed.
//
//nolint:paralleltest // The recovery integration suite shares one checkpoint document.
func TestSameTermBootstrapCollisionIsNotATakeover(t *testing.T) {
	target := recoveryTestClient(t)
	defer func() { require.NoError(t, target.Disconnect(t.Context())) }()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	require.NoError(t, recoveryColl(target).Drop(ctx))

	pipeline := pcsm.New(ctx, target, target, mdb.ServerVersion{}, false, false)
	running, err := bson.Marshal(bson.D{{"state", pcsm.StateRunning}})
	require.NoError(t, err)
	require.NoError(t, pipeline.Recover(ctx, running))

	// Given: the periodic loop has found no document to update and is about to
	// insert one. Holding its insert here reproduces the race deterministically
	// instead of waiting for two round trips to overlap.
	inserting := make(chan struct{})
	released := make(chan struct{})
	announce := sync.OnceFunc(func() { close(inserting) })
	periodic, err := mongo.Connect(options.Client().
		ApplyURI(recoveryMongo(t)).
		SetRetryWrites(false).
		SetMonitor(&event.CommandMonitor{
			Started: func(_ context.Context, command *event.CommandStartedEvent) {
				if command.CommandName != "insert" ||
					command.DatabaseName != config.PCSMDatabase {
					return
				}

				announce()
				select {
				case <-released:
				case <-ctx.Done():
				}
			},
		}))
	require.NoError(t, err)
	defer func() { require.NoError(t, periodic.Disconnect(t.Context())) }()

	fenced := make(chan error, 1)
	go func() { fenced <- DoCheckpoint(ctx, periodic, pipeline, 1, "pcsm-periodic") }()

	select {
	case <-inserting:
	case <-ctx.Done():
		require.FailNow(t, "the periodic checkpoint never reached its bootstrap insert")
	}

	// When: the state-change callback bootstraps the document first, in the
	// same term, and the periodic insert then lands on an existing _id.
	require.NoError(t, DoCheckpoint(ctx, target, pipeline, 1, "pcsm-state-change"))
	close(released)

	// Then: the periodic write lands on the existing document. A takeover needs
	// a stored term newer than ours, which no same-term writer can produce, and
	// treating the collision as one stops checkpointing for good.
	require.NoError(t, <-fenced,
		"a same-term writer is not a newer instance taking over")
}

// A fenced checkpoint has to leave the instance able to come back. The
// tenure it fences ends, and a lease the instance still holds at a term the
// checkpoint rejects (the lease document was reset while recovery data
// existed) is advanced past the fencing term, so the tenure that follows
// restores the checkpoint at a term it accepts.
//
//nolint:paralleltest // The recovery integration suite shares one checkpoint document.
func TestFencedTenureEndsAndTheLeaseIsAdvanced(t *testing.T) {
	target := recoveryTestClient(t)
	defer func() { require.NoError(t, target.Disconnect(t.Context())) }()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	leaseColl := target.Database(config.PCSMDatabase).Collection(config.LeaseCollection)
	require.NoError(t, leaseColl.Drop(ctx))
	require.NoError(t, recoveryColl(target).Drop(ctx))

	// Given: this instance holds the lease at term 1, with a tenure open, and a
	// checkpoint owned by term 5 exists.
	membership, err := ha.JoinMembership(ctx, target, ha.MembershipOptions{InstanceID: "pcsm-fenced", Group: "g"})
	require.NoError(t, err)
	defer func() { require.NoError(t, membership.Stop(t.Context())) }()
	s := &server{
		cfg:           &config.Config{RecoveryCheckpointInterval: time.Hour},
		sourceCluster: target,
		targetCluster: target,
		membership:    membership,
	}
	observe := s.ownershipObserver(ctx)
	defer closeTenure(ctx, s)

	membership.FirstLeaseTick(ctx, observe)
	fenced := awaitTenure(t, ctx, s)
	require.Equal(t, ha.Term(1), fenced.term)

	other := pcsm.New(ctx, target, target, mdb.ServerVersion{}, false, false)
	failed, err := bson.Marshal(bson.D{{"state", pcsm.StateFailed}})
	require.NoError(t, err)
	require.NoError(t, other.Recover(ctx, failed))
	require.NoError(t, DoCheckpoint(ctx, target, other, 5, "pcsm-other"))

	// When: the tenure's checkpoint write is fenced by the stored term.
	s.onFenced(ctx, fenced, 5)

	// Then: the tenure is over and the instance is STANDBY.
	require.Nil(t, currentTenure(s), "a fenced tenure must end")
	require.ErrorIs(t, fenced.ctx.Err(), context.Canceled)
	role, _ := membership.CurrentRole()
	require.Equal(t, ha.RoleStandby, role)

	// And: the next lease attempt, which still owns the lease at term 1,
	// advances it past the fencing term and opens nothing.
	membership.FirstLeaseTick(ctx, observe)
	require.Nil(t, currentTenure(s), "ownership at a fenced term must not open a tenure")

	var lease ha.Lease
	require.NoError(t, leaseColl.FindOne(ctx, bson.D{{"_id", ha.LeaseID}}).Decode(&lease))
	require.Equal(t, "pcsm-fenced", lease.InstanceID)
	require.Equal(t, ha.Term(6), lease.Term, "the lease term must move past the checkpoint's")

	// And: the attempt after that opens a tenure at the advanced term, which
	// restores the checkpoint and is accepted by it.
	membership.FirstLeaseTick(ctx, observe)
	repaired := awaitTenure(t, ctx, s)
	require.Equal(t, ha.Term(6), repaired.term)
	require.Equal(t, pcsm.State(pcsm.StateFailed), repaired.pipeline.Status(ctx).State,
		"the tenure must restore the checkpoint of the fencing term")
	role, term := membership.CurrentRole()
	require.Equal(t, ha.RoleActive, role)
	require.Equal(t, ha.Term(6), term)

	require.NoError(t, DoCheckpoint(ctx, target, repaired.pipeline, int64(repaired.term), "pcsm-fenced"))
	require.Equal(t, int64(6), readCheckpoint(t, ctx, target).Term)
}

// Finalize fires one state-change signal for finalizing and another for
// finalized, and the tenure's single writer saves them in order. What that
// order cannot cover is a write of the same term that lands late or out of
// order, as two writers sharing a term after a lease reset can produce. Both
// carry the same term, so the fence lets both through. The snapshot sequence
// orders them: a snapshot captured earlier must not replace one captured
// later, a newer term still replaces anything, and Recover must never rewind
// the sequence. Recover restores the finalize status
// only for StateFinalized, so a regressed checkpoint would bring a restart
// back without it even though finalization succeeded.
//
//nolint:paralleltest // The recovery integration suite shares one checkpoint document.
func TestOlderSnapshotMustNotOverwriteNewerCheckpoint(t *testing.T) {
	target := recoveryTestClient(t)
	defer func() { require.NoError(t, target.Disconnect(t.Context())) }()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	require.NoError(t, recoveryColl(target).Drop(ctx))

	pipeline := pcsm.New(ctx, target, target, mdb.ServerVersion{}, false, false)
	recoverFrom := func(doc bson.D) {
		data, err := bson.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, pipeline.Recover(ctx, data))
	}
	snapshot := func() bson.Raw {
		data, err := pipeline.Checkpoint(ctx)
		require.NoError(t, err)

		return data
	}
	storedState := func() string {
		return readCheckpoint(t, ctx, target).Data.Lookup("state").StringValue()
	}

	// Given: the run's checkpoint before finalize, then two snapshots captured
	// in order from the same pipeline: finalizing, then finalized.
	recoverFrom(bson.D{{"state", pcsm.StateFinalizing}})
	require.NoError(t, DoCheckpoint(ctx, target, pipeline, 1, "pcsm-run"))
	older := snapshot()
	recoverFrom(bson.D{{"state", pcsm.StateFinalized}})
	newer := snapshot()
	require.Greater(t, snapshotSeq(newer), snapshotSeq(older), "snapshots are numbered in capture order")

	// When: the newer snapshot is saved first and the older one lands after it
	// in the same term, as a canceled writer's late update would.
	require.NoError(t, DoCheckpoint(ctx, target, staticRecoverable{data: newer}, 1, "pcsm-finalize"))
	require.NoError(t, DoCheckpoint(ctx, target, staticRecoverable{data: older}, 1, "pcsm-finalize"),
		"a superseded snapshot of the same term is dropped, not a takeover")

	// Then: the stored checkpoint is still the newer snapshot.
	require.Equal(t, pcsm.StateFinalized, storedState(),
		"an older snapshot overwrote a newer checkpoint at the same term")
	require.Equal(t, snapshotSeq(newer), readCheckpoint(t, ctx, target).Seq)

	// And: a newer term replaces it regardless of the sequence.
	require.NoError(t, DoCheckpoint(ctx, target, staticRecoverable{data: older}, 2, "pcsm-next-term"))
	require.Equal(t, pcsm.StateFinalizing, storedState(), "a newer term always wins")

	// And: the same term is fenced by it, whatever the sequence says.
	require.ErrorIs(t, DoCheckpoint(ctx, target, staticRecoverable{data: newer}, 1, "pcsm-finalize"),
		errCheckpointFenced)

	// And: recovering an older checkpoint never rewinds the sequence, so the
	// next snapshot of this process still outranks everything it saved.
	recoverFrom(bson.D{{"state", pcsm.StateFinalized}, {"seq", int64(1)}})
	require.Greater(t, snapshotSeq(snapshot()), snapshotSeq(newer),
		"a snapshot captured after recovering older data must still be the newest")
}
