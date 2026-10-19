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

// A run writes its first checkpoint from two places at once: the periodic loop
// and the state-change callback. Neither finds a document, so both bootstrap
// it, and the loser reads its duplicate key as a takeover by a newer instance.
// Both writers hold the same term, so neither has been deposed.
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

// A demotion has to leave the instance able to come back. A fence-driven
// demotion stops checkpointing; if it tells membership nothing, a still-held
// lease keeps renewing as ACTIVE, reports no transition, and never triggers a
// promotion to restart the loop or resume the pipeline. The recorded role is
// what turns that next renewal into a transition, so that is what this checks.
//
//nolint:paralleltest // The recovery integration suite shares one checkpoint document.
func TestDemotionRecordsStandbyRole(t *testing.T) {
	target := recoveryTestClient(t)
	defer func() { require.NoError(t, target.Disconnect(t.Context())) }()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	// Given: an ACTIVE instance in term 1 with an open epoch and no lease
	// document, so the fence cannot be a stale lease term to repair.
	require.NoError(t, target.Database(config.PCSMDatabase).Collection(config.LeaseCollection).Drop(ctx))
	membership, err := ha.JoinMembership(ctx, target, ha.MembershipOptions{InstanceID: "pcsm-demoted", Group: "g"})
	require.NoError(t, err)
	defer func() { require.NoError(t, membership.Stop(t.Context())) }()
	membership.SetRole(ha.RoleActive, 1)
	epochCtx, epochCancel := context.WithCancel(ctx)
	defer epochCancel()
	s := &server{
		cfg:           &config.Config{RecoveryCheckpointInterval: time.Second},
		targetCluster: target,
		pcsm:          pcsm.New(ctx, target, target, mdb.ServerVersion{}, false, false),
		membership:    membership,
		activeTerm:    1,
		epochCtx:      epochCtx,
		epochCancel:   epochCancel,
	}

	// When: a checkpoint owned by term 5 fences it.
	s.onFenced(ctx, epochCtx, 1, 5)
	require.Nil(t, s.epochCancel, "demotion must stop checkpointing")

	// Then: the demotion is visible to membership, so the next renewal of the
	// lease it still holds is a real transition back to ACTIVE.
	role, term := membership.CurrentRole()
	require.Equal(t, ha.RoleStandby, role,
		"a demoted instance that still reports ACTIVE is never promoted again, "+
			"so checkpointing stays stopped and a demotion pause is never resumed")
	require.Equal(t, ha.Term(1), term, "a step-down keeps the term")

	// And: the fence belongs to the epoch that raised it. Once a new epoch of
	// the same term is open, a late callback of the old one must not end it;
	// a term does not identify an epoch.
	membership.SetRole(ha.RoleActive, 1)
	s.epochCtx, s.epochCancel = context.WithCancel(ctx)
	defer s.epochCancel()

	s.onFenced(ctx, epochCtx, 1, 5)
	require.NotNil(t, s.epochCancel, "a late fence of an ended epoch must not end the new one")
	role, _ = membership.CurrentRole()
	require.Equal(t, ha.RoleActive, role, "a late fence of an ended epoch must not demote")
}

// Finalize fires one state-change signal for finalizing and another for
// finalized, and the epoch's single writer saves them in order. What that
// order cannot cover is a write of the same term that lands late: the previous
// epoch's writer, canceled with its update on the wire, is joined by the next
// promotion, but MongoDB may still apply its older snapshot after the new
// epoch saved a newer one. Both carry the same term, so the fence lets both
// through. The snapshot sequence orders them: a snapshot captured earlier must
// not replace one captured later, a newer term still replaces anything, and
// Recover must never rewind the sequence. Recover restores the finalize status
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
