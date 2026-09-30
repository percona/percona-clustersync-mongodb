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
		t.Fatal("the periodic checkpoint never reached its bootstrap insert")
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

// A demotion has to leave the instance able to come back. onDemote stops
// checkpointing but tells membership nothing, so a still-held lease keeps
// renewing as ACTIVE, reports no transition, and never triggers a promotion to
// restart the loop or resume the pipeline. The recorded role is what turns that
// next renewal into a transition, so that is what this checks.
//
//nolint:paralleltest // The recovery integration suite shares one checkpoint document.
func TestDemotionRecordsStandbyRole(t *testing.T) {
	target := recoveryTestClient(t)
	defer func() { require.NoError(t, target.Disconnect(t.Context())) }()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	// Given: an ACTIVE instance in term 1 with a running checkpoint loop.
	membership := &ha.Membership{}
	membership.SetRole(ha.RoleActive, 1)
	cpCtx, cpCancel := context.WithCancel(ctx)
	defer cpCancel()
	s := &server{
		cfg:              &config.Config{RecoveryCheckpointInterval: time.Second},
		targetCluster:    target,
		pcsm:             pcsm.New(ctx, target, target, mdb.ServerVersion{}, false, false),
		membership:       membership,
		activeTerm:       1,
		checkpointCancel: cpCancel,
	}
	go RunCheckpointing(cpCtx, target, s.pcsm, 1, "pcsm-demoted",
		time.Second, func() { s.onDemote(ctx, 1) })

	// When: a fenced checkpoint demotes it while it still holds the lease.
	s.onDemote(ctx, 1)
	require.Nil(t, s.checkpointCancel, "demotion must stop checkpointing")

	// Then: the demotion is visible to membership, so the next renewal of the
	// lease it still holds is a real transition back to ACTIVE.
	role, _ := membership.CurrentRole()
	require.Equal(t, ha.RoleStandby, role,
		"a demoted instance that still reports ACTIVE is never promoted again, "+
			"so checkpointing stays stopped and a demotion pause is never resumed")
}

// Finalize fires one state-change callback for finalizing and another for
// finalized. Each snapshots the state under the pipeline lock but writes to
// MongoDB after releasing it, and nothing orders the two writes. Both hold the
// same term, so the fence lets both through and whichever lands last decides
// the stored state, which can leave the checkpoint behind the run it describes.
// Recover restores the finalize status only for StateFinalized, so a restart
// then comes back without it even though finalization succeeded.
//
//nolint:paralleltest // The recovery integration suite shares one checkpoint document.
func TestOlderSnapshotMustNotOverwriteNewerCheckpoint(t *testing.T) {
	target := recoveryTestClient(t)
	defer func() { require.NoError(t, target.Disconnect(t.Context())) }()

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	require.NoError(t, recoveryColl(target).Drop(ctx))

	recovered := func(state string) *pcsm.PCSM {
		p := pcsm.New(ctx, target, target, mdb.ServerVersion{}, false, false)
		data, err := bson.Marshal(bson.D{{"state", state}})
		require.NoError(t, err)
		require.NoError(t, p.Recover(ctx, data))

		return p
	}

	// The checkpoint the run wrote before finalize, so both racing writes
	// update an existing document instead of bootstrapping one.
	require.NoError(t, DoCheckpoint(ctx, target, recovered(pcsm.StateRunning), 1, "pcsm-run"))

	writers := []*pcsm.PCSM{recovered(pcsm.StateFinalizing), recovered(pcsm.StateFinalized)}

	// Which write MongoDB applies last is a coin flip, so repeat rather than
	// force an order. A run that stays correct throughout is the passing case.
	for attempt := 1; attempt <= 50; attempt++ {
		start := make(chan struct{})
		errs := make([]error, len(writers))

		var racing sync.WaitGroup
		for i, rec := range writers {
			racing.Go(func() {
				<-start
				errs[i] = DoCheckpoint(ctx, target, rec, 1, "pcsm-finalize")
			})
		}

		close(start)
		racing.Wait()
		require.NoError(t, errs[0])
		require.NoError(t, errs[1])

		var stored struct {
			Data struct {
				State string `bson:"state"`
			} `bson:"data"`
		}
		require.NoError(t, recoveryColl(target).
			FindOne(ctx, bson.D{{"_id", recoveryID}}).Decode(&stored))
		require.Equal(t, pcsm.StateFinalized, stored.Data.State,
			"attempt %d: an older snapshot overwrote a newer checkpoint at the same term",
			attempt)
	}
}
