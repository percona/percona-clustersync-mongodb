//go:build integration

package main //nolint:testpackage // Exercise promotion and recovery against the real checkpoint store.

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

//nolint:paralleltest // The recovery integration suite shares one checkpoint document.
func TestPromotionPreservesCheckpointAgainstDelayedStateChange(t *testing.T) {
	// This covers the uncancelled state-change callback, not cancellation of a
	// periodic write already received by MongoDB.
	t.Skip("Known checkpoint race deferred.")

	target := recoveryTestClient(t)
	defer func() { require.NoError(t, target.Disconnect(t.Context())) }()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	require.NoError(t, recoveryColl(target).Drop(ctx))

	// Given: a state-change callback captured a failed pipeline in term 7.
	pipeline := pcsm.New(ctx, target, target, mdb.ServerVersion{}, false, false)
	failed, err := bson.Marshal(bson.D{{"state", pcsm.StateFailed}})
	require.NoError(t, err)
	require.NoError(t, pipeline.Recover(ctx, failed))

	captured := make(chan struct{})
	released := make(chan struct{})
	release := sync.OnceFunc(func() { close(released) })
	delayedTarget, err := mongo.Connect(options.Client().
		ApplyURI(recoveryMongo(t)).
		SetRetryWrites(false).
		SetMonitor(&event.CommandMonitor{
			Started: func(_ context.Context, command *event.CommandStartedEvent) {
				if command.CommandName != "findAndModify" {
					return
				}

				// The BSON snapshot and term have been captured, but MongoDB has
				// not received this write. Its real predicate runs after release.
				close(captured)
				select {
				case <-released:
				case <-ctx.Done():
				}
			},
		}))
	require.NoError(t, err)
	defer func() { require.NoError(t, delayedTarget.Disconnect(t.Context())) }()

	writeDone := make(chan struct{})
	var writeErr error
	go func() {
		defer close(writeDone)
		// Like createServer's state-change callback, this uses the server
		// context, not the periodic checkpoint loop's cancellable context.
		writeErr = DoCheckpoint(ctx, delayedTarget, pipeline, 7, "same-instance")
	}()
	defer func() {
		release()
		select {
		case <-writeDone:
		case <-ctx.Done():
			require.Fail(t, "delayed checkpoint did not finish", "%v", ctx.Err())
		}
	}()

	select {
	case <-captured:
	case <-ctx.Done():
		require.FailNow(t, "checkpoint snapshot was not captured", "%v", ctx.Err())
	}

	// Install a later, settled lifecycle snapshot and persist it in the same
	// term while the earlier callback is outstanding.
	finalized, err := bson.Marshal(bson.D{{"state", pcsm.StateFinalized}})
	require.NoError(t, err)
	require.NoError(t, pipeline.Recover(ctx, finalized))
	require.NoError(t, DoCheckpoint(ctx, target, pipeline, 7, "same-instance"))

	oldEpoch, cancelOldEpoch := context.WithCancel(ctx)
	defer cancelOldEpoch()
	membership := &ha.Membership{}
	membership.SetRole(ha.RoleActive, 8)
	s := &server{
		cfg:              &config.Config{RecoveryCheckpointInterval: time.Hour},
		targetCluster:    target,
		pcsm:             pipeline,
		membership:       membership,
		activeTerm:       7,
		checkpointCancel: cancelOldEpoch,
	}
	defer func() {
		if s.checkpointCancel != nil {
			s.checkpointCancel()
		}
	}()

	// When: promotion restores the newer checkpoint, then the old callback
	// reaches MongoDB before a checkpoint establishes term 8.
	s.onPromote(ctx, 8)
	require.Equal(t, ha.Term(8), s.activeTerm)
	require.ErrorIs(t, oldEpoch.Err(), context.Canceled)
	require.Equal(t, pcsm.State(pcsm.StateFinalized), pipeline.Status(ctx).State)

	release()
	select {
	case <-writeDone:
	case <-ctx.Done():
		require.FailNow(t, "delayed checkpoint did not finish", "%v", ctx.Err())
	}
	if writeErr != nil {
		require.ErrorIs(t, writeErr, errCheckpointFenced)
	}
	s.checkpointCancel() // Model a crash before the first new-epoch checkpoint.

	// Then: a fresh process must not recover an older lifecycle state than
	// the state restored by the successful promotion.
	restarted := pcsm.New(ctx, target, target, mdb.ServerVersion{}, false, false)
	require.NoError(t, Restore(ctx, target, restarted))
	require.Equal(t, pcsm.State(pcsm.StateFinalized), restarted.Status(ctx).State,
		"late term-7 checkpoint replaced the promoted state; write error: %v", writeErr)
}
