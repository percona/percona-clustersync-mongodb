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

// A checkpoint write of an ended tenure that reaches the server after the
// next tenure claimed the document is rejected, whatever it holds, so a
// process restarted after that promotion recovers what the promotion
// restored, never the late write. The write is held before the server by the
// command monitor, so the race is driven, not waited for.
//
//nolint:paralleltest // The recovery integration suite shares one checkpoint document.
func TestLateWriteOfEndedTermCannotRegressTheClaimedCheckpoint(t *testing.T) {
	target := recoveryTestClient(t)
	defer func() { require.NoError(t, target.Disconnect(t.Context())) }()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	require.NoError(t, recoveryColl(target).Drop(ctx))

	// Given: the tenure at term 7 claimed the checkpoint and saved a
	// finalizing snapshot.
	pipeline := pcsm.New(ctx, target, target, mdb.ServerVersion{}, false, false)
	recoverFrom := func(state pcsm.State) {
		data, err := bson.Marshal(bson.D{{"state", state}})
		require.NoError(t, err)
		require.NoError(t, pipeline.Recover(ctx, data))
	}
	recoverFrom(pcsm.StateFinalizing)
	require.ErrorIs(t, Restore(ctx, target, pipeline, 7, "pcsm-old"), errNoCheckpoint)
	require.NoError(t, DoCheckpoint(ctx, target, pipeline, 7, "pcsm-old"))

	// And: its next write, a finalized snapshot, has been captured but not
	// received by the server when the tenure ends; its outcome is unknown.
	recoverFrom(pcsm.StateFinalized)

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
		writeErr = DoCheckpoint(ctx, delayedTarget, pipeline, 7, "pcsm-old")
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

	s := &server{
		cfg:           &config.Config{RecoveryCheckpointInterval: time.Hour},
		sourceCluster: target,
		targetCluster: target,
		membership:    &ha.Membership{},
	}
	defer closeTenure(ctx, s)

	// When: ownership at term 8 opens a tenure, which claims the document
	// and restores the finalizing snapshot, and the held write then reaches
	// the server.
	s.observeOwnership(ctx, ha.Ownership{Held: true, Term: 8})
	promoted := awaitTenure(t, ctx, s)
	require.Equal(t, ha.Term(8), promoted.term)
	require.Equal(t, pcsm.State(pcsm.StateFinalizing), promoted.pipeline.Status(ctx).State)

	release()
	select {
	case <-writeDone:
	case <-ctx.Done():
		require.FailNow(t, "delayed checkpoint did not finish", "%v", ctx.Err())
	}

	// Then: the late write is fenced, and the document is still the tenure's.
	require.ErrorIs(t, writeErr, errCheckpointFenced)
	require.Equal(t, int64(8), readCheckpoint(t, ctx, target).Term)
	promoted.cancel() // Model a crash before the first checkpoint of the new tenure.

	// And: a fresh process recovers what the promotion restored.
	restarted := pcsm.New(ctx, target, target, mdb.ServerVersion{}, false, false)
	require.NoError(t, Restore(ctx, target, restarted, 9, "pcsm-new"))
	require.Equal(t, pcsm.State(pcsm.StateFinalizing), restarted.Status(ctx).State,
		"a late write of the ended term replaced the promoted state")
}
