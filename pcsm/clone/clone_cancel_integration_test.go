//go:build integration

package clone_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/event"

	"github.com/percona/percona-clustersync-mongodb/pcsm/clone"
	"github.com/percona/percona-clustersync-mongodb/util"
)

// TestClone_CancellationPreservesInterruptedCheckpoint guards PCSM-352:
// cancel after capturing the oplog anchor, before inventory can complete.
func TestClone_CancellationPreservesInterruptedCheckpoint(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	anchored := make(chan struct{})
	source := connectWithMonitor(t, sourceURI, &event.CommandMonitor{
		Succeeded: func(_ context.Context, cmd *event.CommandSucceededEvent) {
			if cmd.CommandName == "appendOplogNote" {
				cancel()
				close(anchored)
			}
		},
	})
	defer func() { require.NoError(t, source.Disconnect(t.Context())) }()

	// Cancellation occurs before inventory, so no target or catalog is used.
	c := clone.NewClone(source, nil, nil, nil, &clone.Options{}, false)
	done := c.Done()

	require.NoError(t, c.Start(ctx))
	require.NoError(t, util.CtxWithTimeout(t.Context(), 10*time.Second, func(waitCtx context.Context) error {
		awaitClone(waitCtx, t, anchored, "clone did not capture its oplog anchor")
		awaitClone(waitCtx, t, done, "canceled clone did not close Done")

		return nil
	}))
	require.ErrorIs(t, ctx.Err(), context.Canceled)

	status := c.Status()
	assert.True(t, status.IsStarted())
	assert.False(t, status.StartTS.IsZero())
	require.NoError(t, status.Err)
	assert.True(t, status.FinishTime.IsZero())
	assert.False(t, status.IsFinished())

	cp := c.Checkpoint()
	require.NotNil(t, cp)
	assert.Empty(t, cp.Error)
	assert.True(t, cp.FinishTime.IsZero())

	restored := &clone.Clone{}
	require.NoError(t, restored.Recover(cp))
	recoveredStatus := restored.Status()
	require.NoError(t, recoveredStatus.Err)
	assert.True(t, recoveredStatus.IsRunning(), "recovery must recognize an interrupted clone")
	assert.False(t, recoveredStatus.IsFinished())
}
