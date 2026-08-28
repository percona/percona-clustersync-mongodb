//go:build integration

package repl //nolint:testpackage // Integration tests exercise unexported replication internals.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/sel"
)

//nolint:paralleltest // MongoDB testcontainers are serialized to avoid Docker resource contention.
func TestRepl_Recover_clampsReportedFrontierToCheckpointForInterruptedRun(t *testing.T) {
	runReplIntegrationTest(t, func(setupCtx context.Context) {
		// Given
		mongoURI := newReplTestReplicaSet(setupCtx, t)
		client, err := mongo.Connect(
			options.Client().
				ApplyURI(mongoURI).
				SetServerSelectionTimeout(30 * time.Second),
		)
		require.NoError(t, err)
		t.Cleanup(func() {
			disconnectReplTestClient(t, client)
		})
		require.NoError(t, client.Ping(setupCtx, nil))

		sourceVersion, err := mdb.Version(setupCtx, client)
		require.NoError(t, err)
		r := NewRepl(
			client,
			client,
			catalog.NewCatalog(client, client, sourceVersion),
			sel.AllowAllFilter,
			&Options{NumWorkers: 1},
			sourceVersion,
			false,
			false,
		)
		startTime := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
		lastReplicatedOpTime := bson.Timestamp{T: 200, I: 1}
		checkpointOpTime := bson.Timestamp{T: 100, I: 1}
		cp := &Checkpoint{
			StartTime:            startTime,
			LastReplicatedOpTime: lastReplicatedOpTime,
			CheckpointOpTime:     checkpointOpTime,
		}
		require.False(t, cp.StartTime.IsZero())
		require.True(t, cp.PauseTime.IsZero())
		require.True(t, cp.LastReplicatedOpTime.After(cp.CheckpointOpTime))

		// When
		require.NoError(t, r.Recover(setupCtx, cp))
		status := r.Status()

		// Then
		require.Equal(t, cp.CheckpointOpTime, status.CheckpointOpTime)
		require.Equal(t, cp.CheckpointOpTime, status.LastReplicatedOpTime,
			"recovered reported frontier overtook applied checkpoint before replay caught up")
	})
}
