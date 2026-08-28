//go:build integration

package repl //nolint:testpackage // Integration tests exercise unexported replication internals.

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-clustersync-mongodb/util"
)

func runReplIntegrationTest(t *testing.T, fn func(context.Context)) {
	t.Helper()

	require.NoError(t, util.CtxWithTimeout(t.Context(), 3*time.Minute, func(ctx context.Context) error {
		fn(ctx)

		return nil
	}))
}

func disconnectReplTestClient(t *testing.T, client *mongo.Client) {
	t.Helper()

	require.NoError(t, util.CtxWithTimeout(context.Background(), 10*time.Second, client.Disconnect))
}

func newReplTestReplicaSet(ctx context.Context, t *testing.T) string {
	t.Helper()

	mongoVersion := os.Getenv("MONGO_VERSION")
	if mongoVersion == "" {
		mongoVersion = "8.0"
	}

	mongod, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "percona/percona-server-mongodb:" + mongoVersion,
			ExposedPorts: []string{"27017/tcp"},
			Cmd: []string{
				"mongod", "--quiet", "--bind_ip_all", "--dbpath", "/data/db",
				"--wiredTigerCacheSizeGB", "0.5",
				"--replSet", "rs0", "--port", "27017",
			},
			WaitingFor: wait.ForLog("Waiting for connections").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, util.CtxWithTimeout(context.Background(), 10*time.Second,
			func(cleanupCtx context.Context) error {
				return mongod.Terminate(cleanupCtx)
			},
		))
	})

	exitCode, _, err := mongod.Exec(ctx, []string{
		"mongosh", "--quiet", "--eval",
		"rs.initiate({_id:'rs0', members:[{_id:0, host:'localhost:27017'}]})",
	})
	require.NoError(t, err)
	require.Zero(t, exitCode)

	host, err := mongod.Host(ctx)
	require.NoError(t, err)
	mappedPort, err := mongod.MappedPort(ctx, "27017/tcp")
	require.NoError(t, err)

	return "mongodb://" + net.JoinHostPort(host, mappedPort.Port()) + "/?directConnection=true"
}
