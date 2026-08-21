//go:build integration

package repl //nolint:testpackage

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

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
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, mongod.Terminate(cleanupCtx))
	})

	exitCode, _, err := mongod.Exec(ctx, []string{
		"mongosh", "--quiet", "--eval",
		"rs.initiate({_id:'rs0', members:[{_id:0, host:'localhost:27017'}]})",
	})
	require.NoError(t, err)
	require.Zero(t, exitCode)
	require.Eventually(t, func() bool {
		exitCode, _, execErr := mongod.Exec(ctx, []string{
			"mongosh", "--quiet", "--eval", "exit(db.hello().isWritablePrimary ? 0 : 1)",
		})

		return execErr == nil && exitCode == 0
	}, 60*time.Second, 100*time.Millisecond, "replica set did not elect a writable primary")

	host, err := mongod.Host(ctx)
	require.NoError(t, err)
	mappedPort, err := mongod.MappedPort(ctx, "27017/tcp")
	require.NoError(t, err)

	return "mongodb://" + net.JoinHostPort(host, mappedPort.Port()) + "/?directConnection=true"
}
