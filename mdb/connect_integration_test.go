//go:build integration

package mdb //nolint:testpackage // matches connect_test.go; test needs package-internal access

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/percona/percona-clustersync-mongodb/config"
)

// integrationURI is the connection string for the standalone mongod test
// container, set by TestMain. A standalone (non-replica-set) deployment is used
// deliberately: Connect strips directConnection during sanitization, so a
// single-node replica set container would fail server selection because the
// advertised member host is unreachable at the mapped port.
var integrationURI string //nolint:gochecknoglobals // test fixture set by TestMain

func TestMain(m *testing.M) {
	ctx := context.Background()

	version := os.Getenv("MONGO_VERSION")
	if version == "" {
		version = "8.0"
	}

	mongod, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "percona/percona-server-mongodb:" + version,
			ExposedPorts: []string{"27017/tcp"},
			Cmd:          []string{"mongod", "--quiet", "--bind_ip_all", "--port", "27017"},
			WaitingFor:   wait.ForLog("Waiting for connections").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "start mongod container: %v\n", err)
		os.Exit(1)
	}

	host, err := mongod.Host(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "container host: %v\n", err)
		os.Exit(1)
	}

	port, err := mongod.MappedPort(ctx, "27017/tcp")
	if err != nil {
		fmt.Fprintf(os.Stderr, "container mapped port: %v\n", err)
		os.Exit(1)
	}

	integrationURI = "mongodb://" + net.JoinHostPort(host, port.Port())

	code := m.Run()

	err = mongod.Terminate(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "terminate container: %v\n", err)
	}

	os.Exit(code)
}

// TestConnect_MaxPoolSize verifies PCSM-312: a maxPoolSize set in the connection
// string survives sanitization (it used to be stripped) and a real connection is
// established with it.
func TestConnect_MaxPoolSize(t *testing.T) {
	t.Parallel()

	const poolSize uint64 = 42

	uri := fmt.Sprintf("%s/?maxPoolSize=%d", integrationURI, poolSize)

	cfg := &config.Config{Source: uri}
	cfg.MongoDB.OperationTimeout = 10 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := Connect(ctx, uri, cfg)
	require.NoError(t, err, "Connect with maxPoolSize in the URI should succeed")

	defer func() {
		_ = client.Disconnect(ctx)
	}()

	require.NoError(t, client.Ping(ctx, nil), "ping should succeed with maxPoolSize set")
}
