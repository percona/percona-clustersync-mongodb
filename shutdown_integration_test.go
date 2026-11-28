//go:build integration && !windows

package main_test

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/ha"
)

// TestShutdownSignalReleasesHAState checks that a server stopped with SIGINT
// or SIGTERM releases its lease and removes its member document before it
// exits. Kubernetes, systemd and docker stop send SIGTERM.
//
//nolint:paralleltest // The cases share one target, which holds a single lease.
func TestShutdownSignalReleasesHAState(t *testing.T) {
	source := startShutdownMongo(t)
	target := startShutdownMongo(t)

	ctx := t.Context()

	client, err := mongo.Connect(options.Client().ApplyURI(target))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })

	db := client.Database(config.PCSMDatabase)
	members := db.Collection(config.MembersCollection)
	lease := db.Collection(config.LeaseCollection)

	for _, tt := range []struct {
		name   string
		signal os.Signal
	}{
		{"SIGINT", os.Interrupt},
		{"SIGTERM", syscall.SIGTERM},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, db.Drop(ctx))

			port := freePort(t, ctx)

			// The source and target are standalones: the server starts as
			// ACTIVE without a run, which needs no replica set.
			var output bytes.Buffer
			cmd := exec.CommandContext(ctx, binaryPath,
				"--source", source, "--target", target, "--port", strconv.Itoa(port))
			cmd.Stdout = &output
			cmd.Stderr = &output
			require.NoError(t, cmd.Start())

			// waitErr is set before done is closed.
			var waitErr error
			done := make(chan struct{})
			go func() {
				waitErr = cmd.Wait()
				close(done)
			}()

			defer func() {
				select {
				case <-done:
				default:
					_ = cmd.Process.Kill()
					<-done
				}
				if t.Failed() {
					t.Logf("pcsm output:\n%s", output.String())
				}
			}()

			waitForMetrics(t, ctx, port, done)

			var member ha.Member
			require.NoError(t, members.FindOne(ctx, bson.D{}).Decode(&member))
			require.NotEmpty(t, member.InstanceID)

			held, err := lease.CountDocuments(ctx, bson.D{
				{"_id", ha.LeaseID},
				{"instanceId", member.InstanceID},
				{"$expr", bson.D{{"$gt", bson.A{"$expiresAt", "$$NOW"}}}},
			})
			require.NoError(t, err)
			require.Equal(t, int64(1), held, "the server holds the lease before the signal")

			require.NoError(t, cmd.Process.Signal(tt.signal))

			// Close is bounded by config.DisconnectTimeout.
			select {
			case <-done:
			case <-time.After(15 * time.Second):
				require.FailNow(t, "pcsm did not exit after the signal")
			}

			assert.NoError(t, waitErr, "pcsm exit")

			left, err := members.CountDocuments(ctx, bson.D{})
			require.NoError(t, err)
			assert.Zero(t, left, "member documents left after exit")

			released, err := lease.CountDocuments(ctx, bson.D{
				{"_id", ha.LeaseID},
				{"instanceId", member.InstanceID},
				{"$expr", bson.D{{"$lte", bson.A{"$expiresAt", "$$NOW"}}}},
			})
			require.NoError(t, err)
			assert.Equal(t, int64(1), released, "the lease is released at exit")
		})
	}
}

// startShutdownMongo starts a standalone mongod for the test. It is
// terminated on cleanup.
func startShutdownMongo(t *testing.T) string {
	t.Helper()

	version := os.Getenv("MONGO_VERSION")
	if version == "" {
		version = "8.0.29-13"
	}

	ctx := t.Context()

	mongod, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Image:        "percona/percona-server-mongodb:" + version,
		ExposedPorts: []string{"27017/tcp"},
		Cmd: []string{
			"mongod", "--quiet", "--bind_ip_all", "--dbpath", "/data/db",
			"--wiredTigerCacheSizeGB", "0.5", "--port", "27017",
		},
		WaitingFor: wait.ForLog("Waiting for connections").WithStartupTimeout(time.Minute),
		Started:    true,
	})
	testcontainers.CleanupContainer(t, mongod)
	require.NoError(t, err)

	host, err := mongod.Host(ctx)
	require.NoError(t, err)
	port, err := mongod.MappedPort(ctx, "27017/tcp")
	require.NoError(t, err)

	return "mongodb://" + net.JoinHostPort(host, port.Port()) + "/?directConnection=true"
}

// freePort returns a TCP port that is free on localhost.
func freePort(t *testing.T, ctx context.Context) int {
	t.Helper()

	l, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "localhost:0")
	require.NoError(t, err)

	port := l.Addr().(*net.TCPAddr).Port //nolint:forcetypeassert // A TCP listener has a TCP address.
	require.NoError(t, l.Close())

	return port
}

// waitForMetrics waits until the server answers /metrics, which it serves on
// every role once startup is done. An exit, signaled by done, fails the test.
func waitForMetrics(t *testing.T, ctx context.Context, port int, done <-chan struct{}) {
	t.Helper()

	url := "http://localhost:" + strconv.Itoa(port) + "/metrics"
	deadline := time.After(30 * time.Second)

	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}

		select {
		case <-done:
			require.FailNow(t, "pcsm exited before it was ready")
		case <-deadline:
			require.FailNow(t, "pcsm was not ready in 30s")
		case <-time.After(200 * time.Millisecond):
		}
	}
}
