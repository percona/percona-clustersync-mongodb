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
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/util"
)

// integrationURI is the connection string for the standalone mongod test
// container, set by TestMain. A standalone (non-replica-set) deployment is used
// deliberately: Connect strips directConnection during sanitization, so a
// single-node replica set container would fail server selection because the
// advertised member host is unreachable at the mapped port.
var integrationURI string //nolint:gochecknoglobals // test fixture set by TestMain

func mongoImage() string {
	version := os.Getenv("MONGO_VERSION")
	if version == "" {
		version = "8.0.29-13"
	}

	return "percona/percona-server-mongodb:" + version
}

// startMongod starts a standalone mongod container and returns its URI.
func startMongod(ctx context.Context, extraArgs ...string) (testcontainers.Container, string, error) {
	cmd := append([]string{"mongod", "--quiet", "--bind_ip_all", "--port", "27017"}, extraArgs...)

	mongod, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        mongoImage(),
			ExposedPorts: []string{"27017/tcp"},
			Cmd:          cmd,
			WaitingFor:   wait.ForLog("Waiting for connections").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		return nil, "", fmt.Errorf("start mongod container: %w", err)
	}

	host, err := mongod.Host(ctx)
	if err != nil {
		return mongod, "", fmt.Errorf("container host: %w", err)
	}

	port, err := mongod.MappedPort(ctx, "27017/tcp")
	if err != nil {
		return mongod, "", fmt.Errorf("container mapped port: %w", err)
	}

	return mongod, "mongodb://" + net.JoinHostPort(host, port.Port()), nil
}

func TestMain(m *testing.M) {
	ctx := context.Background()

	mongod, uri, err := startMongod(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)

		if mongod != nil {
			_ = mongod.Terminate(ctx)
		}

		os.Exit(1)
	}

	integrationURI = uri

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

// TestGetFCV verifies getParameter works through the Stable API client and that
// a freshly started deployment reports an FCV matching its binary major version.
func TestGetFCV(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{Source: integrationURI}
	cfg.MongoDB.OperationTimeout = 10 * time.Second

	require.NoError(t, util.CtxWithTimeout(t.Context(), 30*time.Second, func(ctx context.Context) error {
		client, err := Connect(ctx, integrationURI, cfg)
		require.NoError(t, err)

		defer func() {
			_ = client.Disconnect(ctx)
		}()

		version, err := Version(ctx, client)
		require.NoError(t, err)

		fcv, err := GetFCV(ctx, client, false, version)
		require.NoError(t, err)
		require.Equal(t, version.Major(), fcv.Major)
		require.NoError(t, CheckFCVCompat(fcv, fcv))

		// The admin.system.version path used for mongos < 8.0 must agree.
		fallback, err := fcvFromSystemVersion(ctx, client)
		require.NoError(t, err)
		require.Equal(t, fcv, fallback)

		return nil
	}))
}

// TestGetFCV_LoweredAndTransition runs against a dedicated container, since it
// changes the FCV. It verifies that a lowered FCV is reported and rejected as a
// replication target for a source at the binary FCV, and that an FCV upgrade or
// downgrade left in progress (via the failUpgrading / failDowngrading failpoints)
// is reported as FCVTransitionError by both retrieval paths.
func TestGetFCV_LoweredAndTransition(t *testing.T) {
	t.Parallel()

	setupCtx := context.Background()

	mongod, uri, err := startMongod(setupCtx, "--setParameter", "enableTestCommands=1")
	if mongod != nil {
		t.Cleanup(func() { _ = mongod.Terminate(context.Background()) })
	}

	require.NoError(t, err)

	cfg := &config.Config{Source: uri}
	cfg.MongoDB.OperationTimeout = 30 * time.Second

	require.NoError(t, util.CtxWithTimeout(t.Context(), 3*time.Minute, func(ctx context.Context) error {
		client, err := Connect(ctx, uri, cfg)
		require.NoError(t, err)

		defer func() {
			_ = client.Disconnect(ctx)
		}()

		// Admin commands (setFeatureCompatibilityVersion, configureFailPoint)
		// go through a plain client, outside the Stable API.
		admin, err := mongo.Connect(options.Client().ApplyURI(uri))
		require.NoError(t, err)

		defer func() {
			_ = admin.Disconnect(ctx)
		}()

		version, err := Version(ctx, client)
		require.NoError(t, err)

		current := FCV{Major: version.Major()}
		lower := FCV{Major: current.Major - 1}

		setFCV := func(fcv FCV) error {
			cmd := bson.D{{"setFeatureCompatibilityVersion", fcv.String()}}
			if current.Major >= 7 { //nolint:mnd // confirm was added in 7.0
				cmd = append(cmd, bson.E{"confirm", true})
			}

			return admin.Database("admin").RunCommand(ctx, cmd).Err()
		}

		failPoint := func(name, mode string) {
			require.NoError(t, admin.Database("admin").RunCommand(ctx, bson.D{
				{"configureFailPoint", name}, {"mode", mode},
			}).Err())
		}

		requireTransition := func() {
			var transition FCVTransitionError

			_, err := GetFCV(ctx, client, false, version)
			require.ErrorAs(t, err, &transition)

			_, err = fcvFromSystemVersion(ctx, client)
			require.ErrorAs(t, err, &transition)
		}

		requireFCV := func(want FCV) {
			got, err := GetFCV(ctx, client, false, version)
			require.NoError(t, err)
			require.Equal(t, want, got)

			got, err = fcvFromSystemVersion(ctx, client)
			require.NoError(t, err)
			require.Equal(t, want, got)
		}

		// Stuck downgrade.
		failPoint("failDowngrading", "alwaysOn")
		require.Error(t, setFCV(lower))
		requireTransition()
		failPoint("failDowngrading", "off")

		// Completed downgrade: same binaries, lower FCV.
		require.NoError(t, setFCV(lower))
		requireFCV(lower)

		var downgrade FCVDowngradeError
		require.ErrorAs(t, CheckFCVCompat(current, lower), &downgrade)
		require.NoError(t, CheckFCVCompat(lower, current))

		// Stuck upgrade.
		failPoint("failUpgrading", "alwaysOn")
		require.Error(t, setFCV(current))
		requireTransition()
		failPoint("failUpgrading", "off")

		require.NoError(t, setFCV(current))
		requireFCV(current)

		return nil
	}))
}
