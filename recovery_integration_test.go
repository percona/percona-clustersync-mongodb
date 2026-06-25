//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"sync"
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
)

//nolint:gochecknoglobals // shared testcontainer for the recovery integration suite
var (
	recoveryMongoURI  string
	recoveryMongoOnce sync.Once
)

// recoveryMongo starts a MongoDB testcontainer once for the suite and returns
// its URI. The container is cleaned up via t.Cleanup on the first caller.
func recoveryMongo(t *testing.T) string {
	t.Helper()

	recoveryMongoOnce.Do(func() {
		ctx := context.Background()

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
					"--wiredTigerCacheSizeGB", "0.5", "--port", "27017",
				},
				WaitingFor: wait.ForLog("Waiting for connections").WithStartupTimeout(60 * time.Second),
			},
			Started: true,
		})
		require.NoError(t, err)

		host, err := mongod.Host(ctx)
		require.NoError(t, err)

		port, err := mongod.MappedPort(ctx, "27017/tcp")
		require.NoError(t, err)

		recoveryMongoURI = fmt.Sprintf("mongodb://%s:%s/?directConnection=true", host, port.Port())
	})

	return recoveryMongoURI
}

// staticRecoverable is a Recoverable that returns fixed checkpoint bytes.
type staticRecoverable struct {
	data []byte
}

func (s staticRecoverable) Checkpoint(context.Context) ([]byte, error) { return s.data, nil }
func (s staticRecoverable) Recover(context.Context, []byte) error      { return nil }

func recoveryTestClient(t *testing.T) *mongo.Client {
	t.Helper()

	uri := recoveryMongo(t)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(5 * time.Second))
	require.NoError(t, err)
	require.NoError(t, client.Ping(ctx, nil))

	return client
}

func recoveryColl(client *mongo.Client) *mongo.Collection {
	return client.Database(config.PCSMDatabase).Collection(config.RecoveryCollection)
}

func readCheckpoint(t *testing.T, ctx context.Context, client *mongo.Client) checkpoint {
	t.Helper()

	var cp checkpoint
	err := recoveryColl(client).FindOne(ctx, bson.D{{"_id", recoveryID}}).Decode(&cp)
	require.NoError(t, err)

	return cp
}

func TestDoCheckpointBootstrapAndTerm(t *testing.T) {
	ctx := t.Context()
	client := recoveryTestClient(t)
	defer func() { _ = client.Disconnect(ctx) }()

	require.NoError(t, recoveryColl(client).Drop(ctx))

	rec := staticRecoverable{data: []byte{0x05, 0x00, 0x00, 0x00, 0x00}}

	// First write bootstraps at term 1.
	require.NoError(t, DoCheckpoint(ctx, client, rec, 1))
	cp := readCheckpoint(t, ctx, client)
	assert.Equal(t, int64(1), cp.Term)

	// Same term renews.
	require.NoError(t, DoCheckpoint(ctx, client, rec, 1))
	assert.Equal(t, int64(1), readCheckpoint(t, ctx, client).Term)

	// Newer term advances the stored term.
	require.NoError(t, DoCheckpoint(ctx, client, rec, 2))
	assert.Equal(t, int64(2), readCheckpoint(t, ctx, client).Term)
}

func TestDoCheckpointFencedByNewerTerm(t *testing.T) {
	ctx := t.Context()
	client := recoveryTestClient(t)
	defer func() { _ = client.Disconnect(ctx) }()

	require.NoError(t, recoveryColl(client).Drop(ctx))

	rec := staticRecoverable{data: []byte{0x05, 0x00, 0x00, 0x00, 0x00}}

	// A new active establishes term 2.
	require.NoError(t, DoCheckpoint(ctx, client, rec, 2))

	// A deposed active still on term 1 must be fenced.
	err := DoCheckpoint(ctx, client, rec, 1)
	require.ErrorIs(t, err, errCheckpointFenced)

	// The stored term is unchanged by the fenced write.
	assert.Equal(t, int64(2), readCheckpoint(t, ctx, client).Term)
}

func TestDeleteRecoveryData(t *testing.T) {
	ctx := t.Context()
	client := recoveryTestClient(t)
	defer func() { _ = client.Disconnect(ctx) }()

	require.NoError(t, recoveryColl(client).Drop(ctx))

	rec := staticRecoverable{data: []byte{0x05, 0x00, 0x00, 0x00, 0x00}}
	require.NoError(t, DoCheckpoint(ctx, client, rec, 1))

	require.NoError(t, DeleteRecoveryData(ctx, client))

	count, err := recoveryColl(client).CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}
