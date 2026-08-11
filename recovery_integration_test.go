//go:build integration

package main

import (
	"context"
	"net"
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
	"github.com/percona/percona-clustersync-mongodb/errors"
)

const mongodStartupTimeout = 60 * time.Second

//nolint:gochecknoglobals // shared testcontainer for the recovery integration suite
var (
	recoveryMongoURI  string
	errRecoveryMongo  error
	recoveryMongoOnce sync.Once
)

// recoveryMongo starts the suite's MongoDB container once. No TestMain is
// possible here (cli_test.go in the external test package already defines
// one), so termination is left to the testcontainers reaper.
func recoveryMongo(t *testing.T) string {
	t.Helper()

	recoveryMongoOnce.Do(func() {
		recoveryMongoURI, errRecoveryMongo = startRecoveryMongo(context.Background())
	})
	require.NoError(t, errRecoveryMongo)

	return recoveryMongoURI
}

func startRecoveryMongo(ctx context.Context) (string, error) {
	version := os.Getenv("MONGO_VERSION")
	if version == "" {
		version = "8.0"
	}

	mongod, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "percona/percona-server-mongodb:" + version,
			ExposedPorts: []string{"27017/tcp"},
			Cmd: []string{
				"mongod", "--quiet", "--bind_ip_all", "--dbpath", "/data/db",
				"--wiredTigerCacheSizeGB", "0.5", "--port", "27017",
			},
			WaitingFor: wait.ForLog("Waiting for connections").WithStartupTimeout(mongodStartupTimeout),
		},
		Started: true,
	})
	if err != nil {
		return "", errors.Wrap(err, "start mongod container")
	}

	host, err := mongod.Host(ctx)
	if err != nil {
		return "", errors.Wrap(err, "container host")
	}

	port, err := mongod.MappedPort(ctx, "27017/tcp")
	if err != nil {
		return "", errors.Wrap(err, "container mapped port")
	}

	return "mongodb://" + net.JoinHostPort(host, port.Port()) + "/?directConnection=true", nil
}

// staticRecoverable is a Recoverable that returns fixed checkpoint bytes.
type staticRecoverable struct {
	data []byte
}

func (s staticRecoverable) Checkpoint(context.Context) ([]byte, error) { return s.data, nil }
func (s staticRecoverable) Recover(context.Context, []byte) error      { return nil }

func recoveryTestClient(t *testing.T) *mongo.Client {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(
		options.Client().ApplyURI(recoveryMongo(t)).SetServerSelectionTimeout(5 * time.Second))
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

// dataFieldType returns the BSON type of the stored checkpoint's "data" field,
// asserting it is an embedded document rather than BSON Binary.
func dataFieldType(t *testing.T, ctx context.Context, client *mongo.Client) bson.Type {
	t.Helper()

	raw, err := recoveryColl(client).FindOne(ctx, bson.D{{"_id", recoveryID}}).Raw()
	require.NoError(t, err)

	return raw.Lookup("data").Type
}

func TestDoCheckpointBootstrapAndTerm(t *testing.T) {
	ctx := t.Context()
	client := recoveryTestClient(t)
	defer func() { _ = client.Disconnect(ctx) }()

	require.NoError(t, recoveryColl(client).Drop(ctx))

	rec := staticRecoverable{data: []byte{0x05, 0x00, 0x00, 0x00, 0x00}}

	// First write bootstraps at term 1 and records the writer's instance id.
	require.NoError(t, DoCheckpoint(ctx, client, rec, 1, "pcsm-a"))
	cp := readCheckpoint(t, ctx, client)
	assert.Equal(t, int64(1), cp.Term)
	assert.Equal(t, "pcsm-a", cp.InstanceID)
	assert.Equal(t, bson.TypeEmbeddedDocument, dataFieldType(t, ctx, client),
		"bootstrap write must store data as an embedded document")

	// Same term renews.
	require.NoError(t, DoCheckpoint(ctx, client, rec, 1, "pcsm-a"))
	assert.Equal(t, int64(1), readCheckpoint(t, ctx, client).Term)
	assert.Equal(t, bson.TypeEmbeddedDocument, dataFieldType(t, ctx, client),
		"update write must store data as an embedded document")

	// Newer term advances the stored term and the recorded instance id.
	require.NoError(t, DoCheckpoint(ctx, client, rec, 2, "pcsm-b"))
	cp = readCheckpoint(t, ctx, client)
	assert.Equal(t, int64(2), cp.Term)
	assert.Equal(t, "pcsm-b", cp.InstanceID)
}

func TestDoCheckpointFencedByNewerTerm(t *testing.T) {
	ctx := t.Context()
	client := recoveryTestClient(t)
	defer func() { _ = client.Disconnect(ctx) }()

	require.NoError(t, recoveryColl(client).Drop(ctx))

	rec := staticRecoverable{data: []byte{0x05, 0x00, 0x00, 0x00, 0x00}}

	// A new active establishes term 2.
	require.NoError(t, DoCheckpoint(ctx, client, rec, 2, "pcsm-b"))

	// A deposed active still on term 1 must be fenced.
	err := DoCheckpoint(ctx, client, rec, 1, "pcsm-a")
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
	require.NoError(t, DoCheckpoint(ctx, client, rec, 1, "pcsm-a"))

	require.NoError(t, DeleteRecoveryData(ctx, client))

	count, err := recoveryColl(client).CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.Zero(t, count)
}

func TestCheckNoLegacyInstance(t *testing.T) {
	ctx := t.Context()
	client := recoveryTestClient(t)
	defer func() { _ = client.Disconnect(ctx) }()

	hbColl := client.Database(config.PCSMDatabase).Collection(config.LegacyHeartbeatCollection)
	require.NoError(t, hbColl.Drop(ctx))

	// Case 1: no heartbeat collection or doc present -> ok.
	require.NoError(t, checkNoLegacyInstance(ctx, client))

	// Case 2: fresh legacy heartbeat present -> error.
	_, err := hbColl.InsertOne(ctx, bson.D{
		{"_id", "pcsm"},
		{"time", time.Now().Unix()},
	})
	require.NoError(t, err)

	err = checkNoLegacyInstance(ctx, client)
	require.ErrorIs(t, err, errLegacyInstance)

	// Case 3: stale legacy heartbeat present (> StaleHeartbeatDuration old) -> ok.
	oldTime := time.Now().Add(-2 * config.StaleHeartbeatDuration).Unix()
	_, err = hbColl.UpdateOne(
		ctx,
		bson.D{{"_id", "pcsm"}},
		bson.D{{"$set", bson.D{{"time", oldTime}}}},
	)
	require.NoError(t, err)

	require.NoError(t, checkNoLegacyInstance(ctx, client))

	// Case 4: dropLegacyHeartbeat drops the collection -> ok.
	require.NoError(t, dropLegacyHeartbeat(ctx, client))
	require.NoError(t, checkNoLegacyInstance(ctx, client))
}
