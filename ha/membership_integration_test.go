//go:build integration

package ha //nolint:testpackage

import (
	"context"
	"fmt"
	"os"
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

var mongoURI string

func TestMain(m *testing.M) {
	ctx := context.Background()

	mongoVersion := os.Getenv("MONGO_VERSION")
	if mongoVersion == "" {
		mongoVersion = "8.0"
	}
	image := "percona/percona-server-mongodb:" + mongoVersion

	mongod, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{"27017/tcp"},
			Cmd: []string{
				"mongod", "--quiet", "--bind_ip_all", "--dbpath", "/data/db",
				"--wiredTigerCacheSizeGB", "0.5",
				"--port", "27017",
			},
			WaitingFor: wait.ForLog("Waiting for connections").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start mongod: %v\n", err)
		os.Exit(1)
	}

	cleanup := func() {
		if mongod != nil {
			_ = mongod.Terminate(ctx)
		}
	}

	exitWithError := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
		cleanup()
		os.Exit(1)
	}

	host, err := mongod.Host(ctx)
	if err != nil {
		exitWithError("Failed to get host: %v", err)
	}

	mappedPort, err := mongod.MappedPort(ctx, "27017/tcp")
	if err != nil {
		exitWithError("Failed to get port: %v", err)
	}

	mongoURI = fmt.Sprintf("mongodb://%s:%s/?directConnection=true", host, mappedPort.Port())

	exitCode := m.Run()

	cleanup()
	os.Exit(exitCode)
}

func connectToMongoDB(t *testing.T) *mongo.Client {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(mongoURI).SetServerSelectionTimeout(5 * time.Second))
	require.NoError(t, err, "MongoDB connection should succeed")

	err = client.Ping(ctx, nil)
	require.NoError(t, err, "MongoDB ping should succeed")

	return client
}

func membersCollection(client *mongo.Client) *mongo.Collection {
	return client.Database(config.PCSMDatabase).Collection(config.MembersCollection)
}

// cleanState drops the members and legacy heartbeat collections so each test
// starts from a known baseline.
func cleanState(t *testing.T, ctx context.Context, client *mongo.Client) {
	t.Helper()

	require.NoError(t, membersCollection(client).Drop(ctx))
	require.NoError(t, client.Database(config.PCSMDatabase).Collection(config.HeartbeatCollection).Drop(ctx))
}

func TestJoinMembershipWritesMemberDoc(t *testing.T) {
	ctx := t.Context()
	client := connectToMongoDB(t)
	defer func() { _ = client.Disconnect(ctx) }()

	cleanState(t, ctx, client)

	m, err := JoinMembership(ctx, client, MembershipOptions{
		InstanceID:  "pcsm-test-1",
		Host:        "test-host",
		Port:        2242,
		PCSMVersion: "v-test",
	})
	require.NoError(t, err)

	defer func() { _ = m.Stop(ctx) }()

	var doc Member
	err = membersCollection(client).FindOne(ctx, bson.D{{"_id", "pcsm-test-1"}}).Decode(&doc)
	require.NoError(t, err, "member document should exist after JoinMembership")

	assert.Equal(t, "pcsm-test-1", doc.InstanceID)
	assert.Equal(t, "test-host", doc.Host)
	assert.Equal(t, 2242, doc.Port)
	assert.Equal(t, "v-test", doc.PCSMVersion)
	assert.Equal(t, RoleStandby, doc.Role, "default advertised role is STANDBY")
	assert.Equal(t, int64(0), doc.Term)
	assert.False(t, doc.LastHeartbeat.IsZero(), "lastHeartbeat should be stamped by the server")
}

func TestMembershipRefreshesHeartbeat(t *testing.T) {
	ctx := t.Context()
	client := connectToMongoDB(t)
	defer func() { _ = client.Disconnect(ctx) }()

	cleanState(t, ctx, client)

	m, err := JoinMembership(ctx, client, MembershipOptions{InstanceID: "pcsm-refresh"})
	require.NoError(t, err)

	defer func() { _ = m.Stop(ctx) }()

	var first Member
	require.NoError(t,
		membersCollection(client).FindOne(ctx, bson.D{{"_id", "pcsm-refresh"}}).Decode(&first))

	// SetRole triggers an immediate beat; the doc should reflect the new role
	// and a newer lastHeartbeat shortly after.
	m.SetRole(RoleActive, 3)

	require.Eventually(t, func() bool {
		var cur Member
		if err := membersCollection(client).
			FindOne(ctx, bson.D{{"_id", "pcsm-refresh"}}).Decode(&cur); err != nil {
			return false
		}

		return cur.Role == RoleActive && cur.Term == 3 && cur.LastHeartbeat.After(first.LastHeartbeat)
	}, 5*time.Second, 100*time.Millisecond, "member doc should reflect promoted role and a refreshed heartbeat")
}

func TestStopRemovesMemberDoc(t *testing.T) {
	ctx := t.Context()
	client := connectToMongoDB(t)
	defer func() { _ = client.Disconnect(ctx) }()

	cleanState(t, ctx, client)

	m, err := JoinMembership(ctx, client, MembershipOptions{InstanceID: "pcsm-stop"})
	require.NoError(t, err)

	require.NoError(t, m.Stop(ctx))

	count, err := membersCollection(client).CountDocuments(ctx, bson.D{{"_id", "pcsm-stop"}})
	require.NoError(t, err)
	assert.Equal(t, int64(0), count, "Stop should remove the member document")
}

func TestMembersFiltersStale(t *testing.T) {
	ctx := t.Context()
	client := connectToMongoDB(t)
	defer func() { _ = client.Disconnect(ctx) }()

	cleanState(t, ctx, client)

	// A fresh member (recent heartbeat) and a stale member (old heartbeat).
	now := time.Now().UTC()
	_, err := membersCollection(client).InsertMany(ctx, []any{
		bson.D{
			{"_id", "fresh"},
			{"role", RoleActive},
			{"term", int64(1)},
			{"lastHeartbeat", now},
		},
		bson.D{
			{"_id", "stale"},
			{"role", RoleStandby},
			{"term", int64(1)},
			{"lastHeartbeat", now.Add(-2 * config.StaleMemberDuration)},
		},
	})
	require.NoError(t, err)

	members, err := Members(ctx, client)
	require.NoError(t, err)

	ids := make(map[string]bool, len(members))
	for _, mem := range members {
		ids[mem.InstanceID] = true
	}

	assert.True(t, ids["fresh"], "fresh member should be returned")
	assert.False(t, ids["stale"], "stale member should be filtered out")
}

func TestCleanupLegacyHeartbeat(t *testing.T) {
	ctx := t.Context()
	client := connectToMongoDB(t)
	defer func() { _ = client.Disconnect(ctx) }()

	cleanState(t, ctx, client)

	legacy := client.Database(config.PCSMDatabase).Collection(config.HeartbeatCollection)
	_, err := legacy.InsertOne(ctx, bson.D{{"_id", legacyHeartbeatID}, {"time", time.Now().Unix()}})
	require.NoError(t, err)

	// JoinMembership cleans up the legacy singleton on startup.
	m, err := JoinMembership(ctx, client, MembershipOptions{InstanceID: "pcsm-legacy"})
	require.NoError(t, err)

	defer func() { _ = m.Stop(ctx) }()

	count, err := legacy.CountDocuments(ctx, bson.D{{"_id", legacyHeartbeatID}})
	require.NoError(t, err)
	assert.Equal(t, int64(0), count, "legacy singleton heartbeat doc should be removed")
}

func TestDeleteMembers(t *testing.T) {
	ctx := t.Context()
	client := connectToMongoDB(t)
	defer func() { _ = client.Disconnect(ctx) }()

	cleanState(t, ctx, client)

	_, err := membersCollection(client).InsertMany(ctx, []any{
		bson.D{{"_id", "a"}, {"lastHeartbeat", time.Now()}},
		bson.D{{"_id", "b"}, {"lastHeartbeat", time.Now()}},
	})
	require.NoError(t, err)

	require.NoError(t, DeleteMembers(ctx, client))

	count, err := membersCollection(client).CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), count, "DeleteMembers should remove all member documents")
}
