//go:build integration

package catalog_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
)

const testDB = "pcsm_test_catalog"

var mongosURI string

func TestMain(m *testing.M) {
	ctx := context.Background()

	mongoVersion := os.Getenv("MONGO_VERSION")
	if mongoVersion == "" {
		mongoVersion = "8.0"
	}
	image := "percona/percona-server-mongodb:" + mongoVersion

	// 1. Create Docker network
	nw, err := network.New(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create network: %v\n", err)
		os.Exit(1)
	}

	// Container references for cleanup
	var cfgContainer, rs0Container, rs1Container, mongosContainer testcontainers.Container

	cleanup := func() {
		if mongosContainer != nil {
			_ = mongosContainer.Terminate(ctx)
		}
		if rs1Container != nil {
			_ = rs1Container.Terminate(ctx)
		}
		if rs0Container != nil {
			_ = rs0Container.Terminate(ctx)
		}
		if cfgContainer != nil {
			_ = cfgContainer.Terminate(ctx)
		}
		if nw != nil {
			_ = nw.Remove(ctx)
		}
	}

	exitWithError := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
		cleanup()
		os.Exit(1)
	}

	// 2. Start config server
	cfgContainer, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{"28000/tcp"},
			Networks:     []string{nw.Name},
			NetworkAliases: map[string][]string{
				nw.Name: {"tgt-cfg0"},
			},
			Cmd: []string{
				"mongod", "--quiet", "--bind_ip_all", "--dbpath", "/data/db",
				"--wiredTigerCacheSizeGB", "0.25",
				"--configsvr", "--replSet", "tgt-cfg", "--port", "28000",
			},
			WaitingFor: wait.ForLog("Waiting for connections").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		exitWithError("Failed to start config server: %v", err)
	}

	// 3. Start shard rs0
	rs0Container, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{"40000/tcp"},
			Networks:     []string{nw.Name},
			NetworkAliases: map[string][]string{
				nw.Name: {"tgt-rs00"},
			},
			Cmd: []string{
				"mongod", "--quiet", "--bind_ip_all", "--dbpath", "/data/db",
				"--wiredTigerCacheSizeGB", "0.4",
				"--shardsvr", "--replSet", "rs0", "--port", "40000",
			},
			WaitingFor: wait.ForLog("Waiting for connections").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		exitWithError("Failed to start shard rs0: %v", err)
	}

	// 4. Start shard rs1
	rs1Container, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{"40100/tcp"},
			Networks:     []string{nw.Name},
			NetworkAliases: map[string][]string{
				nw.Name: {"tgt-rs10"},
			},
			Cmd: []string{
				"mongod", "--quiet", "--bind_ip_all", "--dbpath", "/data/db",
				"--wiredTigerCacheSizeGB", "0.4",
				"--shardsvr", "--replSet", "rs1", "--port", "40100",
			},
			WaitingFor: wait.ForLog("Waiting for connections").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		exitWithError("Failed to start shard rs1: %v", err)
	}

	// 5. Init replica sets
	if err := initReplicaSet(ctx, cfgContainer, `rs.initiate({_id:'tgt-cfg', configsvr:true, members:[{_id:0, host:'tgt-cfg0:28000', priority:2}]})`, 28000); err != nil {
		exitWithError("Failed to init config server: %v", err)
	}

	if err := initReplicaSet(ctx, rs0Container, `rs.initiate({_id:'rs0', members:[{_id:0, host:'tgt-rs00:40000', priority:2}]})`, 40000); err != nil {
		exitWithError("Failed to init shard rs0: %v", err)
	}

	if err := initReplicaSet(ctx, rs1Container, `rs.initiate({_id:'rs1', members:[{_id:0, host:'tgt-rs10:40100', priority:2}]})`, 40100); err != nil {
		exitWithError("Failed to init shard rs1: %v", err)
	}

	// 6. Start mongos (AFTER replica sets initialized)
	mongosContainer, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{"27017/tcp"},
			Networks:     []string{nw.Name},
			NetworkAliases: map[string][]string{
				nw.Name: {"tgt-mongos"},
			},
			Cmd: []string{
				"mongos", "--quiet", "--bind_ip_all",
				"--configdb", "tgt-cfg/tgt-cfg0:28000",
				"--port", "27017",
			},
			WaitingFor: wait.ForLog("Waiting for connections").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		exitWithError("Failed to start mongos: %v", err)
	}

	// 7. Wait for mongos ready
	if err := waitForMongos(ctx, mongosContainer); err != nil {
		exitWithError("Mongos not ready: %v", err)
	}

	// 8. Add shards
	if err := addShards(ctx, mongosContainer); err != nil {
		exitWithError("Failed to add shards: %v", err)
	}

	// 9. MongoDB 8.0+: transition config server
	if strings.HasPrefix(mongoVersion, "8.") {
		if err := transitionConfigServer(ctx, mongosContainer); err != nil {
			exitWithError("Failed to transition config server: %v", err)
		}
	}

	// 10. Get dynamic mongos URI
	host, err := mongosContainer.Host(ctx)
	if err != nil {
		exitWithError("Failed to get mongos host: %v", err)
	}
	mappedPort, err := mongosContainer.MappedPort(ctx, "27017/tcp")
	if err != nil {
		exitWithError("Failed to get mongos port: %v", err)
	}
	mongosURI = fmt.Sprintf("mongodb://%s:%s", host, mappedPort.Port())

	// 11. Run tests
	exitCode := m.Run()

	// 12. Cleanup
	cleanup()
	os.Exit(exitCode)
}

// initReplicaSet initializes a replica set using inline --eval and waits for primary
func initReplicaSet(ctx context.Context, container testcontainers.Container, initJS string, port int) error {
	portStr := fmt.Sprintf("%d", port)

	exitCode, _, err := container.Exec(ctx, []string{
		"mongosh", "--quiet", "--port", portStr, "--eval", initJS,
	})
	if err != nil {
		return fmt.Errorf("exec init: %w", err)
	}
	if exitCode != 0 {
		return fmt.Errorf("init exited with code %d", exitCode)
	}

	if err := waitForPrimary(ctx, container, port); err != nil {
		return fmt.Errorf("wait for primary: %w", err)
	}

	deprioritizeJS := "let c=rs.config(); c.members.forEach(m=>delete m.priority); rs.reconfig(c)"
	exitCode, _, err = container.Exec(ctx, []string{
		"mongosh", "--quiet", "--port", portStr, "--eval", deprioritizeJS,
	})
	if err != nil {
		return fmt.Errorf("exec deprioritize: %w", err)
	}
	if exitCode != 0 {
		return fmt.Errorf("deprioritize exited with code %d", exitCode)
	}

	return nil
}

func waitForPrimary(ctx context.Context, container testcontainers.Container, port int) error {
	timeout := time.After(60 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	portStr := fmt.Sprintf("%d", port)
	for {
		select {
		case <-timeout:
			return fmt.Errorf("timeout waiting for primary")
		case <-ticker.C:
			exitCode, _, err := container.Exec(ctx, []string{
				"mongosh", "--quiet", "--port", portStr,
				"--eval", "exit(db.hello().isWritablePrimary ? 0 : 1)",
			})
			if err == nil && exitCode == 0 {
				return nil
			}
		}
	}
}

func waitForMongos(ctx context.Context, container testcontainers.Container) error {
	timeout := time.After(60 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("timeout waiting for mongos")
		case <-ticker.C:
			exitCode, _, err := container.Exec(ctx, []string{
				"mongosh", "--quiet", "--port", "27017",
				"--eval", "db.adminCommand('ping')",
			})
			if err == nil && exitCode == 0 {
				return nil
			}
		}
	}
}

func addShards(ctx context.Context, container testcontainers.Container) error {
	addShardsCmd := "sh.addShard('rs0/tgt-rs00:40000'); sh.addShard('rs1/tgt-rs10:40100');"
	exitCode, _, err := container.Exec(ctx, []string{
		"mongosh", "--quiet", "--port", "27017",
		"--eval", addShardsCmd,
	})
	if err != nil {
		return fmt.Errorf("exec addShard: %w", err)
	}
	if exitCode != 0 {
		return fmt.Errorf("addShard exited with code %d", exitCode)
	}
	return nil
}

// transitionConfigServer runs transitionFromDedicatedConfigServer for MongoDB 8.0+
func transitionConfigServer(ctx context.Context, container testcontainers.Container) error {
	exitCode, _, err := container.Exec(ctx, []string{
		"mongosh", "--quiet", "--port", "27017",
		"--eval", "db.adminCommand('transitionFromDedicatedConfigServer')",
	})
	if err != nil {
		return fmt.Errorf("exec transition: %w", err)
	}
	if exitCode != 0 {
		return fmt.Errorf("transition exited with code %d", exitCode)
	}
	return nil
}

func connectToMongoDB(t *testing.T) *mongo.Client {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(mongosURI).SetServerSelectionTimeout(5 * time.Second))
	require.NoError(t, err, "MongoDB connection should succeed")

	err = client.Ping(ctx, nil)
	require.NoError(t, err, "MongoDB ping should succeed")

	return client
}

func TestCreateCollection_Idempotency(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := connectToMongoDB(t)
	defer func() { _ = client.Disconnect(ctx) }()

	cat := catalog.NewCatalog(client)

	db := testDB + "_coll"
	coll := "test_create_collection"

	defer func() { _ = client.Database(db).Drop(ctx) }()

	err := cat.CreateCollection(ctx, db, coll, &catalog.CreateCollectionOptions{})
	require.NoError(t, err, "First CreateCollection call should succeed")

	colls, err := client.Database(db).ListCollectionNames(ctx, bson.D{{"name", coll}})
	require.NoError(t, err)
	require.Len(t, colls, 1, "Collection should exist after first call")

	err = cat.CreateCollection(ctx, db, coll, &catalog.CreateCollectionOptions{})
	assert.NoError(t, err, "Second CreateCollection call should be idempotent")
}

func TestCreateView_Idempotency(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := connectToMongoDB(t)
	defer func() { _ = client.Disconnect(ctx) }()

	cat := catalog.NewCatalog(client)

	db := testDB + "_view"
	sourceColl := "test_view_source"
	view := "test_view"

	defer func() { _ = client.Database(db).Drop(ctx) }()

	err := cat.CreateCollection(ctx, db, sourceColl, &catalog.CreateCollectionOptions{})
	require.NoError(t, err, "Source collection creation should succeed")

	opts := &catalog.CreateCollectionOptions{
		ViewOn:   sourceColl,
		Pipeline: bson.A{},
	}

	err = cat.CreateCollection(ctx, db, view, opts)
	require.NoError(t, err, "First CreateView call should succeed")

	colls, err := client.Database(db).ListCollectionNames(ctx, bson.D{{"name", view}})
	require.NoError(t, err)
	require.Len(t, colls, 1, "View should exist after first call")

	err = cat.CreateCollection(ctx, db, view, opts)
	assert.NoError(t, err, "Second CreateView call should be idempotent")
}

func TestShardCollection_Idempotency(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := connectToMongoDB(t)
	defer func() { _ = client.Disconnect(ctx) }()

	cat := catalog.NewCatalog(client)

	db := testDB + "_shard"
	coll := "test_shard_collection"

	defer func() { _ = client.Database(db).Drop(ctx) }()

	err := client.Database("admin").RunCommand(ctx, bson.D{{"enableSharding", db}}).Err()
	require.NoError(t, err, "Enable sharding should succeed")

	err = cat.CreateCollection(ctx, db, coll, &catalog.CreateCollectionOptions{})
	require.NoError(t, err, "Collection creation should succeed")

	shardKey := bson.D{{"_id", 1}}

	err = cat.ShardCollection(ctx, db, coll, shardKey, false)
	require.NoError(t, err, "First ShardCollection call should succeed")

	var result bson.M
	err = client.Database("config").Collection("collections").
		FindOne(ctx, bson.D{{"_id", db + "." + coll}}).
		Decode(&result)
	require.NoError(t, err, "Collection should exist in config.collections")

	_, hasKey := result["key"]
	require.True(t, hasKey, "Collection should have shard key after first call")

	err = cat.ShardCollection(ctx, db, coll, shardKey, false)
	assert.NoError(t, err, "Second ShardCollection call should be idempotent")
}
