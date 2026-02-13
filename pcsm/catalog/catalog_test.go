//go:build integration

package catalog_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/compose"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
)

const testDB = "pcsm_test_catalog"

var mongosURI string

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Find project root by walking up to go.mod
	projectRoot, err := findProjectRoot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to find project root: %v\n", err)
		os.Exit(1)
	}

	composePath := filepath.Join(projectRoot, "hack", "sh", "compose.yml")

	// Create compose stack
	mongoVersion := os.Getenv("MONGO_VERSION")
	if mongoVersion == "" {
		mongoVersion = "8.0"
	}

	stack, err := compose.NewDockerComposeWith(
		compose.WithStackFiles(composePath),
		compose.StackIdentifier("catalog-integration-test"),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create compose stack: %v\n", err)
		os.Exit(1)
	}

	stack = stack.WithEnv(map[string]string{"MONGO_VERSION": mongoVersion}).(*compose.DockerCompose)

	// Phase 1: Start config server and shards
	err = stack.Up(ctx, compose.RunServices("tgt-cfg0", "tgt-rs00", "tgt-rs10"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start config and shards: %v\n", err)
		os.Exit(1)
	}

	// Initialize replica sets
	if err := initReplicaSet(ctx, stack, "tgt-cfg0", "/cfg/tgt/cfg.js", 28000); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to init config server: %v\n", err)
		_ = stack.Down(ctx, compose.RemoveOrphans(true), compose.RemoveVolumes(true))
		os.Exit(1)
	}

	if err := initReplicaSet(ctx, stack, "tgt-rs00", "/cfg/tgt/rs0.js", 40000); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to init shard rs0: %v\n", err)
		_ = stack.Down(ctx, compose.RemoveOrphans(true), compose.RemoveVolumes(true))
		os.Exit(1)
	}

	if err := initReplicaSet(ctx, stack, "tgt-rs10", "/cfg/tgt/rs1.js", 40100); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to init shard rs1: %v\n", err)
		_ = stack.Down(ctx, compose.RemoveOrphans(true), compose.RemoveVolumes(true))
		os.Exit(1)
	}

	// Phase 2: Start mongos
	err = stack.Up(ctx, compose.RunServices("tgt-mongos"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start mongos: %v\n", err)
		_ = stack.Down(ctx, compose.RemoveOrphans(true), compose.RemoveVolumes(true))
		os.Exit(1)
	}

	// Wait for mongos to be ready
	if err := waitForMongos(ctx, stack); err != nil {
		fmt.Fprintf(os.Stderr, "Mongos not ready: %v\n", err)
		_ = stack.Down(ctx, compose.RemoveOrphans(true), compose.RemoveVolumes(true))
		os.Exit(1)
	}

	// Add shards to cluster
	if err := addShards(ctx, stack); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to add shards: %v\n", err)
		_ = stack.Down(ctx, compose.RemoveOrphans(true), compose.RemoveVolumes(true))
		os.Exit(1)
	}

	// MongoDB 8.0+: transition from dedicated config server
	if strings.HasPrefix(mongoVersion, "8.") {
		if err := transitionConfigServer(ctx, stack); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to transition config server: %v\n", err)
			_ = stack.Down(ctx, compose.RemoveOrphans(true), compose.RemoveVolumes(true))
			os.Exit(1)
		}
	}

	// Set mongos URI for tests
	mongosURI = "mongodb://tgt-mongos:29017"

	// Run tests
	exitCode := m.Run()

	// Teardown
	_ = stack.Down(ctx, compose.RemoveOrphans(true), compose.RemoveVolumes(true))

	os.Exit(exitCode)
}

// findProjectRoot walks up directories until finding go.mod
func findProjectRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found")
		}
		dir = parent
	}
}

// initReplicaSet initializes a replica set and waits for primary
func initReplicaSet(ctx context.Context, stack *compose.DockerCompose, containerName, initScript string, port int) error {
	container, err := stack.ServiceContainer(ctx, containerName)
	if err != nil {
		return fmt.Errorf("get container %s: %w", containerName, err)
	}

	// Run rs.initiate script
	_, _, err = container.Exec(ctx, []string{"mongosh", "--quiet", "--port", fmt.Sprintf("%d", port), initScript})
	if err != nil {
		return fmt.Errorf("exec init script: %w", err)
	}

	// Wait for primary
	if err := waitForPrimary(ctx, container, port); err != nil {
		return fmt.Errorf("wait for primary: %w", err)
	}

	// Run deprioritize script
	_, _, err = container.Exec(ctx, []string{"mongosh", "--quiet", "--port", fmt.Sprintf("%d", port), "/cfg/scripts/deprioritize.js"})
	if err != nil {
		return fmt.Errorf("exec deprioritize: %w", err)
	}

	return nil
}

// waitForPrimary polls until replica set has a primary
func waitForPrimary(ctx context.Context, container *testcontainers.DockerContainer, port int) error {
	timeout := time.After(60 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("timeout waiting for primary")
		case <-ticker.C:
			exitCode, _, err := container.Exec(ctx, []string{
				"mongosh", "--quiet", "--port", fmt.Sprintf("%d", port),
				"--eval", "exit(db.hello().isWritablePrimary ? 0 : 1)",
			})
			if err == nil && exitCode == 0 {
				return nil
			}
		}
	}
}

// waitForMongos polls until mongos is ready
func waitForMongos(ctx context.Context, stack *compose.DockerCompose) error {
	container, err := stack.ServiceContainer(ctx, "tgt-mongos")
	if err != nil {
		return fmt.Errorf("get mongos container: %w", err)
	}

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

// addShards adds shards to the cluster via mongos
func addShards(ctx context.Context, stack *compose.DockerCompose) error {
	container, err := stack.ServiceContainer(ctx, "tgt-mongos")
	if err != nil {
		return fmt.Errorf("get mongos container: %w", err)
	}

	addShardsCmd := "sh.addShard('rs0/tgt-rs00:40000'); sh.addShard('rs1/tgt-rs10:40100');"
	_, _, err = container.Exec(ctx, []string{
		"mongosh", "--quiet", "--port", "27017",
		"--eval", addShardsCmd,
	})
	if err != nil {
		return fmt.Errorf("exec addShard: %w", err)
	}

	return nil
}

// transitionConfigServer runs transitionFromDedicatedConfigServer for MongoDB 8.0+
func transitionConfigServer(ctx context.Context, stack *compose.DockerCompose) error {
	container, err := stack.ServiceContainer(ctx, "tgt-mongos")
	if err != nil {
		return fmt.Errorf("get mongos container: %w", err)
	}

	_, _, err = container.Exec(ctx, []string{
		"mongosh", "--quiet", "--port", "27017",
		"--eval", "db.adminCommand('transitionFromDedicatedConfigServer')",
	})
	if err != nil {
		return fmt.Errorf("exec transition: %w", err)
	}

	return nil
}

// connectToMongoDB establishes a connection to MongoDB
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
