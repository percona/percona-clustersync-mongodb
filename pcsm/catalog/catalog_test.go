package catalog_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
)

const (
	testTargetURI = "mongodb://tgt-mongos:29017"
	testDB        = "pcsm_test_catalog"
)

// connectToMongoDB establishes a connection to MongoDB and returns the client.
// If MongoDB is unavailable, the test is skipped.
func connectToMongoDB(t *testing.T) *mongo.Client {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(testTargetURI).SetServerSelectionTimeout(2 * time.Second))
	if err != nil {
		t.Skip("MongoDB not available:", err)
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		_ = client.Disconnect(context.Background())
		t.Skip("MongoDB not reachable:", err)
	}

	return client
}

func TestCreateCollection_Idempotency(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client := connectToMongoDB(t)
	defer func() { _ = client.Disconnect(ctx) }()

	cat := catalog.NewCatalog(client)

	testColl := "test_create_collection"

	// Clean up before and after
	defer func() { _ = client.Database(testDB).Drop(ctx) }()

	// First call - should succeed
	err := cat.CreateCollection(ctx, testDB, testColl, &catalog.CreateCollectionOptions{})
	require.NoError(t, err, "First CreateCollection call should succeed")

	// Verify collection exists
	colls, err := client.Database(testDB).ListCollectionNames(ctx, bson.D{{"name", testColl}})
	require.NoError(t, err)
	require.Len(t, colls, 1, "Collection should exist after first call")

	// Second call - should be idempotent (no error)
	err = cat.CreateCollection(ctx, testDB, testColl, &catalog.CreateCollectionOptions{})
	assert.NoError(t, err, "Second CreateCollection call should be idempotent")
}

func TestCreateView_Idempotency(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client := connectToMongoDB(t)
	defer func() { _ = client.Disconnect(ctx) }()

	cat := catalog.NewCatalog(client)

	sourceColl := "test_view_source"
	testView := "test_view"

	// Clean up before and after
	defer func() { _ = client.Database(testDB).Drop(ctx) }()

	// Create source collection first
	err := cat.CreateCollection(ctx, testDB, sourceColl, &catalog.CreateCollectionOptions{})
	require.NoError(t, err, "Source collection creation should succeed")

	// Prepare view options
	opts := &catalog.CreateCollectionOptions{
		ViewOn:   sourceColl,
		Pipeline: bson.A{},
	}

	// First call - should succeed
	err = cat.CreateCollection(ctx, testDB, testView, opts)
	require.NoError(t, err, "First CreateView call should succeed")

	// Verify view exists
	colls, err := client.Database(testDB).ListCollectionNames(ctx, bson.D{{"name", testView}})
	require.NoError(t, err)
	require.Len(t, colls, 1, "View should exist after first call")

	// Second call - should be idempotent (no error)
	err = cat.CreateCollection(ctx, testDB, testView, opts)
	assert.NoError(t, err, "Second CreateView call should be idempotent")
}

func TestShardCollection_Idempotency(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client := connectToMongoDB(t)
	defer func() { _ = client.Disconnect(ctx) }()

	cat := catalog.NewCatalog(client)

	testColl := "test_shard_collection"

	// Clean up before and after
	defer func() { _ = client.Database(testDB).Drop(ctx) }()

	// Enable sharding on the database first
	err := client.Database("admin").RunCommand(ctx, bson.D{{"enableSharding", testDB}}).Err()
	require.NoError(t, err, "Enable sharding should succeed")

	// First create the collection
	err = cat.CreateCollection(ctx, testDB, testColl, &catalog.CreateCollectionOptions{})
	require.NoError(t, err, "Collection creation should succeed")

	// Define shard key
	shardKey := bson.D{{"_id", 1}}

	// First call - should succeed
	err = cat.ShardCollection(ctx, testDB, testColl, shardKey, false)
	require.NoError(t, err, "First ShardCollection call should succeed")

	// Verify collection is sharded by checking config.collections
	var result bson.M
	err = client.Database("config").Collection("collections").
		FindOne(ctx, bson.D{{"_id", testDB + "." + testColl}}).
		Decode(&result)
	require.NoError(t, err, "Collection should exist in config.collections")

	// Check if the collection has a key field (indicating it's sharded)
	_, hasKey := result["key"]
	require.True(t, hasKey, "Collection should have shard key after first call")

	// Second call - should be idempotent (no error)
	err = cat.ShardCollection(ctx, testDB, testColl, shardKey, false)
	assert.NoError(t, err, "Second ShardCollection call should be idempotent")
}
