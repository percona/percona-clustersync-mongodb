package catalog_test

import (
	"context"
	"os"
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
	// Default to RS topology target URI. Override with TEST_TARGET_URI for sharded.
	defaultTargetURI = "mongodb://rs10:30100"
	testDB           = "pcsm_test_catalog"
)

// targetURI returns the MongoDB target URI from environment or falls back to
// the default RS topology URI.
func targetURI() string {
	if uri := os.Getenv("TEST_TARGET_URI"); uri != "" {
		return uri
	}

	return defaultTargetURI
}

// isShardedTopology returns true if the connected MongoDB is a sharded cluster.
func isShardedTopology(ctx context.Context, client *mongo.Client) bool {
	var result bson.M

	err := client.Database("admin").RunCommand(ctx, bson.D{{"hello", 1}}).Decode(&result)
	if err != nil {
		return false
	}

	msg, _ := result["msg"].(string)

	return msg == "isdbgrid"
}

// connectToMongoDB establishes a connection to MongoDB and returns the client.
// If MongoDB is unavailable, the test is skipped.
func connectToMongoDB(t *testing.T) *mongo.Client {
	t.Helper()

	uri := targetURI()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(5 * time.Second))
	if err != nil {
		t.Skip("MongoDB not available:", err)
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		_ = client.Disconnect(t.Context())
		t.Skip("MongoDB not reachable:", err)
	}

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

	if !isShardedTopology(ctx, client) {
		t.Skip("ShardCollection test requires sharded topology (set TEST_TARGET_URI to mongos)")
	}

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
