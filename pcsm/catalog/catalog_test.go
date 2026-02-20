//go:build integration

package catalog_test

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

	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
)

const testDB = "pcsm_test_catalog"

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
				"--replSet", "rs0", "--port", "27017",
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

	exitCode, _, err := mongod.Exec(ctx, []string{
		"mongosh", "--quiet", "--eval",
		"rs.initiate({_id:'rs0', members:[{_id:0, host:'localhost:27017'}]})",
	})
	if err != nil || exitCode != 0 {
		exitWithError("Failed to init replica set: err=%v exit=%d", err, exitCode)
	}

	if err := waitForPrimary(ctx, mongod); err != nil {
		exitWithError("Failed waiting for primary: %v", err)
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

	exitCode = m.Run()

	cleanup()
	os.Exit(exitCode)
}

func waitForPrimary(ctx context.Context, container testcontainers.Container) error {
	timeout := time.After(60 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("timeout waiting for primary")
		case <-ticker.C:
			exitCode, _, err := container.Exec(ctx, []string{
				"mongosh", "--quiet",
				"--eval", "exit(db.hello().isWritablePrimary ? 0 : 1)",
			})
			if err == nil && exitCode == 0 {
				return nil
			}
		}
	}
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

	require.Contains(t, cat.Databases, db, "Database should be tracked in catalog after first call")
	require.Contains(t, cat.Databases[db].Collections, coll,
		"Collection should be tracked in catalog after first call")

	err = cat.CreateCollection(ctx, db, coll, &catalog.CreateCollectionOptions{})
	assert.NoError(t, err, "Second CreateCollection call should be idempotent")

	assert.Contains(t, cat.Databases[db].Collections, coll,
		"Collection should still be tracked in catalog after idempotent call")
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
