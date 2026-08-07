//go:build integration

package clone_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/pcsm/clone"
	"github.com/percona/percona-clustersync-mongodb/sel"
)

// sourceURI is a single-node replica set (needed for the oplog); targetURI is a
// standalone used as the clone target. Both are set by TestMain.
var (
	sourceURI string //nolint:gochecknoglobals // test fixture set by TestMain
	targetURI string //nolint:gochecknoglobals // test fixture set by TestMain
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	version := os.Getenv("MONGO_VERSION")
	if version == "" {
		version = "8.0"
	}
	image := "percona/percona-server-mongodb:" + version

	// Source: single-node replica set with test commands enabled so the
	// sleepBetweenInsertOpTimeGenerationAndLogOp failpoint is available.
	source, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{"27017/tcp"},
			Cmd: []string{
				"mongod", "--quiet", "--bind_ip_all", "--dbpath", "/data/db",
				"--wiredTigerCacheSizeGB", "0.5",
				"--replSet", "rs0", "--port", "27017",
				"--setParameter", "enableTestCommands=1",
			},
			WaitingFor: wait.ForLog("Waiting for connections").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "start source mongod container: %v\n", err)
		os.Exit(1)
	}

	// Target: standalone mongod. The clone copies into the same namespace as the
	// source, so the target must be a separate deployment.
	target, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{"27017/tcp"},
			Cmd: []string{
				"mongod", "--quiet", "--bind_ip_all", "--dbpath", "/data/db",
				"--wiredTigerCacheSizeGB", "0.5", "--port", "27017",
			},
			WaitingFor: wait.ForLog("Waiting for connections").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		_ = source.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "start target mongod container: %v\n", err)
		os.Exit(1)
	}

	cleanup := func() {
		_ = target.Terminate(ctx)
		_ = source.Terminate(ctx)
	}

	fail := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
		cleanup()
		os.Exit(1)
	}

	exitCode, _, err := source.Exec(ctx, []string{
		"mongosh", "--quiet", "--eval",
		"rs.initiate({_id:'rs0', members:[{_id:0, host:'localhost:27017'}]})",
	})
	if err != nil || exitCode != 0 {
		fail("init replica set: err=%v exit=%d", err, exitCode)
	}

	if err := waitForPrimary(ctx, source); err != nil {
		fail("wait for primary: %v", err)
	}

	sourceURI, err = containerURI(ctx, source)
	if err != nil {
		fail("source URI: %v", err)
	}
	targetURI, err = containerURI(ctx, target)
	if err != nil {
		fail("target URI: %v", err)
	}

	code := m.Run()
	cleanup()
	os.Exit(code)
}

func containerURI(ctx context.Context, container testcontainers.Container) (string, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return "", err
	}
	port, err := container.MappedPort(ctx, "27017/tcp")
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("mongodb://%s:%s/?directConnection=true", host, port.Port()), nil
}

func waitForPrimary(ctx context.Context, container testcontainers.Container) error {
	timeout := time.After(60 * time.Second)
	ticker := time.NewTicker(time.Second)
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

func connect(t *testing.T, uri string) *mongo.Client {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(5 * time.Second))
	require.NoError(t, err, "MongoDB connection should succeed")
	require.NoError(t, client.Ping(ctx, nil), "MongoDB ping should succeed")

	return client
}

const (
	// oplogHole is how long the failpoint keeps each in-flight insert invisible.
	// It must exceed the clone's startTS-capture-plus-scan window.
	oplogHole = 10 * time.Second

	inflightFailpoint = "sleepBetweenInsertOpTimeGenerationAndLogOp"
)

// setFailpoint toggles the named failpoint. data may be nil to turn it off.
func setFailpoint(t *testing.T, client *mongo.Client, name, mode string, data bson.D) {
	t.Helper()

	cmd := bson.D{{"configureFailPoint", name}, {"mode", mode}}
	if data != nil {
		cmd = append(cmd, bson.E{Key: "data", Value: data})
	}

	err := client.Database("admin").RunCommand(t.Context(), cmd).Err()
	require.NoError(t, err, "configureFailPoint %s=%s", name, mode)
}

// TestClone_DoesNotLoseInflightWrite is the clone-path regression gate for
// PCSM-241. It runs the real clone, so startTS is captured by clone.go itself:
// the test FAILS with ping (mdb.ClusterTime) and PASSES with appendOplogNote
// (mdb.AdvanceClusterTime).
//
// A write must be seen by the clone scan or by the change stream that starts at
// startTS; startTS is inclusive, so any write with oplog ts < startTS that the
// scan misses is lost. An in-flight insert reserves its oplog ts (advancing the
// clock) before the entry is visible. ping returns that advanced clock without
// waiting, so startTS can land past a write the scan cannot yet see;
// appendOplogNote blocks until every in-flight write is durable, so the scan
// sees them.
//
// Two writes make the loss deterministic: lostDoc reserves the earlier ts,
// boundaryDoc a later one, so startTS lands strictly after lostDoc. A single
// write would sit on the inclusive startTS boundary and survive.
//
//nolint:paralleltest // drives a server-global failpoint; must not run in parallel
func TestClone_DoesNotLoseInflightWrite(t *testing.T) {
	ctx := t.Context()

	source := connect(t, sourceURI)
	defer func() { _ = source.Disconnect(ctx) }()

	target := connect(t, targetURI)
	defer func() { _ = target.Disconnect(ctx) }()

	dbName := "pcsm241_" + bson.NewObjectID().Hex()
	coll := source.Database(dbName).Collection("c")
	defer func() { _ = source.Database(dbName).Drop(ctx) }()
	defer func() { _ = target.Database(dbName).Drop(ctx) }()

	_, err := coll.InsertOne(ctx, bson.D{{"_id", "base"}})
	require.NoError(t, err, "baseline insert")

	// Open the oplog hole: each insert reserves its timestamp, then sleeps before
	// the entry becomes visible.
	setFailpoint(t, source, inflightFailpoint, "alwaysOn", bson.D{
		{"waitForMillis", oplogHole.Milliseconds()},
	})
	failpointOff := func() { setFailpoint(t, source, inflightFailpoint, "off", nil) }

	// insertInflight starts an insert in the background (a maxTimeMS insert would
	// roll back instead of committing) and returns once its oplog slot is
	// reserved, which is when the cluster clock advances past `before`.
	insertInflight := func(id string) chan error {
		before, e := mdb.ClusterTime(ctx, source)
		require.NoError(t, e, "cluster time before %s", id)

		done := make(chan error, 1)
		go func() {
			_, insErr := coll.InsertOne(context.Background(), bson.D{{"_id", id}})
			done <- insErr
		}()

		require.Eventuallyf(t, func() bool {
			ts, te := mdb.ClusterTime(ctx, source)

			return te == nil && ts.After(before)
		}, oplogHole, 20*time.Millisecond, "%s should reserve its oplog slot", id)

		return done
	}

	// lostDoc reserves the earlier timestamp; boundaryDoc pushes startTS past it.
	lostInserted := insertInflight("lostDoc")
	boundaryInserted := insertInflight("boundaryDoc")

	// Run the real clone: clone.go captures startTS, then scans the collection.
	sourceVer, err := mdb.Version(ctx, source)
	require.NoError(t, err, "source server version")

	cat := catalog.NewCatalog(target, sourceVer)
	nsFilter := sel.MakeFilter([]string{dbName + ".*"}, nil)
	c := clone.NewClone(source, target, cat, nsFilter, &clone.Options{})

	require.NoError(t, c.Start(ctx), "clone start")

	select {
	case <-c.Done():
	case <-time.After(oplogHole + 30*time.Second):
		t.Fatal("clone did not finish in time")
	}

	status := c.Status()
	require.NoError(t, status.Err, "clone should complete without error")
	require.False(t, status.StartTS.IsZero(), "clone must record a startTS")

	// Open the change stream at startTS, exactly as replication does (repl.go).
	startTS := status.StartTS
	stream, err := coll.Watch(ctx, mongo.Pipeline{},
		options.ChangeStream().SetStartAtOperationTime(&startTS))
	require.NoError(t, err, "open change stream at startTS")
	defer func() { _ = stream.Close(ctx) }()

	// Close the holes and let both in-flight inserts commit.
	failpointOff()
	require.NoError(t, <-lostInserted, "insert lostDoc")
	require.NoError(t, <-boundaryInserted, "insert boundaryDoc")

	// Sentinel is strictly after startTS; draining until it arrives means we have
	// seen every event replication would apply, so a missing lostDoc is real loss.
	_, err = coll.InsertOne(ctx, bson.D{{"_id", "sentinel"}})
	require.NoError(t, err, "sentinel insert")

	streamed := drainUntil(t, ctx, stream, "sentinel")

	// What the target ends up with: cloned rows plus rows replication would apply.
	cloned := scanIDs(t, ctx, target.Database(dbName).Collection("c"))
	replicated := union(cloned, streamed)

	require.Containsf(t, replicated, "lostDoc",
		"DATA LOSS (PCSM-241): in-flight write lostDoc was neither cloned nor replicated "+
			"(its oplog ts < startTS %v); clone.go must capture startTS with appendOplogNote, "+
			"not ping", startTS)
}

// scanIDs returns the set of _id string values currently visible in coll,
// modeling the clone's collection scan (committed reads only).
func scanIDs(t *testing.T, ctx context.Context, coll *mongo.Collection) map[string]struct{} {
	t.Helper()

	cur, err := coll.Find(ctx, bson.D{})
	require.NoError(t, err, "clone scan Find")
	defer func() { _ = cur.Close(ctx) }()

	ids := map[string]struct{}{}
	for cur.Next(ctx) {
		var doc struct {
			ID string `bson:"_id"`
		}
		require.NoError(t, cur.Decode(&doc))
		ids[doc.ID] = struct{}{}
	}
	require.NoError(t, cur.Err())

	return ids
}

// drainUntil reads change-stream events until it observes the sentinel _id (or
// times out), returning the set of _id values delivered.
func drainUntil(t *testing.T, ctx context.Context, stream *mongo.ChangeStream, sentinel string) map[string]struct{} {
	t.Helper()

	ids := map[string]struct{}{}

	deadline, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	for stream.Next(deadline) {
		var ev struct {
			DocumentKey struct {
				ID string `bson:"_id"`
			} `bson:"documentKey"`
		}
		require.NoError(t, stream.Decode(&ev))
		ids[ev.DocumentKey.ID] = struct{}{}
		if ev.DocumentKey.ID == sentinel {
			return ids
		}
	}
	require.NoErrorf(t, stream.Err(), "change stream error before sentinel arrived")
	require.Contains(t, ids, sentinel, "timed out waiting for sentinel event")

	return ids
}

func union(a, b map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		out[k] = struct{}{}
	}
	for k := range b {
		out[k] = struct{}{}
	}

	return out
}
