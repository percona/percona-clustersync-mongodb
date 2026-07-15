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

// sourceURI points at a single-node replica-set mongod (the clone source). A
// replica set is required because the behavior under test depends on the oplog,
// which standalone deployments do not have. targetURI points at a standalone
// mongod used as the clone target.
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
	// oplogHole is how long the sleepBetweenInsertOpTimeGenerationAndLogOp
	// failpoint holds each oplog hole open: an in-flight insert reserves its
	// oplog timestamp, then sleeps this long before the entry becomes visible.
	// It must be comfortably larger than the full clone-capture-plus-scan window
	// (Clone.Start -> run() startTS capture -> collectSizeMap -> collection scan).
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
// PCSM-241. It runs the REAL clone (clone.Clone), so startTS is captured by
// whatever clone.go run() actually calls. The test therefore FAILS today
// (clone.go uses ping, mdb.ClusterTime) and PASSES once clone.go switches to
// appendOplogNote (mdb.AdvanceClusterTime).
//
// PCSM's contract: every source write must be captured by the clone scan OR by
// the change stream that replication opens at startAtOperationTime = startTS
// (repl.go). startAtOperationTime is INCLUSIVE, so any write whose oplog
// timestamp is STRICTLY LESS THAN startTS is invisible to the stream forever
// and MUST have been seen by the clone scan, or it is silently lost.
//
// The race: inserting a doc reserves its oplog timestamp (advancing the logical
// clock) BEFORE the entry becomes visible ("oplog hole"). ping reports that
// advanced clusterTime without waiting for the in-flight write to commit, so
// startTS can land past a write the clone scan cannot yet see. appendOplogNote
// instead performs a durable oplog write that cannot be acknowledged until the
// no-holes point passes all in-flight writes, so by the time startTS is
// returned they are committed and the clone scan copies them.
//
// Two writes are held in the hole so the loss is deterministic:
//
//   - lostDoc reserves the earlier timestamp T_lost,
//   - boundaryDoc reserves a later timestamp T_boundary > T_lost.
//
// startTS >= T_boundary > T_lost STRICTLY, so lostDoc is strictly before startTS
// and thus never streamed. If the clone scan (running inside the hole with ping)
// also misses it, it is lost. A single write would sit on the inclusive startTS
// boundary and survive, which is why two are needed.
//
// The sleepBetweenInsertOpTimeGenerationAndLogOp failpoint holds the holes open
// long enough to cover the whole clone capture+scan window, removing flakiness.
//
// Not parallel: it drives the server-global failpoint on the shared source
// container and must not run concurrently with other tests.
//
//nolint:paralleltest // global failpoint forbids parallel execution
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

	// Baseline document so the collection exists and gets cloned.
	_, err := coll.InsertOne(ctx, bson.D{{"_id", "base"}})
	require.NoError(t, err, "baseline insert")

	// Open the oplog hole: each insert reserves its timestamp, then sleeps
	// waitForMillis before the entry becomes visible.
	setFailpoint(t, source, inflightFailpoint, "alwaysOn", bson.D{
		{"waitForMillis", oplogHole.Milliseconds()},
	})
	failpointOff := func() { setFailpoint(t, source, inflightFailpoint, "off", nil) }

	// insertInflight fires an insert on a dedicated goroutine (a maxTimeMS insert
	// would be rolled back instead of committing) and, once its oplog slot is
	// reserved, returns a channel that reports the insert's completion.
	insertInflight := func(id string) chan error {
		before, e := mdb.ClusterTime(ctx, source)
		require.NoError(t, e, "cluster time before %s", id)

		done := make(chan error, 1)
		go func() {
			_, insErr := coll.InsertOne(context.Background(), bson.D{{"_id", id}})
			done <- insErr
		}()

		// Wait until the logical clock advances past `before`, which happens the
		// moment this insert reserves its oplog timestamp. This both confirms the
		// reservation and orders the two reservations deterministically.
		require.Eventuallyf(t, func() bool {
			ts, te := mdb.ClusterTime(ctx, source)

			return te == nil && ts.After(before)
		}, oplogHole, 20*time.Millisecond, "cluster time should advance as %s reserves its oplog slot", id)

		return done
	}

	// lostDoc reserves the earlier timestamp; boundaryDoc reserves a later one.
	lostInserted := insertInflight("lostDoc")
	boundaryInserted := insertInflight("boundaryDoc")

	// Run the REAL clone. clone.run() captures startTS (via clone.go's actual
	// code path) then scans the collection. With ping, the scan runs inside the
	// hole and misses lostDoc; with appendOplogNote, capture blocks until the
	// holes fill so the scan copies lostDoc.
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

	// Open the change stream at startAtOperationTime = startTS, exactly as
	// replication does (repl.go SetStartAtOperationTime). Events with
	// ts < startTS are never delivered; startTS itself is inclusive.
	startTS := status.StartTS
	stream, err := coll.Watch(ctx, mongo.Pipeline{},
		options.ChangeStream().SetStartAtOperationTime(&startTS))
	require.NoError(t, err, "open change stream at startTS")
	defer func() { _ = stream.Close(ctx) }()

	// Ensure the holes are closed and both in-flight inserts have committed.
	failpointOff()
	require.NoError(t, <-lostInserted, "background insert of lostDoc should succeed")
	require.NoError(t, <-boundaryInserted, "background insert of boundaryDoc should succeed")

	// Sentinel strictly AFTER startTS. Draining until it arrives guarantees we
	// have observed every event replication would apply at or after startTS, so
	// any absence of lostDoc is a real loss, not a timing artifact.
	_, err = coll.InsertOne(ctx, bson.D{{"_id", "sentinel"}})
	require.NoError(t, err, "sentinel insert")

	streamed := drainUntil(t, ctx, stream, "sentinel")
	require.Contains(t, streamed, "sentinel", "sentinel proves the change stream is live")

	// What the target ends up with: cloned rows + rows replication would apply.
	cloned := scanIDs(t, ctx, target.Database(dbName).Collection("c"))
	replicated := union(cloned, streamed)

	// The gate: lostDoc must survive. With ping it is neither cloned (scan ran
	// inside the hole) nor streamed (its oplog ts < startTS) -> FAIL. With
	// appendOplogNote the clone scan copies it -> PASS.
	require.Containsf(t, replicated, "lostDoc",
		"DATA LOSS (PCSM-241): in-flight write lostDoc is neither cloned to the target "+
			"nor replicated (its oplog ts is < startTS %v). clone.go must capture startTS "+
			"with appendOplogNote (mdb.AdvanceClusterTime), not ping (mdb.ClusterTime)", startTS)
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
