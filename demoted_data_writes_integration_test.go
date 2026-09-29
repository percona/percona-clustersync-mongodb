//go:build integration

package main //nolint:testpackage // Exercise demotion against a real source, target and pipeline.

import (
	"context"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/ha"
	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm"
)

const (
	demotedTestDB     = "demoted_fencing_db"
	demotedTestColl   = "docs"
	cloneWaitInterval = 200 * time.Millisecond
)

// The source must be a replica set to serve change streams; the target is a
// separate standalone because replication copies into the same namespace.
//
//nolint:gochecknoglobals // shared testcontainers for the demotion fencing suite
var (
	demotedSourceURI string
	demotedTargetURI string
	errDemotedMongo  error
	demotedMongoOnce sync.Once
)

// demotedMongo starts the suite's source and target containers once. No
// TestMain is possible in this package (cli_test.go already defines one), so
// termination is left to the testcontainers reaper.
func demotedMongo(t *testing.T) (string, string) {
	t.Helper()

	demotedMongoOnce.Do(func() {
		demotedSourceURI, demotedTargetURI, errDemotedMongo = startDemotedMongo(context.Background())
	})
	require.NoError(t, errDemotedMongo)

	return demotedSourceURI, demotedTargetURI
}

func startDemotedMongo(ctx context.Context) (string, string, error) {
	version := os.Getenv("MONGO_VERSION")
	if version == "" {
		version = "8.0.29-13"
	}
	image := "percona/percona-server-mongodb:" + version

	base := []string{
		"mongod", "--quiet", "--bind_ip_all", "--dbpath", "/data/db",
		"--wiredTigerCacheSizeGB", "0.5", "--port", "27017",
	}

	newMongod := func(extra ...string) (testcontainers.Container, error) {
		cmd := make([]string, 0, len(base)+len(extra))
		cmd = append(cmd, base...)
		cmd = append(cmd, extra...)

		req := testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{"27017/tcp"},
			Cmd:          cmd,
			WaitingFor: wait.ForLog("Waiting for connections").
				WithStartupTimeout(mongodStartupTimeout),
		}

		return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
	}

	source, err := newMongod("--replSet", "rs0")
	if err != nil {
		return "", "", errors.Wrap(err, "start source mongod container")
	}

	target, err := newMongod()
	if err != nil {
		_ = source.Terminate(ctx)

		return "", "", errors.Wrap(err, "start target mongod container")
	}

	exitCode, _, err := source.Exec(ctx, []string{
		"mongosh", "--quiet", "--eval",
		"rs.initiate({_id:'rs0', members:[{_id:0, host:'localhost:27017'}]})",
	})
	if err != nil {
		return "", "", errors.Wrap(err, "init source replica set")
	}
	// Wrapf returns nil for a nil cause, so a nonzero exit needs its own error
	// or the caller gets empty URIs and no failure.
	if exitCode != 0 {
		return "", "", errors.Errorf("init source replica set: mongosh exited %d", exitCode)
	}

	err = waitForDemotedSourcePrimary(ctx, source)
	if err != nil {
		return "", "", err
	}

	sourceURI, err := demotedContainerURI(ctx, source)
	if err != nil {
		return "", "", err
	}

	targetURI, err := demotedContainerURI(ctx, target)
	if err != nil {
		return "", "", err
	}

	return sourceURI, targetURI, nil
}

// waitForDemotedSourcePrimary blocks until the source replica set has elected
// itself, so the first write does not race the election.
func waitForDemotedSourcePrimary(ctx context.Context, container testcontainers.Container) error {
	timeout := time.After(mongodStartupTimeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return errors.New("timeout waiting for the source primary")
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

func demotedContainerURI(ctx context.Context, container testcontainers.Container) (string, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return "", errors.Wrap(err, "container host")
	}

	port, err := container.MappedPort(ctx, "27017/tcp")
	if err != nil {
		return "", errors.Wrap(err, "container mapped port")
	}

	return "mongodb://" + net.JoinHostPort(host, port.Port()) + "/?directConnection=true", nil
}

// bulkWriteGate blocks target bulk writes so a write can be held in flight
// across a demotion and released afterwards.
type bulkWriteGate struct {
	armed   chan struct{}
	release chan struct{}
	once    sync.Once
	arm     sync.Once
}

func newBulkWriteGate() *bulkWriteGate {
	return &bulkWriteGate{armed: make(chan struct{}), release: make(chan struct{})}
}

// monitor holds the pipeline's target writes until releaseAll is called.
// Only writes issued after enable() are held, so the clone is unaffected.
func (g *bulkWriteGate) monitor(ctx context.Context, enabled *bool, mu *sync.Mutex) *event.CommandMonitor {
	return &event.CommandMonitor{
		Started: func(_ context.Context, cmd *event.CommandStartedEvent) {
			switch cmd.CommandName {
			case "bulkWrite":
				// Client-level, used against 8.0. It runs on admin with the
				// namespace inside the command, so there is no database to
				// match on here.
			case "insert", "update", "delete":
				// Collection-level, used below 8.0. Checkpoints are written to
				// the target too, and holding one deadlocks DoCheckpoint.
				if cmd.DatabaseName != demotedTestDB {
					return
				}
			default:
				return
			}

			mu.Lock()
			on := *enabled
			mu.Unlock()

			if !on {
				return
			}

			g.arm.Do(func() { close(g.armed) })

			select {
			case <-g.release:
			case <-ctx.Done():
			}
		},
	}
}

func (g *bulkWriteGate) releaseAll() {
	g.once.Do(func() { close(g.release) })
}

// TestDemotedActiveMustNotWriteTargetData pins the contract that an instance
// which has lost the lease cannot write user data to the target.
//
// Term fencing is applied only to checkpoint documents (see DoCheckpoint).
// Clone, replication bulk writes and catalog operations never consult the
// term, and onDemote's pause is explicitly best-effort, so a write already in
// flight when the lease is lost still lands. The new ACTIVE will not re-apply
// what it has already applied, so the divergence is permanent and both
// lag and finalization still report success over it.
//
//nolint:paralleltest // Drives one pipeline against a shared source and target.
func TestDemotedActiveMustNotWriteTargetData(t *testing.T) {
	sourceURI, targetURI := demotedMongo(t)

	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()

	source, err := mongo.Connect(options.Client().
		ApplyURI(sourceURI).SetServerSelectionTimeout(30 * time.Second))
	require.NoError(t, err)
	defer func() { _ = source.Disconnect(context.Background()) }()

	// The gate is installed up front but only holds writes once enabled, so
	// the clone and the catalog setup run unimpeded.
	var (
		mu      sync.Mutex
		enabled bool
	)
	gate := newBulkWriteGate()
	defer gate.releaseAll()

	target, err := mongo.Connect(options.Client().
		ApplyURI(targetURI).
		SetServerSelectionTimeout(30 * time.Second).
		SetRetryWrites(false).
		SetMonitor(gate.monitor(ctx, &enabled, &mu)))
	require.NoError(t, err)
	defer func() { _ = target.Disconnect(context.Background()) }()

	require.NoError(t, source.Database(demotedTestDB).Drop(ctx))
	require.NoError(t, target.Database(demotedTestDB).Drop(ctx))
	require.NoError(t, target.Database(config.PCSMDatabase).Drop(ctx))

	sourceColl := source.Database(demotedTestDB).Collection(demotedTestColl)
	_, err = sourceColl.InsertOne(ctx, bson.D{{"_id", "cloned"}})
	require.NoError(t, err)

	sourceVer, err := mdb.Version(ctx, source)
	require.NoError(t, err)

	// Given: this instance is the ACTIVE of term 1 and is replicating.
	pipeline := pcsm.New(ctx, source, target, sourceVer, false, false)
	membership := &ha.Membership{}
	membership.SetRole(ha.RoleActive, 1)
	s := &server{
		cfg:           &config.Config{RecoveryCheckpointInterval: time.Hour},
		sourceCluster: source,
		targetCluster: target,
		pcsm:          pipeline,
		membership:    membership,
		activeTerm:    1,
	}

	require.NoError(t, pipeline.Start(ctx, &pcsm.StartOptions{}))
	requireDemotedCloneDone(t, ctx, pipeline)

	// When: a write is in flight to the target and the lease moves on.
	mu.Lock()
	enabled = true
	mu.Unlock()

	_, err = sourceColl.InsertOne(ctx, bson.D{{"_id", "written-after-demotion"}})
	require.NoError(t, err)

	select {
	case <-gate.armed:
	case <-ctx.Done():
		require.FailNow(t, "no target write was issued for the source insert", "%v", ctx.Err())
	}

	// Another instance takes the lease and establishes term 2. This instance
	// is now provably deposed: its own checkpoint writes are fenced.
	// Only the term matters to the fence, and the payload is never read back
	// here, so the successor is a checkpoint rather than a second pipeline.
	// A real one would clone the source, and its copy of the document below
	// would be indistinguishable from a write by the deposed instance.
	require.NoError(t, DoCheckpoint(ctx, target, pipeline, 2, "pcsm-successor"))
	require.ErrorIs(t, DoCheckpoint(ctx, target, pipeline, 1, "pcsm-deposed"), errCheckpointFenced)

	// Demote through the real path. Pause is asynchronous, so run it
	// concurrently with the release rather than assuming it returns first.
	demoted := make(chan struct{})
	go func() {
		defer close(demoted)
		s.onDemote(ctx, 1)
	}()
	time.AfterFunc(2*time.Second, gate.releaseAll)

	select {
	case <-demoted:
	case <-ctx.Done():
		require.FailNow(t, "demotion did not complete", "%v", ctx.Err())
	}

	// Then: the deposed instance must not have landed the write.
	targetColl := target.Database(demotedTestDB).Collection(demotedTestColl)
	require.Eventually(t, func() bool {
		return !pipeline.Status(ctx).Repl.Pausing
	}, 30*time.Second, 200*time.Millisecond, "replication never finished pausing")

	count, err := targetColl.CountDocuments(ctx, bson.D{{"_id", "written-after-demotion"}})
	require.NoError(t, err)
	require.Zero(t, count,
		"a deposed ACTIVE wrote user data to the target after losing the lease to term 2")
}

// requireDemotedCloneDone waits until replication is running on a finished
// clone, so a later source insert is applied by repl rather than copied.
func requireDemotedCloneDone(t *testing.T, ctx context.Context, pipeline *pcsm.PCSM) {
	t.Helper()

	deadline := time.Now().Add(90 * time.Second)
	for {
		status := pipeline.Status(ctx)
		if status.State == pcsm.StateRunning && status.Clone.IsFinished() {
			return
		}
		if time.Now().After(deadline) {
			require.FailNow(t, "clone did not finish", "%+v", status)
		}

		time.Sleep(cloneWaitInterval)
	}
}
