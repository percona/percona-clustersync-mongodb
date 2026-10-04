//go:build integration

package main //nolint:testpackage // Exercise demotion against a real source, target and pipeline.

import (
	"bytes"
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
	"github.com/percona/percona-clustersync-mongodb/util"
)

const (
	demotedTestDB     = "demoted_fencing_db"
	demotedTestColl   = "docs"
	demotedMarker     = "written-after-demotion"
	cloneWaitInterval = 200 * time.Millisecond
)

// startDemotedMongo starts the source and target containers. The source must
// be a replica set to serve change streams; the target is a separate
// standalone because replication copies into the same namespace. No TestMain
// is possible in this package (cli_test.go already defines one), so
// termination is left to the testcontainers reaper.
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

// markerWriteGate parks the first target data write that carries marker
// inside its CommandStarted hook. The hook returns when the write's own
// context is canceled (the fence under test) or, as a failure bound only,
// when the gate is released. It also records how the parked command finished
// and whether any data write was started while the instance was demoted.
type markerWriteGate struct {
	marker []byte

	mu        sync.Mutex
	enabled   bool
	parkedSet bool
	parkedID  int64
	started   map[int64]struct{} // RequestIDs of data writes carrying the marker
	demoted   bool
	late      int // data writes started while demoted

	armed   chan struct{}
	release chan struct{}
	armOnce sync.Once
	relOnce sync.Once

	parked          chan bool     // how the parked hook returned: true = its ctx was canceled
	parkedFailed    chan struct{} // CommandFailed for the parked RequestID
	markerSucceeded chan struct{} // CommandSucceeded for a write carrying the marker
}

func newMarkerWriteGate(marker string) *markerWriteGate {
	return &markerWriteGate{
		marker:          []byte(marker),
		started:         make(map[int64]struct{}),
		armed:           make(chan struct{}),
		release:         make(chan struct{}),
		parked:          make(chan bool, 1),
		parkedFailed:    make(chan struct{}, 1),
		markerSucceeded: make(chan struct{}, 1),
	}
}

func (g *markerWriteGate) enable() {
	g.mu.Lock()
	g.enabled = true
	g.mu.Unlock()
}

func (g *markerWriteGate) setDemoted(demoted bool) {
	g.mu.Lock()
	g.demoted = demoted
	g.mu.Unlock()
}

func (g *markerWriteGate) lateStarts() int {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.late
}

func (g *markerWriteGate) releaseAll() {
	g.relOnce.Do(func() { close(g.release) })
}

// isDataWrite reports whether cmd is a user-data write issued by the pipeline.
func isDataWrite(cmd *event.CommandStartedEvent) bool {
	switch cmd.CommandName {
	case "bulkWrite":
		// Client-level, used against 8.0. It runs on admin with the
		// namespaces inside the command, so the marker match is the filter.
		return true
	case "insert", "update", "delete":
		// Collection-level, used below 8.0. Checkpoints are written to the
		// target too, and parking one would deadlock DoCheckpoint.
		return cmd.DatabaseName == demotedTestDB
	default:
		return false
	}
}

// monitor parks the first enabled data write carrying the marker. The driver
// invokes these callbacks synchronously on the operation's goroutine and
// passes the operation's own context to Started, so the hook can observe the
// cancellation that the fence is supposed to deliver. Signal sends never
// block: every channel is buffered and written at most once per event.
func (g *markerWriteGate) monitor() *event.CommandMonitor {
	return &event.CommandMonitor{
		Started: func(opCtx context.Context, cmd *event.CommandStartedEvent) {
			if !isDataWrite(cmd) {
				return
			}

			g.mu.Lock()
			if g.demoted {
				g.late++
			}

			carries := bytes.Contains(cmd.Command, g.marker)
			if carries {
				g.started[cmd.RequestID] = struct{}{}
			}

			park := carries && g.enabled && !g.parkedSet
			if park {
				g.parkedSet = true
				g.parkedID = cmd.RequestID
			}
			g.mu.Unlock()

			if !park {
				return
			}

			g.armOnce.Do(func() { close(g.armed) })

			select {
			case <-opCtx.Done():
				g.parked <- true
			case <-g.release:
				g.parked <- false
			}
		},
		Failed: func(_ context.Context, evt *event.CommandFailedEvent) {
			g.mu.Lock()
			isParked := g.parkedSet && evt.RequestID == g.parkedID
			g.mu.Unlock()

			if !isParked {
				return
			}

			select {
			case g.parkedFailed <- struct{}{}:
			default:
			}
		},
		Succeeded: func(_ context.Context, evt *event.CommandSucceededEvent) {
			g.mu.Lock()
			_, carries := g.started[evt.RequestID]
			g.mu.Unlock()

			if !carries {
				return
			}

			select {
			case g.markerSucceeded <- struct{}{}:
			default:
			}
		},
	}
}

// TestDemotedActiveMustNotWriteTargetData pins the fence: an instance that
// has lost the lease stops writing user data to the target, and a same-term
// re-promotion replays what the suspension abandoned.
//
// A target write is parked inside its CommandStarted hook, another instance
// takes the lease (term 2), and this instance is demoted. Demotion must
// cancel the parked write's context (the driver then fails the command
// before it reaches the wire), leave the pipeline suspended, and start no
// further data write. Re-promotion in the same term resumes from the
// inclusive checkpoint floor and the write lands exactly once.
//
//nolint:paralleltest // Drives one pipeline against a shared source and target.
func TestDemotedActiveMustNotWriteTargetData(t *testing.T) {
	sourceURI, targetURI, err := startDemotedMongo(t.Context())
	require.NoError(t, err)

	require.NoError(t, util.CtxWithTimeout(t.Context(), 4*time.Minute, func(ctx context.Context) error {
		source, err := mongo.Connect(options.Client().
			ApplyURI(sourceURI).SetServerSelectionTimeout(30 * time.Second))
		require.NoError(t, err)
		defer func() { _ = source.Disconnect(context.Background()) }()

		// The gate is installed up front but only parks writes once enabled, so
		// the clone and the catalog setup run unimpeded.
		gate := newMarkerWriteGate(demotedMarker)
		defer gate.releaseAll()

		target, err := mongo.Connect(options.Client().
			ApplyURI(targetURI).
			SetServerSelectionTimeout(30 * time.Second).
			SetRetryWrites(false).
			SetMonitor(gate.monitor()))
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
		gate.enable()

		_, err = sourceColl.InsertOne(ctx, bson.D{{"_id", demotedMarker}})
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

		// Demote through the real path. Suspension cancels the parked write's
		// context, which releases the hook; the driver then fails the command
		// before it reaches the wire. The timer is a failure bound only: with the
		// fence in place the context wins long before it fires, and a release by
		// timer is reported as a failure below (byCancel false).
		gate.setDemoted(true)
		releaseBound := time.AfterFunc(10*time.Second, gate.releaseAll)
		defer releaseBound.Stop()

		s.onDemote(ctx, 1)

		var byCancel bool
		select {
		case byCancel = <-gate.parked:
		case <-ctx.Done():
			require.FailNow(t, "parked write was never released", "%v", ctx.Err())
		}
		require.True(t, byCancel, "demotion did not cancel the parked write's context")

		select {
		case <-gate.parkedFailed:
		case <-ctx.Done():
			require.FailNow(t, "parked write did not report CommandFailed", "%v", ctx.Err())
		}

		// Then: the deposed instance landed nothing and settled as suspended.
		status := pipeline.Status(ctx)
		require.Equal(t, pcsm.State(pcsm.StatePaused), status.State, "demotion did not suspend the pipeline")
		require.False(t, status.Repl.Pausing, "replication is still pausing after demotion returned")

		targetColl := target.Database(demotedTestDB).Collection(demotedTestColl)
		count, err := targetColl.CountDocuments(ctx, bson.D{{"_id", demotedMarker}})
		require.NoError(t, err)
		require.Zero(t, count,
			"a deposed ACTIVE wrote user data to the target after losing the lease to term 2")
		require.Zero(t, gate.lateStarts(), "a data write was started while demoted")

		// And: a same-term re-promotion resumes the suspended pipeline, which
		// replays the abandoned write from the inclusive checkpoint floor. A
		// same term means no other instance held the lease: the stand-in
		// successor's checkpoint goes first, or the resumed pipeline's own
		// checkpoint would be fenced by it and demote this instance again.
		require.NoError(t, DeleteRecoveryData(ctx, target))
		gate.setDemoted(false)
		s.onPromote(ctx, 1)

		select {
		case <-gate.markerSucceeded:
		case <-ctx.Done():
			require.FailNow(t, "resumed pipeline never replayed the suspended write", "%v", ctx.Err())
		}

		count, err = targetColl.CountDocuments(ctx, bson.D{{"_id", demotedMarker}})
		require.NoError(t, err)
		require.Equal(t, int64(1), count, "same-term resume did not replay the suspended write exactly once")

		return nil
	}))
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
