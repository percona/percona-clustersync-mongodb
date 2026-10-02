//go:build integration

package clone_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/pcsm/clone"
	"github.com/percona/percona-clustersync-mongodb/sel"
)

// cloneTargetGate parks the first insert into coll inside its CommandStarted
// hook until that write's own context is canceled (the suspension under test)
// or the gate is released (failure bound). Once told that a resume started it
// records which target collections are dropped and written again.
type cloneTargetGate struct {
	db   string
	coll string

	mu       sync.Mutex
	parked   bool
	resumed  bool
	dropped  []string
	inserted map[string]int

	armed     chan struct{}
	release   chan struct{}
	relOnce   sync.Once
	parkedEnd chan bool
}

func newCloneTargetGate(db, coll string) *cloneTargetGate {
	return &cloneTargetGate{
		db:        db,
		coll:      coll,
		inserted:  make(map[string]int),
		armed:     make(chan struct{}),
		release:   make(chan struct{}),
		parkedEnd: make(chan bool, 1),
	}
}

func (g *cloneTargetGate) releaseAll() { g.relOnce.Do(func() { close(g.release) }) }

func (g *cloneTargetGate) markResumed() {
	g.mu.Lock()
	g.resumed = true
	g.mu.Unlock()
}

func (g *cloneTargetGate) droppedAfterResume() []string {
	g.mu.Lock()
	defer g.mu.Unlock()

	return append([]string(nil), g.dropped...)
}

func (g *cloneTargetGate) insertsAfterResume(coll string) int {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.inserted[coll]
}

func (g *cloneTargetGate) monitor() *event.CommandMonitor {
	return &event.CommandMonitor{
		Started: func(opCtx context.Context, cmd *event.CommandStartedEvent) {
			if cmd.DatabaseName != g.db {
				return
			}

			switch cmd.CommandName {
			case "drop":
				name, _ := cmd.Command.Lookup("drop").StringValueOK()

				g.mu.Lock()
				if g.resumed {
					g.dropped = append(g.dropped, name)
				}
				g.mu.Unlock()

				return
			case "insert":
			default:
				return
			}

			name, _ := cmd.Command.Lookup("insert").StringValueOK()

			g.mu.Lock()
			if g.resumed {
				g.inserted[name]++
			}

			park := name == g.coll && !g.parked && !g.resumed
			if park {
				g.parked = true
			}
			g.mu.Unlock()

			if !park {
				return
			}

			close(g.armed)

			select {
			case <-opCtx.Done():
				g.parkedEnd <- true
			case <-g.release:
				g.parkedEnd <- false
			}
		},
	}
}

func connectWithMonitor(t *testing.T, uri string, mon *event.CommandMonitor) *mongo.Client {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().
		ApplyURI(uri).
		SetServerSelectionTimeout(5 * time.Second).
		SetRetryWrites(false).
		SetMonitor(mon))
	require.NoError(t, err)
	require.NoError(t, client.Ping(ctx, nil))

	return client
}

func awaitClone(ctx context.Context, t *testing.T, done <-chan struct{}, msg string) {
	t.Helper()

	select {
	case <-done:
	case <-ctx.Done():
		require.FailNow(t, msg, ctx.Err().Error())
	}
}

func seedPadded(ctx context.Context, t *testing.T, coll *mongo.Collection, n, pad int) {
	t.Helper()

	docs := make([]any, 0, n)
	for i := range n {
		docs = append(docs, bson.D{{"_id", i}, {"pad", strings.Repeat("x", pad)}})
	}

	_, err := coll.InsertMany(ctx, docs)
	require.NoError(t, err)
}

func countDocs(ctx context.Context, t *testing.T, coll *mongo.Collection) int64 {
	t.Helper()

	n, err := coll.CountDocuments(ctx, bson.D{})
	require.NoError(t, err)

	return n
}

func naturalIDs(ctx context.Context, t *testing.T, coll *mongo.Collection) []int32 {
	t.Helper()

	cur, err := coll.Find(ctx, bson.D{}, options.Find().SetSort(bson.D{{"$natural", 1}}))
	require.NoError(t, err)

	var docs []struct {
		ID int32 `bson:"_id"`
	}
	require.NoError(t, cur.All(ctx, &docs))

	ids := make([]int32, len(docs))
	for i, d := range docs {
		ids[i] = d.ID
	}

	return ids
}

// TestClone_SuspendResumeRedoesOnlyInFlightCollections pins the clone half of
// the fence: canceling the run context ends a parked target insert before it
// reaches the wire and leaves the clone suspended (no error, no finish time);
// Resume keeps the completed collections, copies the in-flight one again from
// scratch, copies the rest, keeps the original startTS, and counts bytes
// exactly like an uninterrupted clone.
//
//nolint:paralleltest // drives the shared containers and parks writes with a command monitor
func TestClone_SuspendResumeRedoesOnlyInFlightCollections(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	source := connect(t, sourceURI)
	defer func() { _ = source.Disconnect(ctx) }()

	dbName := "pcsm388_" + bson.NewObjectID().Hex()
	defer func() { _ = source.Database(dbName).Drop(ctx) }()

	gate := newCloneTargetGate(dbName, "mid")
	defer gate.releaseAll()
	target := connectWithMonitor(t, targetURI, gate.monitor())
	defer func() { _ = target.Disconnect(ctx) }()
	defer func() { _ = target.Database(dbName).Drop(ctx) }()

	// Given: four collections whose sizes order them big, mid, small, capped.
	// Parallelism 1 copies them in that order, so mid is in flight once big
	// has completed.
	srcDB := source.Database(dbName)
	seedPadded(ctx, t, srcDB.Collection("big"), 300, 2048)
	seedPadded(ctx, t, srcDB.Collection("mid"), 200, 1024)
	seedPadded(ctx, t, srcDB.Collection("small"), 50, 256)
	require.NoError(t, srcDB.CreateCollection(ctx, "capped",
		options.CreateCollection().SetCapped(true).SetSizeInBytes(1<<20)))
	seedPadded(ctx, t, srcDB.Collection("capped"), 20, 16)

	sourceVer, err := mdb.Version(ctx, source)
	require.NoError(t, err)
	nsFilter := sel.MakeFilter([]string{dbName + ".*"}, nil)
	cat := catalog.NewCatalog(source, target, sourceVer)
	c := clone.NewClone(source, target, cat, nsFilter, &clone.Options{Parallelism: 1}, false)

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	require.NoError(t, c.Start(runCtx))
	done := c.Done()

	awaitClone(ctx, t, gate.armed, "no insert into mid was issued")

	// When: the run is canceled while mid's insert is parked.
	cancelRun()
	awaitClone(ctx, t, done, "clone did not stop after cancellation")

	// Then: the parked insert was ended by its own context and the clone is
	// suspended: not failed, not finished.
	select {
	case byCancel := <-gate.parkedEnd:
		require.True(t, byCancel, "cancellation did not reach the parked insert's context")
	case <-ctx.Done():
		require.FailNow(t, "parked insert never returned", ctx.Err().Error())
	}

	suspended := c.Status()
	require.NoError(t, suspended.Err)
	require.True(t, suspended.IsStarted())
	require.False(t, suspended.IsFinished(), "a canceled clone must not look finished")

	tgtDB := target.Database(dbName)
	require.Equal(t, int64(300), countDocs(ctx, t, tgtDB.Collection("big")))
	require.Zero(t, countDocs(ctx, t, tgtDB.Collection("small")))

	// Sentinels: the resume must leave a completed collection alone and must
	// drop and copy again the one that was in flight.
	_, err = tgtDB.Collection("big").InsertOne(ctx, bson.D{{"_id", "kept"}})
	require.NoError(t, err)
	_, err = tgtDB.Collection("mid").InsertOne(ctx, bson.D{{"_id", "redone"}})
	require.NoError(t, err)

	// And: resuming completes the clone from the same startTS.
	gate.markResumed()
	require.NoError(t, c.Resume(ctx))
	awaitClone(ctx, t, c.Done(), "resumed clone did not finish")

	final := c.Status()
	require.NoError(t, final.Err)
	require.True(t, final.IsFinished())
	require.Equal(t, suspended.StartTS, final.StartTS, "resume must keep the oplog anchor")
	require.False(t, final.FinishTS.IsZero())

	require.NotContains(t, gate.droppedAfterResume(), "big")
	require.Zero(t, gate.insertsAfterResume("big"))
	require.Contains(t, gate.droppedAfterResume(), "mid")

	require.Equal(t, int64(301), countDocs(ctx, t, tgtDB.Collection("big")))
	require.Equal(t, int64(200), countDocs(ctx, t, tgtDB.Collection("mid")))
	require.Equal(t, int64(50), countDocs(ctx, t, tgtDB.Collection("small")))
	require.Equal(t, naturalIDs(ctx, t, srcDB.Collection("capped")),
		naturalIDs(ctx, t, tgtDB.Collection("capped")))

	// And: the interrupted clone counted bytes exactly like a fresh one.
	require.NoError(t, tgtDB.Drop(ctx))
	control := clone.NewClone(source, target,
		catalog.NewCatalog(source, target, sourceVer), nsFilter, &clone.Options{Parallelism: 1}, false)
	require.NoError(t, control.Start(ctx))
	awaitClone(ctx, t, control.Done(), "control clone did not finish")

	controlStatus := control.Status()
	require.NoError(t, controlStatus.Err)
	require.Equal(t, controlStatus.CopiedSizeBytes, final.CopiedSizeBytes,
		"resume double-counted or lost the bytes of the redone collection")
	require.Equal(t, controlStatus.EstimatedTotalSizeBytes, final.EstimatedTotalSizeBytes)
}
