package repl //nolint

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
)

// makeTestPool creates a worker pool with buffered channels but no running
// goroutines, so Route() enqueues events that can be inspected without races.
func makeTestPool(numWorkers int) *workerPool {
	p := &workerPool{
		workers:    make([]*worker, numWorkers),
		numWorkers: numWorkers,
	}

	for i := range numWorkers {
		p.workers[i] = &worker{
			id:            strconv.Itoa(i),
			routedEventCh: make(chan *routedEvent, 1000),
		}
	}

	return p
}

// makeChangeEvent builds a minimal ChangeEvent whose RawData contains a
// documentKey field, matching what Route() reads via bson.Raw.Lookup.
func makeChangeEvent(docKey bson.D) *ChangeEvent {
	raw, err := bson.Marshal(bson.D{{"documentKey", docKey}})
	if err != nil {
		panic(fmt.Sprintf("marshal documentKey: %v", err))
	}

	return &ChangeEvent{
		RawData: bson.Raw(raw),
	}
}

// workerEventCounts returns the number of events queued on each worker's channel.
func workerEventCounts(p *workerPool) []int {
	counts := make([]int, p.numWorkers)
	for i, w := range p.workers {
		counts[i] = len(w.routedEventCh)
	}

	return counts
}

func TestHashDocumentKey_Deterministic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		key        bson.D
		numWorkers int
	}{
		{"objectid_8w", bson.D{{"_id", bson.NewObjectID()}}, 8},
		{"string_8w", bson.D{{"_id", "some-key"}}, 8},
		{"int_16w", bson.D{{"_id", 42}}, 16},
		{"compound_4w", bson.D{{"_id", "x"}, {"sk", 1}}, 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			raw, err := bson.Marshal(bson.D{{"documentKey", tt.key}})
			require.NoError(t, err)

			docKey := bson.Raw(raw).Lookup("documentKey")
			first := hashDocumentKey(docKey.Value, tt.numWorkers)

			for range 100 {
				got := hashDocumentKey(docKey.Value, tt.numWorkers)
				assert.Equal(t, first, got, "hash must be deterministic")
			}
		})
	}
}

func TestHashNamespace_Deterministic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ns         catalog.Namespace
		numWorkers int
	}{
		{"simple", catalog.Namespace{Database: "db1", Collection: "coll1"}, 8},
		{"dotted", catalog.Namespace{Database: "my.db", Collection: "my.coll"}, 8},
		{"large_pool", catalog.Namespace{Database: "db", Collection: "c"}, 64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			first := hashNamespace(tt.ns, tt.numWorkers)

			for range 100 {
				got := hashNamespace(tt.ns, tt.numWorkers)
				assert.Equal(t, first, got, "hash must be deterministic")
			}

			assert.GreaterOrEqual(t, first, 0)
			assert.Less(t, first, tt.numWorkers)
		})
	}
}

func TestHashNamespace_DifferentNamespacesCanDiffer(t *testing.T) {
	t.Parallel()

	// With 64 workers and 20 distinct namespaces, it is extremely unlikely
	// that every namespace hashes to the same index.
	const numWorkers = 64

	seen := make(map[int]bool)

	for i := range 20 {
		ns := catalog.Namespace{
			Database:   fmt.Sprintf("db_%d", i),
			Collection: fmt.Sprintf("coll_%d", i),
		}
		seen[hashNamespace(ns, numWorkers)] = true
	}

	assert.Greater(t, len(seen), 1, "different namespaces should hash to different workers")
}

func TestRoute_SameDocumentGoesToSameWorker(t *testing.T) {
	t.Parallel()

	const numWorkers = 8
	const numEvents = 50

	pool := makeTestPool(numWorkers)
	ns := catalog.Namespace{Database: "db", Collection: "coll"}
	docKey := bson.D{{"_id", "same-doc"}}

	for range numEvents {
		pool.Route(makeChangeEvent(docKey), ns)
	}

	counts := workerEventCounts(pool)

	nonZero := 0

	for _, c := range counts {
		if c > 0 {
			nonZero++
			assert.Equal(t, numEvents, c, "all events must land on the same worker")
		}
	}

	assert.Equal(t, 1, nonZero, "exactly one worker should receive all events")
}

func TestRoute_DifferentDocumentsDistribute(t *testing.T) {
	t.Parallel()

	const numWorkers = 8
	const numEvents = 200

	pool := makeTestPool(numWorkers)
	ns := catalog.Namespace{Database: "db", Collection: "coll"}

	for i := range numEvents {
		docKey := bson.D{{"_id", fmt.Sprintf("doc-%d", i)}}
		pool.Route(makeChangeEvent(docKey), ns)
	}

	counts := workerEventCounts(pool)

	workersUsed := 0
	total := 0

	for _, c := range counts {
		total += c
		if c > 0 {
			workersUsed++
		}
	}

	assert.Equal(t, numEvents, total)
	assert.Greater(t, workersUsed, 1, "different documents should spread across multiple workers")
}

func TestRoute_CappedNamespaceRoutesToSameWorker(t *testing.T) {
	t.Parallel()

	const numWorkers = 8
	const numEvents = 50

	pool := makeTestPool(numWorkers)
	ns := catalog.Namespace{Database: "db", Collection: "capped_coll", Capped: true}

	// Route events with different document keys — they must all go to one worker.
	for i := range numEvents {
		docKey := bson.D{{"_id", fmt.Sprintf("doc-%d", i)}}
		pool.Route(makeChangeEvent(docKey), ns)
	}

	counts := workerEventCounts(pool)

	nonZero := 0

	for _, c := range counts {
		if c > 0 {
			nonZero++
			assert.Equal(t, numEvents, c, "all events for a capped namespace must land on the same worker")
		}
	}

	assert.Equal(t, 1, nonZero, "exactly one worker should receive all capped namespace events")
}

func TestRoute_CappedDifferentNamespacesCanDiffer(t *testing.T) {
	t.Parallel()

	const numWorkers = 64
	const numEvents = 20

	ns1 := catalog.Namespace{Database: "db1", Collection: "capped1", Capped: true}
	ns2 := catalog.Namespace{Database: "db2", Collection: "capped2", Capped: true}

	// Route each namespace to a separate pool to verify independently
	// that all events for each capped namespace land on a single worker.
	pool1 := makeTestPool(numWorkers)
	pool2 := makeTestPool(numWorkers)

	ns1Worker := -1
	ns2Worker := -1

	for i := range numEvents {
		pool1.Route(makeChangeEvent(bson.D{{"_id", fmt.Sprintf("a-%d", i)}}), ns1)
		pool2.Route(makeChangeEvent(bson.D{{"_id", fmt.Sprintf("b-%d", i)}}), ns2)
	}

	counts1 := workerEventCounts(pool1)
	counts2 := workerEventCounts(pool2)

	for i, c := range counts1 {
		if c == numEvents {
			ns1Worker = i
		}
	}

	for i, c := range counts2 {
		if c == numEvents {
			ns2Worker = i
		}
	}

	assert.GreaterOrEqual(t, ns1Worker, 0, "ns1 events must all be on one worker")
	assert.GreaterOrEqual(t, ns2Worker, 0, "ns2 events must all be on one worker")

	// With 64 workers, two different namespaces are unlikely to collide,
	// but it's valid if they do. Just log it.
	t.Logf("ns1 -> worker %d, ns2 -> worker %d", ns1Worker, ns2Worker)
}

func TestRoute_NonCappedIgnoresCappedRouting(t *testing.T) {
	t.Parallel()

	const numWorkers = 8
	const numEvents = 200

	pool := makeTestPool(numWorkers)
	ns := catalog.Namespace{Database: "db", Collection: "regular", Capped: false}

	for i := range numEvents {
		docKey := bson.D{{"_id", fmt.Sprintf("doc-%d", i)}}
		pool.Route(makeChangeEvent(docKey), ns)
	}

	counts := workerEventCounts(pool)

	workersUsed := 0

	for _, c := range counts {
		if c > 0 {
			workersUsed++
		}
	}

	assert.Greater(t, workersUsed, 1,
		"non-capped collections must still distribute by document key across multiple workers")
}

// ---------------------------------------------------------------------------
// Mock bulkWriter and helpers for barrier deadlock tests
// ---------------------------------------------------------------------------

// mockBulkWriter implements bulkWriter with configurable failure behavior.
// When doErr is set, Do() returns that error, simulating a flush failure
// (e.g. schema validation error, write concern error).
type mockBulkWriter struct {
	doErr  error // if set, Do() returns this error
	count  int   // number of pending operations
	fullAt int   // Full() returns true when count >= fullAt (0 = never full)
}

func (m *mockBulkWriter) Full() bool  { return m.fullAt > 0 && m.count >= m.fullAt }
func (m *mockBulkWriter) Empty() bool { return m.count == 0 }

func (m *mockBulkWriter) Do(_ context.Context, _ *mongo.Client) (int, error) {
	n := m.count
	m.count = 0

	if m.doErr != nil {
		return 0, m.doErr
	}

	return n, nil
}

func (m *mockBulkWriter) Insert(_ catalog.Namespace, _ *InsertEvent)   { m.count++ }
func (m *mockBulkWriter) Update(_ catalog.Namespace, _ *UpdateEvent)   { m.count++ }
func (m *mockBulkWriter) Replace(_ catalog.Namespace, _ *ReplaceEvent) { m.count++ }
func (m *mockBulkWriter) Delete(_ catalog.Namespace, _ *DeleteEvent)   { m.count++ }

// makeInsertEvent creates a valid routedEvent with an insert ChangeEvent.
// The RawData contains the minimal BSON that parseDMLEvent can unmarshal.
func makeInsertEvent(id string) *routedEvent {
	raw, err := bson.Marshal(bson.D{
		{"documentKey", bson.D{{"_id", id}}},
		{"fullDocument", bson.D{{"_id", id}, {"x", 1}}},
	})
	if err != nil {
		panic(fmt.Sprintf("marshal insert event: %v", err))
	}

	return &routedEvent{
		change: &ChangeEvent{
			EventHeader: EventHeader{OperationType: Insert},
			RawData:     bson.Raw(raw),
		},
		ns: catalog.Namespace{Database: "testdb", Collection: "testcoll"},
	}
}

// makeTestPoolLive creates a worker pool with real running goroutines using
// the provided bulkWriters. Unlike makeTestPool, workers are actively
// processing events. The pool is cleaned up when the test finishes.
func makeTestPoolLive(t *testing.T, bws []bulkWriter) *workerPool {
	t.Helper()

	numWorkers := len(bws)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	p := &workerPool{
		workers:    make([]*worker, numWorkers),
		numWorkers: numWorkers,
		errCh:      errCh,
		cancel:     cancel,
	}

	for i, bw := range bws {
		w := &worker{
			id:            strconv.Itoa(i),
			routedEventCh: make(chan *routedEvent, 100),
			bulkWrite:     bw,
			barrierReq:    make(chan struct{}),
			barrierDone:   make(chan error),
			resumeCh:      make(chan struct{}),
			done:          make(chan struct{}),
			errCh:         errCh,
		}
		p.workers[i] = w

		p.wg.Go(func() {
			w.run(ctx)
		})
	}

	t.Cleanup(func() {
		cancel()

		for _, w := range p.workers {
			close(w.routedEventCh)
		}

		// Drain barrier channels to unblock workers stuck in barrier wait
		for _, w := range p.workers {
			select {
			case w.resumeCh <- struct{}{}:
			default:
			}
		}

		p.wg.Wait()
	})

	return p
}

// barrierTimeout is the deadline for barrier operations in tests.
// Kept short to fail fast on deadlock, but long enough for goroutine scheduling.
const barrierTimeout = 3 * time.Second

// TestBarrier_WorkerDiesDuringFlush verifies that Barrier() does not deadlock
// when a worker's flush fails inside the barrier handler (Race 1).
//
// Scenario:
//  1. Events are routed to workers (both healthy and failing).
//  2. Barrier() is called, which triggers flush on all workers.
//  3. Worker 0's flush fails (mockBulkWriter.doErr) → worker exits run()
//     without sending on barrierDone.
//  4. BUG: Barrier() blocks forever on <-w.barrierDone for the dead worker.
//  5. FIX: Barrier() should detect the dead worker and return an error.
func TestBarrier_WorkerDiesDuringFlush(t *testing.T) {
	t.Parallel()

	pool := makeTestPoolLive(t, []bulkWriter{
		&mockBulkWriter{doErr: errors.New("validation error")}, // worker 0: flush fails
		&mockBulkWriter{}, // worker 1: healthy
	})

	// Route one event to each worker directly via their channels.
	// These sit in the worker's routedEventCh until drained by the barrier.
	pool.workers[0].routedEventCh <- makeInsertEvent("doc-0")
	pool.workers[1].routedEventCh <- makeInsertEvent("doc-1")

	// Call Barrier(). Worker 0 will drain its event, flush → fail, and exit
	// without signaling barrierDone. On current code this deadlocks.
	done := make(chan struct{})

	go func() {
		_ = pool.Barrier()
		close(done)
	}()

	select {
	case <-done:
		// After fix: Barrier returned (worker error propagated). Test passes.
	case <-time.After(barrierTimeout):
		t.Fatal("Barrier() deadlocked: worker 0 died during barrier flush " +
			"but Barrier blocks forever waiting on barrierDone")
	}
}

// TestBarrier_WorkerAlreadyDead verifies that Barrier() does not deadlock
// when called after a worker has already exited (Race 2).
//
// Scenario:
//  1. Worker 0's mockBulkWriter has fullAt=1, so the first event triggers
//     Full() → flush() → fails → worker exits run().
//  2. The error is confirmed via pool.Err().
//  3. Barrier() is called on the pool with the dead worker.
//  4. BUG: Barrier() blocks forever on w.barrierReq <- struct{}{} because
//     the dead worker is not reading from barrierReq.
//  5. FIX: Barrier() should detect the dead worker and return an error.
func TestBarrier_WorkerAlreadyDead(t *testing.T) {
	t.Parallel()

	pool := makeTestPoolLive(t, []bulkWriter{
		&mockBulkWriter{doErr: errors.New("write error"), fullAt: 1}, // worker 0: dies on first flush
		&mockBulkWriter{}, // worker 1: healthy
	})

	// Send one event to worker 0 → count reaches fullAt → Full() == true →
	// flush() called in the event case → fails → worker exits.
	pool.workers[0].routedEventCh <- makeInsertEvent("doc-0")

	// Wait for the worker to report its error before calling Barrier.
	select {
	case <-pool.Err():
		// Worker 0 is confirmed dead.
	case <-time.After(barrierTimeout):
		t.Fatal("timed out waiting for worker 0 to report error")
	}

	// Now call Barrier(). Worker 0 is dead and can't receive on barrierReq.
	// On current code, the send blocks forever.
	done := make(chan struct{})

	go func() {
		_ = pool.Barrier()
		close(done)
	}()

	select {
	case <-done:
		// After fix: Barrier detected dead worker via w.done, returned error.
	case <-time.After(barrierTimeout):
		t.Fatal("Barrier() deadlocked: dead worker can't receive on barrierReq, " +
			"Barrier blocks forever on send")
	}
}

// TestReleaseBarrier_WorkerDead verifies that ReleaseBarrier() does not
// deadlock when a worker has exited. This mirrors the real-world sequence:
// Barrier() detects a dead worker and returns an error, then the dispatcher
// calls ReleaseBarrier() to unblock surviving workers before calling setFailed.
//
// Scenario:
//  1. Worker 0 dies during a Barrier (flush fails).
//  2. Barrier() returns an error (worker 0 dead, worker 1 paused on resumeCh).
//  3. ReleaseBarrier() is called to unblock worker 1.
//  4. BUG: ReleaseBarrier() blocks forever on dead worker 0's resumeCh.
//  5. FIX: ReleaseBarrier() should skip dead workers via w.done select.
func TestReleaseBarrier_WorkerDead(t *testing.T) {
	t.Parallel()

	pool := makeTestPoolLive(t, []bulkWriter{
		&mockBulkWriter{doErr: errors.New("write error")}, // worker 0: flush fails during barrier
		&mockBulkWriter{}, // worker 1: healthy
	})

	// Route one event to each worker so both have work to flush during barrier.
	pool.workers[0].routedEventCh <- makeInsertEvent("doc-0")
	pool.workers[1].routedEventCh <- makeInsertEvent("doc-1")

	// Barrier: worker 0 flush fails → Barrier returns error.
	// Worker 1 completed its barrier and is now blocked on resumeCh.
	err := pool.Barrier()
	require.Error(t, err, "Barrier should return error from worker 0's failed flush")

	// ReleaseBarrier must unblock worker 1 and skip dead worker 0.
	done := make(chan struct{})

	go func() {
		pool.ReleaseBarrier()
		close(done)
	}()

	select {
	case <-done:
		// ReleaseBarrier skipped dead worker 0, resumed worker 1.
	case <-time.After(barrierTimeout):
		t.Fatal("ReleaseBarrier() deadlocked: dead worker can't receive on resumeCh, " +
			"ReleaseBarrier blocks forever on send")
	}
}
