package repl //nolint

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

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
