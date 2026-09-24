package repl //nolint:testpackage // Exercises the Route/Checkpoint interleaving guard.

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// docIDForWorker returns a document id whose documentKey hashes to workerIdx
// in a pool of numWorkers, so a test can pick the worker an event routes to.
func docIDForWorker(t *testing.T, workerIdx, numWorkers int) string {
	t.Helper()

	for i := range 10_000 {
		id := "doc-" + strconv.Itoa(i)
		raw, err := bson.Marshal(bson.D{{"_id", id}})
		require.NoError(t, err)

		if hashDocumentKey(raw, numWorkers) == workerIdx {
			return id
		}
	}

	require.FailNowf(t, "no document id found", "no id hashes to worker %d of %d", workerIdx, numWorkers)

	return ""
}

// makeRacePool is makeTestPool with tiny queues: the race test builds
// thousands of pools and never drains them.
func makeRacePool(numWorkers int) *workerPool {
	p := &workerPool{
		workers:    make([]*worker, numWorkers),
		numWorkers: numWorkers,
	}

	for i := range numWorkers {
		p.workers[i] = &worker{
			id:            strconv.Itoa(i),
			routedEventCh: make(chan *routedEvent, 4),
		}
	}

	return p
}

// TestCheckpoint_ConcurrentRouteNeverSkipsNewlyRoutedWorker is a guard, not a
// proof: it can only fail when the scan is not snapshot-consistent with Route.
//
// The first worker receives exactly one event at T=100 and never commits, so
// the correct floor is 100 for the whole test. Every later event goes to the
// last worker with a newer timestamp. A scan that reads the first worker before
// its Route and the last worker after it takes the last worker's firstRoutedTS
// as the floor and runs past the unapplied T=100, which is the
// silent-loss-on-resume case Checkpoint's doc comment forbids. The workers in
// between only widen the window between those two loads. The scanner and the
// router run concurrently, iterations are bounded, nothing sleeps.
//
// ReportedFrontier has the same shape and the same hazard (worker 0 is busy:
// routed, never committed), so it races routing in its own subtest rather than
// behind Checkpoint's locked scan, which would already have serialized it.
func TestCheckpoint_ConcurrentRouteNeverSkipsNewlyRoutedWorker(t *testing.T) {
	t.Parallel()

	scans := map[string]func(*workerPool) bson.Timestamp{
		"Checkpoint":       (*workerPool).Checkpoint,
		"ReportedFrontier": (*workerPool).ReportedFrontier,
	}

	for name, scan := range scans {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			runRouteRace(t, scan)
		})
	}
}

func runRouteRace(t *testing.T, scan func(*workerPool) bson.Timestamp) {
	t.Helper()

	const (
		numWorkers  = 32
		rounds      = 20_000
		laterEvents = 2
	)

	floor := bson.Timestamp{T: 100, I: 1}
	firstID := docIDForWorker(t, 0, numWorkers)
	laterID := docIDForWorker(t, numWorkers-1, numWorkers)

	for round := range rounds {
		pool := makeRacePool(numWorkers)
		done := make(chan struct{})

		go func() {
			defer close(done)

			first := makeInsertEventWithTS(firstID, floor)
			pool.Route(first.change, first.ns)

			for i := range uint32(laterEvents) {
				ts := bson.Timestamp{T: floor.T + 1 + i, I: 1}
				later := makeInsertEventWithTS(laterID, ts)
				pool.Route(later.change, later.ns)
			}
		}()

		for scanning := true; scanning; {
			select {
			case <-done:
				scanning = false
			default:
			}

			got := scan(pool)
			if !got.IsZero() {
				require.Falsef(t, floor.Before(got),
					"round %d: scan %v ran past worker 0's unapplied %v", round, got, floor)
			}
		}

		require.Equalf(t, floor, scan(pool), "round %d: settled value", round)
	}
}
