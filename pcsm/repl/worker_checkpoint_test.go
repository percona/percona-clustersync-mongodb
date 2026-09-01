package repl //nolint

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/errors"
)

// TestCheckpoint_DoesNotAdvancePastFailedWorker is regression test for
// silent-data-skip-on-resume bug. It asserts the following scenario:
//  1. When worker's first bulk write fails, its lastCommitedTS stays nil
//     (uninitialized — the writer goroutine never reaches w.lastCommitedTS.Store())
//  2. workerPool.Checkpoint() silently skips that nil and returns the
//     min over the remaining workers, which is strictly greater than the
//     failed worker's last routed timestamp
//
// Together those two facts mean PCSM's deferred run() exit overwrites
// r.lastReplicatedOpTime with a checkpoint that is past the failure point;
// on resume, the change stream restarts there and every event in
// (T_fail, Checkpoint) is silently dropped.
func TestCheckpoint_DoesNotAdvancePastFailedWorker(t *testing.T) {
	t.Parallel()

	pool := makeTestPoolLive(t, []bulkWriter{
		&mockBulkWriter{doErr: errors.New("simulated apply failure")},
		&mockBulkWriter{},
	})

	tsFail := bson.Timestamp{T: 100, I: 0}
	tsGood := bson.Timestamp{T: 200, I: 0}

	pool.workers[0].lastRoutedTS.Store(&tsFail)
	pool.workers[0].routedEventCh <- makeInsertEventWithTS("victim", tsFail)

	pool.workers[1].lastRoutedTS.Store(&tsGood)
	pool.workers[1].routedEventCh <- makeInsertEventWithTS("buddy", tsGood)

	select {
	case err := <-pool.Err():
		require.Error(t, err, "expected the failing worker to report a bulk-write error")
	case <-time.After(barrierTimeout):
		t.Fatal("timed out waiting for the failing worker to report its error")
	}

	require.Eventually(t, func() bool {
		ts := pool.workers[1].lastCommittedTS.Load()

		return ts != nil && !ts.Before(tsGood)
	}, barrierTimeout, 10*time.Millisecond,
		"healthy worker should have committed up to tsGood=%v", tsGood)

	// failed-on-first-bulk worker leaves lastCommitedTS uninitialized
	failedLastTS := pool.workers[0].lastCommittedTS.Load()
	require.Nil(t, failedLastTS,
		"expected the failing worker's lastCommitedTS to be nil after its first bulk write failure, "+
			"but got %v.", failedLastTS)
	t.Logf("Half 1 -- failed worker lastCommitedTS after error: %v", failedLastTS)

	cp := pool.Checkpoint()
	healthyLastTS := pool.workers[1].lastCommittedTS.Load()
	t.Logf("Half 2 -- Checkpoint=%v (expected: <= T_fail=%v); healthy worker lastCommitedTS=%v",
		cp, tsFail, healthyLastTS)

	assert.False(t, cp.After(tsFail),
		"Checkpoint must not advance past the failed worker's routed "+
			"timestamp. Got cp=%v, T_fail=%v. Bug: workerPool.Checkpoint "+
			"silently ignores nil lastCommitedTS, so the resume checkpoint advances "+
			"past T_fail and PCSM silently drops every change-stream event "+
			"in (T_fail, cp) on resume.", cp, tsFail)
}

// TestCheckpoint_DoesNotAdvancePastFirstUncommittedEvent verifies that a worker
// with multiple routed events and no committed bulk resumes at the first event,
// not immediately before the last routed event.
func TestCheckpoint_DoesNotAdvancePastFirstUncommittedEvent(t *testing.T) {
	t.Parallel()

	pool := &workerPool{
		workers: []*worker{{
			id:            "0",
			routedEventCh: make(chan *routedEvent, 2),
		}},
		numWorkers: 1,
	}

	firstTS := bson.Timestamp{T: 100, I: 1}
	lastTS := bson.Timestamp{T: 100, I: 10}
	first := makeInsertEventWithTS("first-uncommitted", firstTS)
	last := makeInsertEventWithTS("last-uncommitted", lastTS)

	pool.Route(first.change, first.ns)
	pool.Route(last.change, last.ns)

	assert.Equal(t, firstTS, pool.Checkpoint(),
		"checkpoint must resume at the first routed event when no event has committed")
}
