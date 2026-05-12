package repl //nolint

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/errors"
)

// TestCheckpoint_NilSkipAdvancesPastFailedWorker is regression test for
// silent-data-skip-on-resume bug. It asserts the following scenario:
//  1. When worker's first bulk write fails, its lastTS stays nil
//     (uninitialized — the writer goroutine never reaches w.lastTS.Store())
//  2. workerPool.Checkpoint() silently skips that nil and returns the
//     min over the remaining workers, which is strictly greater than the
//     failed worker's last routed timestamp
//
// Together those two facts mean PCSM's deferred run() exit overwrites
// r.lastReplicatedOpTime with a checkpoint that is past the failure point;
// on resume, the change stream restarts there and every event in
// (T_fail, Checkpoint) is silently dropped.
//
// Affected code: pcsm/repl/worker.go: workerPool.Checkpoint()
//
//	for _, w := range p.workers {
//	    ts := w.lastTS.Load()
//	    if ts == nil {
//	        continue   // <-- silently skips failed workers
//	    }
//	    if first || ts.Before(minTS) {
//	        minTS = *ts; first = false
//	    }
//	}
func TestCheckpoint_NilSkipAdvancesPastFailedWorker(t *testing.T) {
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
		ts := pool.workers[1].lastCommitedTS.Load()

		return ts != nil && !ts.Before(tsGood)
	}, barrierTimeout, 10*time.Millisecond,
		"healthy worker should have committed up to tsGood=%v", tsGood)

	// Half 1 -- failed-on-first-bulk worker really does leave lastTS uninitialized
	failedLastTS := pool.workers[0].lastCommitedTS.Load()
	require.Nil(t, failedLastTS,
		"expected the failing worker's lastTS to be nil after its bulk write failure, "+
			"but got %v.", failedLastTS)
	t.Logf("Half 1 -- failed worker lastTS after error: %v", failedLastTS)

	// Half 2 -- Checkpoint silently skips that nil and min over remaining workers ends up > T_fail
	cp := pool.Checkpoint()
	healthyLastTS := pool.workers[1].lastCommitedTS.Load()
	t.Logf("Half 2 -- Checkpoint=%v (expected: <= T_fail=%v); healthy worker lastTS=%v",
		cp, tsFail, healthyLastTS)

	assert.False(t, cp.After(tsFail),
		"Checkpoint must not advance past the failed worker's routed "+
			"timestamp. Got cp=%v, T_fail=%v. Bug: workerPool.Checkpoint "+
			"silently ignores nil lastTS, so the resume checkpoint advances "+
			"past T_fail and PCSM silently drops every change-stream event "+
			"in (T_fail, cp) on resume.", cp, tsFail)
}
