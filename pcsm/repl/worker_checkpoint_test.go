package repl //nolint

import (
	"math"
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
		ts := pool.workers[1].lastCommitedTS.Load()

		return ts != nil && !ts.Before(tsGood)
	}, barrierTimeout, 10*time.Millisecond,
		"healthy worker should have committed up to tsGood=%v", tsGood)

	// Half 1 -- failed-on-first-bulk worker really does leave lastCommitedTS uninitialized
	failedLastTS := pool.workers[0].lastCommitedTS.Load()
	require.Nil(t, failedLastTS,
		"expected the failing worker's lastCommitedTS to be nil after its first bulk write failure, "+
			"but got %v.", failedLastTS)
	t.Logf("Half 1 -- failed worker lastCommitedTS after error: %v", failedLastTS)

	// Half 2 -- Checkpoint silently skips that nil and min over remaining workers ends up > T_fail
	cp := pool.Checkpoint()
	healthyLastTS := pool.workers[1].lastCommitedTS.Load()
	t.Logf("Half 2 -- Checkpoint=%v (expected: <= T_fail=%v); healthy worker lastTS=%v",
		cp, tsFail, healthyLastTS)

	assert.False(t, cp.After(tsFail),
		"Checkpoint must not advance past the failed worker's routed "+
			"timestamp. Got cp=%v, T_fail=%v. Bug: workerPool.Checkpoint "+
			"silently ignores nil lastCommitedTS, so the resume checkpoint advances "+
			"past T_fail and PCSM silently drops every change-stream event "+
			"in (T_fail, cp) on resume.", cp, tsFail)
}

// TestTsPredecessor verifies tsPredecessor returns the largest bson.Timestamp
// strictly less than the input, with correct wrap-around from (T, 0) to
// (T-1, math.MaxUint32) and saturation at the zero timestamp.
func TestTsPredecessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   bson.Timestamp
		want bson.Timestamp
	}{
		{
			name: "decrements I when I > 0",
			in:   bson.Timestamp{T: 100, I: 5},
			want: bson.Timestamp{T: 100, I: 4},
		},
		{
			name: "wraps to previous T when I == 0",
			in:   bson.Timestamp{T: 100, I: 0},
			want: bson.Timestamp{T: 99, I: math.MaxUint32},
		},
		{
			name: "saturates at zero timestamp",
			in:   bson.Timestamp{T: 0, I: 0},
			want: bson.Timestamp{T: 0, I: 0},
		},
		{
			name: "T == 1, I == 0 wraps to (0, MaxUint32)",
			in:   bson.Timestamp{T: 1, I: 0},
			want: bson.Timestamp{T: 0, I: math.MaxUint32},
		},
		{
			name: "I == 1 decrements to (T, 0) without wrapping",
			in:   bson.Timestamp{T: 42, I: 1},
			want: bson.Timestamp{T: 42, I: 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tsPredecessor(tt.in)
			assert.Equal(t, tt.want, got)
		})
	}
}
