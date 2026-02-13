// Guard regression test for PCSM-222 deadlock.
//
// This test exercises the REAL production CopyManager orchestration
// (runInsertDispatcher → worker pool → waitAndCleanup) to verify the
// deadlock pattern cannot be reintroduced.
//
// Unlike deadlock_test.go (which uses self-contained copies of v0.6.0/HEAD
// logic), this test imports and calls actual production functions. The only
// mock is insertWorkerFunc on CopyManager, which bypasses real MongoDB
// InsertMany calls while preserving the full orchestration flow.
//
// If someone reintroduces a channel-based result collection pattern
// (replacing OnDone callbacks), this test will deadlock and fail.
//
// How to Run:
//
//	go test -v -run "TestCopyCollection_DeadlockGuard" ./pcsm/ -timeout 60s
package pcsm //nolint:testpackage

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCopyCollection_DeadlockGuard(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// 1. Create CopyManager with shim
		cm := NewCopyManager(nil, nil, CopyManagerOptions{
			NumInsertWorkers: 2,
			NumReadWorkers:   1,
		})
		cm.insertWorkerFunc = func(ctx context.Context, task insertTask) {
			// Simulate instant insert — call OnDone with success
			time.Sleep(10 * time.Millisecond) // simulate work
			task.OnDone(CopyProgressUpdate{
				SizeBytes: uint64(task.SizeBytes),
				Count:     len(task.Documents),
			})
		}
		defer cm.Close()

		// 2. Create session (like copyCollection does)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		session := newCollectionCopySession(ctx, Namespace{Database: "test", Collection: "test"}, false)

		// 3. Create progressCh with small buffer (creates natural back-pressure)
		progressCh := make(chan CopyProgressUpdate, 1)

		// 4. Start insert dispatcher (real production code)
		go cm.runInsertDispatcher(session, progressCh)

		// 5. Start waitAndCleanup in goroutine (normally blocks until session completes)
		//    We mark activeSegmentsWg as having one active segment (us, the test)
		session.activeSegmentsWg.Add(1)
		cleanupDone := make(chan struct{})
		go func() {
			session.waitAndCleanup()
			close(cleanupDone)
		}()

		// 6. Start draining progressCh (like production's Start() caller does)
		//    MUST start BEFORE feeding batches to avoid blocking workers
		go func() {
			for range progressCh {
			}
		}()

		// 7. Create a simple BSON document for test batches
		doc, _ := bson.Marshal(bson.D{{"_id", 1}})

		// 8. Feed batches to readBatchCh (bypassing readSegment/cursor entirely)
		numBatches := 5
		for i := range numBatches {
			session.readBatchCh <- readBatch{
				ID:        uint32(i + 1),
				Documents: []any{bson.Raw(doc), bson.Raw(doc)},
				SizeBytes: len(doc) * 2,
			}
		}

		// 9. Signal that reading is done — cancel session context (simulates segment completion)
		session.activeSegmentsWg.Done() // segment "done"
		session.cancel()                // triggers waitAndCleanup's <-s.ctx.Done()

		// 10. Wait for cleanup to complete — if deadlock, synctest panics
		<-cleanupDone

		// 11. Close progressCh after all done
		close(progressCh)

		t.Log("guard test: production orchestration completed without deadlock")
	})
}
