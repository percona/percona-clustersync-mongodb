package repl

import (
	"context"
	"hash/fnv"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/log"
	"github.com/percona/percona-clustersync-mongodb/metrics"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
)

// routedEvent bundles a change event with its pre-resolved namespace.
// Namespace is resolved in the dispatcher using uuidMap before routing to worker.
// This includes enriched fields like Sharded and ShardKey from the catalog.
type routedEvent struct {
	change *ChangeEvent
	ns     catalog.Namespace
}

// worker handles a subset of document writes in parallel.
// Events are routed to workers based on document key hash to ensure
// operations on the same document are processed by the same worker,
// preserving per-document ordering.
type worker struct {
	id int

	routedEventCh chan *routedEvent
	bulkWrite     bulkWriter
	lastTS        atomic.Pointer[bson.Timestamp] // last committed timestamp
	pendingTS     bson.Timestamp                 // timestamp of last event in current batch

	target *mongo.Client

	// Barrier coordination
	barrierReq  chan struct{}
	barrierDone chan struct{}
	resumeCh    chan struct{}

	// Error reporting
	errCh chan<- error

	// Metrics
	eventsApplied atomic.Int64
}

// newWorker creates a new replication worker.
func newWorker(
	id int,
	target *mongo.Client,
	useCollectionBulk bool,
	useSimpleCollation bool,
	errC chan<- error,
) *worker {
	var bw bulkWriter
	if useCollectionBulk {
		bw = newCollectionBulkWriter(config.BulkOpsSize, useSimpleCollation)
	} else {
		bw = newClientBulkWriter(config.BulkOpsSize, useSimpleCollation)
	}

	return &worker{
		id:            id,
		routedEventCh: make(chan *routedEvent, config.ReplQueueSize),
		bulkWrite:     bw,
		target:        target,
		barrierReq:    make(chan struct{}),
		barrierDone:   make(chan struct{}),
		resumeCh:      make(chan struct{}),
		errCh:         errC,
	}
}

// run is the main loop for the worker. It processes events from eventC,
// batches them, and flushes on buffer full, time interval, or barrier signal.
func (w *worker) run(ctx context.Context) {
	lg := log.New("repl:worker").With(log.Int64("id", int64(w.id)))
	lg.Debug("Worker started")

	ticker := time.NewTicker(config.BulkOpsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Flush remaining ops before exit
			if !w.bulkWrite.Empty() {
				w.flush(ctx, lg)
			}

			lg.Debug("Worker stopped (context done)")

			return

		case <-w.barrierReq:
			lg.Debug("Barrier requested")

			// Drain all buffered events routed before the barrier was requested.
			if !w.drainRoutedEvents(ctx, lg) {
				return
			}

			// Flush remaining ops and signal done.
			if !w.bulkWrite.Empty() {
				if !w.flush(ctx, lg) {
					lg.Debug("Worker stopped (flush error)")

					return
				}
			}

			w.barrierDone <- struct{}{}
			lg.Debug("Barrier complete, waiting for resume")

			// Wait for resume signal or context cancellation
			select {
			case <-w.resumeCh:
				lg.Debug("Resumed")
			case <-ctx.Done():
				lg.Debug("Worker stopped (context done while waiting for resume)")

				return
			}

		case <-ticker.C:
			if !w.bulkWrite.Empty() {
				if !w.flush(ctx, lg) {
					lg.Debug("Worker stopped (flush error)")

					return
				}
			}

		case event, ok := <-w.routedEventCh:
			if !ok {
				// Channel closed, flush and exit
				if !w.bulkWrite.Empty() {
					w.flush(ctx, lg)
				}

				lg.Debug("Worker stopped (channel closed)")

				return
			}

			w.addToBatch(event)

			if w.bulkWrite.Full() {
				if !w.flush(ctx, lg) {
					lg.Debug("Worker stopped (flush error)")

					return
				}
			}
		}
	}
}

// addToBatch adds an event to the bulk write buffer.
func (w *worker) addToBatch(event *routedEvent) {
	switch event.change.OperationType { //nolint:exhaustive
	case Insert:
		e := event.change.Event.(InsertEvent) //nolint:forcetypeassert
		w.bulkWrite.Insert(event.ns, &e)

	case Update:
		e := event.change.Event.(UpdateEvent) //nolint:forcetypeassert
		w.bulkWrite.Update(event.ns, &e)

	case Delete:
		e := event.change.Event.(DeleteEvent) //nolint:forcetypeassert
		w.bulkWrite.Delete(event.ns, &e)

	case Replace:
		e := event.change.Event.(ReplaceEvent) //nolint:forcetypeassert
		w.bulkWrite.Replace(event.ns, &e)
	}

	// Track the timestamp of the last event in the batch
	w.pendingTS = event.change.ClusterTime
}

// flush executes the bulk write and updates metrics.
// Returns false if an error occurred and the worker should stop.
func (w *worker) flush(ctx context.Context, lg log.Logger) bool {
	size, err := w.bulkWrite.Do(ctx, w.target)
	if err != nil {
		// Report error and signal to stop
		select {
		case w.errCh <- err:
		default:
			// Error channel full, another error already reported
		}

		return false
	}

	if size > 0 {
		w.eventsApplied.Add(int64(size))
		metrics.AddEventsApplied(size)

		// Update the last committed timestamp
		ts := w.pendingTS
		w.lastTS.Store(&ts)

		lg.With(log.Int64("size", int64(size))).Debug("Flushed batch")
	}

	return true
}

// drainRoutedEvents reads all buffered events from routedEventCh and adds them
// to the current batch, flushing when the batch is full. This must be called
// when handling a barrier request to ensure all events routed before the
// barrier are included in the bulk.
// Returns false if a flush error occurred and the worker should stop.
func (w *worker) drainRoutedEvents(ctx context.Context, lg log.Logger) bool {
	for {
		select {
		case event := <-w.routedEventCh:
			w.addToBatch(event)

			if w.bulkWrite.Full() {
				if !w.flush(ctx, lg) {
					return false
				}
			}
		default:
			return true
		}
	}
}

// workerPool manages parallel replication workers.
type workerPool struct {
	workers    []*worker
	numWorkers int

	target             *mongo.Client
	useCollectionBulk  bool
	useSimpleCollation bool

	errCh  chan error
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// newWorkerPool creates a new worker pool with the specified number of workers.
// If numWorkers is 0, it defaults to runtime.NumCPU().
func newWorkerPool(
	ctx context.Context,
	numWorkers int,
	target *mongo.Client,
	useCollectionBulk bool,
	useSimpleCollation bool,
) *workerPool {
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}

	poolCtx, cancel := context.WithCancel(ctx)

	p := &workerPool{
		workers:            make([]*worker, numWorkers),
		numWorkers:         numWorkers,
		target:             target,
		useCollectionBulk:  useCollectionBulk,
		useSimpleCollation: useSimpleCollation,
		errCh:              make(chan error, 1),
		cancel:             cancel,
	}

	// Create and start workers
	for i := range numWorkers {
		w := newWorker(i, target, useCollectionBulk, useSimpleCollation, p.errCh)
		p.workers[i] = w

		p.wg.Go(func() {
			w.run(poolCtx)
		})
	}

	log.New("repl:pool").With(log.Int64("workers", int64(numWorkers))).
		Info("Worker pool started")

	return p
}

// Route sends an event to the appropriate worker based on document key hash.
func (p *workerPool) Route(change *ChangeEvent, ns catalog.Namespace) {
	docKey := extractDocumentKey(change)
	workerIdx := hashDocumentKey(docKey, p.numWorkers)

	p.workers[workerIdx].routedEventCh <- &routedEvent{
		change: change,
		ns:     ns,
	}
}

// Barrier flushes all workers and waits for them to complete.
// After Barrier returns, all workers are paused and no writes are in-flight.
func (p *workerPool) Barrier() {
	// Signal all workers to flush and pause
	for _, w := range p.workers {
		w.barrierReq <- struct{}{}
	}

	// Wait for all workers to acknowledge and flush pending ops
	for _, w := range p.workers {
		<-w.barrierDone
	}
}

// ReleaseBarrier signals all workers to resume processing.
func (p *workerPool) ReleaseBarrier() {
	for _, w := range p.workers {
		w.resumeCh <- struct{}{}
	}
}

// SafeCheckpoint returns the minimum committed timestamp across all workers.
// This is the safe point to resume from after a restart.
func (p *workerPool) SafeCheckpoint() bson.Timestamp {
	var minTS bson.Timestamp
	first := true

	for _, w := range p.workers {
		ts := w.lastTS.Load()
		if ts == nil {
			continue
		}

		if first || ts.Before(minTS) {
			minTS = *ts
			first = false
		}
	}

	return minTS
}

// TotalEventsApplied returns the sum of events applied across all workers.
func (p *workerPool) TotalEventsApplied() int64 {
	var total int64
	for _, w := range p.workers {
		total += w.eventsApplied.Load()
	}

	return total
}

// Stop gracefully shuts down all workers.
func (p *workerPool) Stop() {
	p.cancel()

	// Close all worker event channels to signal them to exit
	for _, w := range p.workers {
		close(w.routedEventCh)
	}

	// Drain barrier channels to unblock any workers waiting on barrier operations.
	// This handles the case where Stop() is called while a barrier is in progress.
	for _, w := range p.workers {
		// Non-blocking drain of resumeC to unblock workers waiting for resume
		select {
		case w.resumeCh <- struct{}{}:
		default:
		}
	}

	p.wg.Wait()

	log.New("repl:pool").Debug("Worker pool stopped")
}

// Err returns the error channel for receiving worker errors.
func (p *workerPool) Err() <-chan error {
	return p.errCh
}

// NumWorkers returns the number of workers in the pool.
func (p *workerPool) NumWorkers() int {
	return p.numWorkers
}

// extractDocumentKey extracts the document key bytes from a change event for hashing.
func extractDocumentKey(event *ChangeEvent) []byte {
	switch e := event.Event.(type) {
	case InsertEvent:
		data, _ := bson.Marshal(e.DocumentKey)

		return data

	case UpdateEvent:
		data, _ := bson.Marshal(e.DocumentKey)

		return data

	case DeleteEvent:
		data, _ := bson.Marshal(e.DocumentKey)

		return data

	case ReplaceEvent:
		// ReplaceEvent.DocumentKey is already bson.Raw
		return []byte(e.DocumentKey)
	}

	return nil
}

// hashDocumentKey computes a consistent hash of the document key
// and returns a worker index in range [0, numWorkers).
func hashDocumentKey(key []byte, numWorkers int) int {
	h := fnv.New32a()
	h.Write(key)

	return int(h.Sum32()) % numWorkers
}

// applyTransaction applies all DML events from a transaction as a single ordered
// bulk write. This preserves intra-transaction ordering while applying everything
// in one round-trip instead of individual operations.
func applyTransaction(
	ctx context.Context,
	target *mongo.Client,
	events []*routedEvent,
	useCollectionBulk bool,
	useSimpleCollation bool,
) error {
	if len(events) == 0 {
		return nil
	}

	var bw bulkWriter
	if useCollectionBulk {
		bw = newCollectionBulkWriter(len(events), useSimpleCollation)
	} else {
		bw = newClientBulkWriter(len(events), useSimpleCollation)
	}

	for _, event := range events {
		switch event.change.OperationType { //nolint:exhaustive
		case Insert:
			e := event.change.Event.(InsertEvent) //nolint:forcetypeassert
			bw.Insert(event.ns, &e)

		case Update:
			e := event.change.Event.(UpdateEvent) //nolint:forcetypeassert
			bw.Update(event.ns, &e)

		case Delete:
			e := event.change.Event.(DeleteEvent) //nolint:forcetypeassert
			bw.Delete(event.ns, &e)

		case Replace:
			e := event.change.Event.(ReplaceEvent) //nolint:forcetypeassert
			bw.Replace(event.ns, &e)

		default:
			return errors.Errorf("unexpected operation %q in transaction", event.change.OperationType)
		}
	}

	_, err := bw.Do(ctx, target)

	return errors.Wrap(err, "transaction bulk write")
}
