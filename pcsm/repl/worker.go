package repl

import (
	"context"
	"hash/fnv"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

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

// pendingBulk is a sealed bulk ready for writing by the writer goroutine.
// It bundles the filled bulkWriter with the checkpoint timestamp that should
// be committed after a successful write.
type pendingBulk struct {
	writer     bulkWriter     // filled bulk, ready to execute
	checkpoint bson.Timestamp // pendingTS when this bulk was sealed
}

// worker handles a subset of document writes in parallel.
// Events are routed to workers based on document key hash to ensure
// operations on the same document are processed by the same worker,
// preserving per-document ordering.
//
// Each worker has an async writer goroutine (runWriter) that executes
// bulk writes in the background. The main loop (run) builds bulks and
// enqueues them to pendingBulkCh; the writer goroutine dequeues and writes
// them. This decouples event reading from MongoDB writes, allowing the
// worker to keep reading events while a write is in-flight.
type worker struct {
	id string

	routedEventCh chan *routedEvent
	lastTS        atomic.Pointer[bson.Timestamp] // last committed timestamp
	lastRoutedTS  atomic.Pointer[bson.Timestamp] // last timestamp dispatched to this worker
	pendingTS     bson.Timestamp                 // timestamp of last event in current batch
	tickerOffset  time.Duration                  // stagger delay before starting the flush ticker
	flushInterval time.Duration                  // maximum interval between bulk write flushes

	target *mongo.Client

	// Async writer pipeline
	currentBulkWrite bulkWriter        // active bulk being filled by the main loop, not safe for concurrent access
	pendingBulkCh    chan *pendingBulk // buffered queue of sealed bulks for the writer goroutine
	writerDone       chan struct{}     // closed when the writer goroutine exits
	writerErr        error             // set by runWriter on failure, read after <-writerDone

	// Factory for creating fresh bulkWriters after each enqueueBulk/restart.
	bulkQueueSize int
	newBulkWriter func() bulkWriter

	// Barrier coordination
	barrierReq  chan struct{}
	barrierDone chan error
	resumeCh    chan struct{}

	// Lifecycle: closed when run() exits, used by Barrier/ReleaseBarrier
	// to detect dead workers and avoid blocking on their channels.
	done chan struct{}

	// Error reporting
	errCh chan<- error

	// Metrics
	eventsApplied atomic.Int64
}

// newWorker creates a new replication worker.
func newWorker(
	id int,
	opts *Options,
	target *mongo.Client,
	useCollectionBulk bool,
	useSimpleCollation bool,
	errC chan<- error,
) *worker {
	w := &worker{
		id:            strconv.Itoa(id),
		routedEventCh: make(chan *routedEvent, opts.WorkerQueueSize),
		flushInterval: opts.WorkerFlushInterval,
		target:        target,
		bulkQueueSize: opts.WorkerBulkQueueSize,
		barrierReq:    make(chan struct{}),
		barrierDone:   make(chan error),
		resumeCh:      make(chan struct{}),
		done:          make(chan struct{}),
		errCh:         errC,
	}

	bulkOpsSize := opts.BulkOpsSize

	if useCollectionBulk {
		w.newBulkWriter = func() bulkWriter {
			return newCollectionBulkWriter(bulkOpsSize, useSimpleCollation)
		}
	} else {
		w.newBulkWriter = func() bulkWriter {
			return newClientBulkWriter(bulkOpsSize, useSimpleCollation)
		}
	}

	w.currentBulkWrite = w.newBulkWriter()
	w.pendingBulkCh = make(chan *pendingBulk, w.bulkQueueSize)
	w.writerDone = make(chan struct{})

	return w
}

// runWriter is the writer goroutine. It reads sealed bulks from bulkQueue,
// executes them against MongoDB, and updates the committed timestamp.
// It exits when bulkQueue is closed (normal) or a write fails (error).
// On exit it closes writerDone so the main loop can detect it.
func (w *worker) runWriter(ctx context.Context) {
	defer close(w.writerDone)

	lg := log.New("repl:writer").With(log.String("id", w.id))

	for pb := range w.pendingBulkCh {
		start := time.Now()

		size, err := pb.writer.Do(ctx, w.target)
		if err != nil {
			w.writerErr = errors.Wrap(err, "bulk write")
			w.reportError(w.writerErr)

			return
		}

		if size > 0 {
			w.eventsApplied.Add(int64(size))
			metrics.AddEventsApplied(size)
			metrics.AddReplWorkerEventsApplied(w.id, size)
			metrics.ObserveReplWorkerFlushBatchSize(w.id, size)
			metrics.ObserveReplWorkerFlushDuration(w.id, time.Since(start))

			ts := pb.checkpoint
			w.lastTS.Store(&ts)

			lg.With(log.Int64("size", int64(size))).Trace("Flushed batch")
		}
	}
}

// stopWriter closes the bulk queue and waits for the writer goroutine to
// finish. It is safe to call even if the writer has already exited.
func (w *worker) stopWriter() {
	select {
	case <-w.writerDone:
		return // writer already exited
	default:
	}

	close(w.pendingBulkCh)
	<-w.writerDone
}

// restartWriter creates fresh async writer state and launches a new writer
// goroutine. Called after a barrier+resume cycle to resume async writing.
func (w *worker) restartWriter(ctx context.Context) {
	w.writerErr = nil
	w.pendingBulkCh = make(chan *pendingBulk, w.bulkQueueSize)
	w.writerDone = make(chan struct{})
	w.currentBulkWrite = w.newBulkWriter()

	go w.runWriter(ctx)
}

// run is the main loop for the worker. It processes events from eventC,
// batches them, and enqueues bulks to the async writer on buffer full, time interval,
// or barrier signal.
func (w *worker) run(ctx context.Context) {
	defer close(w.done)  // signal Barrier/ReleaseBarrier that this worker has exited
	defer w.stopWriter() // ensure writer goroutine is cleaned up on any exit path

	lg := log.New("repl:worker").With(log.String("id", w.id))
	lg.Debug("Worker started")

	// Start the async writer goroutine.
	go w.runWriter(ctx)

	// Stagger ticker start to spread flushes evenly across the interval.
	// Without this, all workers flush simultaneously causing write contention.
	if w.tickerOffset > 0 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(w.tickerOffset):
		}
	}

	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Enqueue remaining ops before exit (best-effort)
			_ = w.enqueueBulk()

			lg.Debug("Worker stopped (context done)")

			return

		case <-w.writerDone:
			// Writer goroutine died (error already reported via reportError)
			lg.Debug("Worker stopped (writer error)")

			return

		case <-w.barrierReq:
			lg.Trace("Barrier requested")

			// Drain all buffered events routed before the barrier was requested.
			if !w.drainRoutedEvents() {
				w.barrierDone <- errors.Errorf("writer failed during barrier drain")

				return
			}

			// Enqueue remaining ops to the writer goroutine.
			if !w.enqueueBulk() {
				w.barrierDone <- errors.Errorf("writer failed during barrier enqueue")

				return
			}

			// Close bulkQueue and wait for the writer to finish all pending bulks.
			close(w.pendingBulkCh)
			<-w.writerDone

			if w.writerErr != nil {
				w.barrierDone <- w.writerErr

				return
			}

			w.barrierDone <- nil
			lg.Trace("Barrier complete, waiting for resume")

			// Wait for resume signal or context cancellation
			select {
			case <-w.resumeCh:
				lg.Trace("Resumed")
				w.restartWriter(ctx)
			case <-ctx.Done():
				lg.Debug("Worker stopped (context done while waiting for resume)")

				return
			}

		case <-ticker.C:
			if !w.currentBulkWrite.Empty() {
				if !w.enqueueBulk() {
					lg.Debug("Worker stopped (writer error)")

					return
				}
			}

		case event, ok := <-w.routedEventCh:
			if !ok {
				// Channel closed, enqueue and exit (best-effort)
				_ = w.enqueueBulk()

				lg.Debug("Worker stopped (channel closed)")

				return
			}

			err := w.addToCurrentBulk(event)
			if err != nil {
				w.reportError(err)

				return
			}

			if w.currentBulkWrite.Full() {
				if !w.enqueueBulk() {
					lg.Debug("Worker stopped (writer error)")

					return
				}
			}
		}
	}
}

// addToCurrentBulk parses the deferred DML event body from raw BSON and adds
// it to the current bulk write.
// Returns a non-nil error if parsing fails, which is reported to the main loop.
func (w *worker) addToCurrentBulk(event *routedEvent) error {
	parsed, err := parseDMLEvent(event.change.RawData, event.change.OperationType)
	if err != nil {
		return err
	}

	event.change.RawData = nil // release raw bytes for GC

	switch e := parsed.(type) { //nolint:exhaustive
	case InsertEvent:
		w.currentBulkWrite.Insert(event.ns, &e)

	case UpdateEvent:
		w.currentBulkWrite.Update(event.ns, &e)

	case DeleteEvent:
		w.currentBulkWrite.Delete(event.ns, &e)

	case ReplaceEvent:
		w.currentBulkWrite.Replace(event.ns, &e)
	}

	// Track the timestamp of the last event in the batch
	w.pendingTS = event.change.ClusterTime

	return nil
}

// reportError sends an error to the error channel (non-blocking).
func (w *worker) reportError(err error) {
	select {
	case w.errCh <- err:
	default:
		// Error channel full, another error already reported
	}
}

// queueBulk sends a sealed bulk to the writer goroutine. Returns true on
// success, false if the writer has already exited (writerDone closed).
// Blocks when bulkQueue is full until the writer dequeues a bulk or dies.
func (w *worker) queueBulk(pb *pendingBulk) bool {
	select {
	case w.pendingBulkCh <- pb:
		return true
	case <-w.writerDone:
		return false
	}
}

// enqueueBulk seals the current bulk, sends it to the writer goroutine, and
// creates a fresh bulkWriter for the next batch. Returns true on success,
// false if the writer goroutine has died (error already reported).
func (w *worker) enqueueBulk() bool {
	if w.currentBulkWrite.Empty() {
		return true
	}

	pb := &pendingBulk{
		writer:     w.currentBulkWrite,
		checkpoint: w.pendingTS,
	}

	select {
	case w.pendingBulkCh <- pb:
		metrics.SetReplWorkerBulkQueueSize(w.id, len(w.pendingBulkCh))
	case <-w.writerDone:
		return false
	}

	w.currentBulkWrite = w.newBulkWriter()

	return true
}

// drainRoutedEvents reads all buffered events from routedEventCh and adds them
// to the current batch, enqueuing when the batch is full. This must be called
// when handling a barrier request to ensure all events routed before the
// barrier are included in the bulk.
// Returns true on success, false if a parse error or writer failure occurred.
func (w *worker) drainRoutedEvents() bool {
	for {
		select {
		case event := <-w.routedEventCh:
			err := w.addToCurrentBulk(event)
			if err != nil {
				w.reportError(err)

				return false
			}

			if w.currentBulkWrite.Full() {
				if !w.enqueueBulk() {
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

	target *mongo.Client

	errCh  chan error
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// newWorkerPool creates a new worker pool with the specified number of workers.
func newWorkerPool(
	ctx context.Context,
	opts *Options,
	target *mongo.Client,
	useCollectionBulk bool,
	useSimpleCollation bool,
) *workerPool {
	numWorkers := opts.NumWorkers

	poolCtx, cancel := context.WithCancel(ctx)

	p := &workerPool{
		workers:    make([]*worker, numWorkers),
		numWorkers: numWorkers,
		target:     target,
		errCh:      make(chan error, 1),
		cancel:     cancel,
	}

	// Create and start workers
	for i := range numWorkers {
		w := newWorker(i, opts, target, useCollectionBulk, useSimpleCollation, p.errCh)
		w.tickerOffset = time.Duration(i) * opts.WorkerFlushInterval / time.Duration(numWorkers)
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
// For capped collections, events are routed by namespace instead of document
// key to ensure all operations on the same capped collection are processed by
// the same worker, preserving insertion order.
// Uses bson.Raw.Lookup to extract the documentKey bytes directly from the raw
// BSON, avoiding the unmarshal-then-remarshal round-trip of extractDocumentKey.
func (p *workerPool) Route(change *ChangeEvent, ns catalog.Namespace) {
	var workerIdx int
	if ns.Capped {
		workerIdx = hashNamespace(ns, p.numWorkers)
	} else {
		docKey := change.RawData.Lookup("documentKey")
		workerIdx = hashDocumentKey(docKey.Value, p.numWorkers)
	}

	w := p.workers[workerIdx]

	ts := change.ClusterTime
	w.lastRoutedTS.Store(&ts)

	w.routedEventCh <- &routedEvent{
		change: change,
		ns:     ns,
	}

	metrics.SetReplWorkerEventQueueSize(w.id, len(w.routedEventCh))
}

// Barrier flushes all workers and waits for them to complete.
// After Barrier returns nil, all workers are paused and no writes are in-flight.
// Returns a non-nil error if any worker died during (or before) the barrier.
func (p *workerPool) Barrier() error {
	// Signal all workers to flush and pause, skipping dead ones.
	for _, w := range p.workers {
		select {
		case w.barrierReq <- struct{}{}:
		case <-w.done:
			// Worker already exited; will collect error below.
		}
	}

	// Wait for all workers to acknowledge and flush pending ops.
	var firstErr error

	for _, w := range p.workers {
		select {
		case err := <-w.barrierDone:
			if err != nil && firstErr == nil {
				firstErr = err
			}
		case <-w.done:
			// Worker exited without sending on barrierDone (e.g. died
			// before barrier was sent, or context canceled).
			if firstErr == nil {
				firstErr = errors.Errorf("worker %s died", w.id)
			}
		}
	}

	return firstErr
}

// ReleaseBarrier signals all workers to resume processing.
// Dead workers are skipped to avoid blocking on their resumeCh.
func (p *workerPool) ReleaseBarrier() {
	for _, w := range p.workers {
		select {
		case w.resumeCh <- struct{}{}:
		case <-w.done:
			// Worker is dead, skip.
		}
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

// Idle returns true when every worker that has been routed events has
// committed (flushed) up to its last routed timestamp. Workers that were
// never routed an event are skipped.
//
// This differs from SafeCheckpoint which returns the MIN committed timestamp
// across all workers (useful for crash recovery). Idle checks each worker
// against its own last routed timestamp, correctly handling the case where
// workers receive events with different timestamp ranges.
func (p *workerPool) Idle() bool {
	for _, w := range p.workers {
		routed := w.lastRoutedTS.Load()
		if routed == nil {
			continue
		}

		committed := w.lastTS.Load()
		if committed == nil || committed.Before(*routed) {
			return false
		}
	}

	return true
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
	// Flush all workers via barrier before canceling context.
	// Errors are best-effort — we're shutting down.
	_ = p.Barrier()

	p.cancel()

	// Close all worker event channels to signal them to exit
	for _, w := range p.workers {
		close(w.routedEventCh)
	}

	p.ReleaseBarrier()

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

// hashDocumentKey computes a consistent hash of the document key
// and returns a worker index in range [0, numWorkers).
func hashDocumentKey(key []byte, numWorkers int) int {
	h := fnv.New32a()
	h.Write(key)

	return int(h.Sum32()) % numWorkers
}

// hashNamespace computes a consistent hash of the namespace (database.collection)
// and returns a worker index in range [0, numWorkers).
// Used for capped collections to route all events for the same collection to the
// same worker, preserving insertion order.
func hashNamespace(ns catalog.Namespace, numWorkers int) int {
	h := fnv.New32a()
	h.Write([]byte(ns.Database))
	h.Write([]byte("."))
	h.Write([]byte(ns.Collection))

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
