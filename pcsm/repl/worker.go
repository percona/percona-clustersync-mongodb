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
	id string

	routedEventCh chan *routedEvent
	bulkWrite     bulkWriter
	lastTS        atomic.Pointer[bson.Timestamp] // last committed timestamp
	pendingTS     bson.Timestamp                 // timestamp of last event in current batch
	tickerOffset  time.Duration                  // stagger delay before starting the flush ticker

	target *mongo.Client

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
	var bw bulkWriter
	if useCollectionBulk {
		bw = newCollectionBulkWriter(opts.BulkOpsSize, useSimpleCollation)
	} else {
		bw = newClientBulkWriter(opts.BulkOpsSize, useSimpleCollation)
	}

	return &worker{
		id:            strconv.Itoa(id),
		routedEventCh: make(chan *routedEvent, opts.WorkerQueueSize),
		bulkWrite:     bw,
		target:        target,
		barrierReq:    make(chan struct{}),
		barrierDone:   make(chan error),
		resumeCh:      make(chan struct{}),
		done:          make(chan struct{}),
		errCh:         errC,
	}
}

// run is the main loop for the worker. It processes events from eventC,
// batches them, and flushes on buffer full, time interval, or barrier signal.
func (w *worker) run(ctx context.Context) {
	defer close(w.done) // signal Barrier/ReleaseBarrier that this worker has exited

	lg := log.New("repl:worker").With(log.String("id", w.id))
	lg.Debug("Worker started")

	// Stagger ticker start to spread flushes evenly across the interval.
	// Without this, all workers flush simultaneously causing write contention.
	if w.tickerOffset > 0 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(w.tickerOffset):
		}
	}

	ticker := time.NewTicker(config.WorkerFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Flush remaining ops before exit (best-effort)
			if !w.bulkWrite.Empty() {
				_ = w.flush(ctx, lg)
			}

			lg.Debug("Worker stopped (context done)")

			return

		case <-w.barrierReq:
			lg.Trace("Barrier requested")

			// Drain all buffered events routed before the barrier was requested.
			err := w.drainRoutedEvents(ctx, lg)
			if err != nil {
				w.barrierDone <- err

				return
			}

			// Flush remaining ops and signal done.
			if !w.bulkWrite.Empty() {
				err = w.flush(ctx, lg)
				if err != nil {
					w.barrierDone <- err
					lg.Debug("Worker stopped (flush error)")

					return
				}
			}

			w.barrierDone <- nil
			lg.Trace("Barrier complete, waiting for resume")

			// Wait for resume signal or context cancellation
			select {
			case <-w.resumeCh:
				lg.Trace("Resumed")
			case <-ctx.Done():
				lg.Debug("Worker stopped (context done while waiting for resume)")

				return
			}

		case <-ticker.C:
			if !w.bulkWrite.Empty() {
				flushErr := w.flush(ctx, lg)
				if flushErr != nil {
					lg.Debug("Worker stopped (flush error)")

					return
				}
			}

		case event, ok := <-w.routedEventCh:
			if !ok {
				// Channel closed, flush and exit (best-effort)
				if !w.bulkWrite.Empty() {
					_ = w.flush(ctx, lg)
				}

				lg.Debug("Worker stopped (channel closed)")

				return
			}

			err := w.addToBatch(event)
			if err != nil {
				w.reportError(err)

				return
			}

			if w.bulkWrite.Full() {
				flushErr := w.flush(ctx, lg)
				if flushErr != nil {
					lg.Debug("Worker stopped (flush error)")

					return
				}
			}
		}
	}
}

// addToBatch parses the deferred DML event body from raw BSON and adds
// it to the bulk write buffer. Parsing is done here (in the worker) rather
// than in the single-threaded reader to parallelize the expensive
// deserialization of fullDocument across all workers.
func (w *worker) addToBatch(event *routedEvent) error {
	parsed, err := parseDMLEvent(event.change.RawData, event.change.OperationType)
	if err != nil {
		return err
	}

	event.change.RawData = nil // release raw bytes for GC

	switch e := parsed.(type) { //nolint:exhaustive
	case InsertEvent:
		w.bulkWrite.Insert(event.ns, &e)

	case UpdateEvent:
		w.bulkWrite.Update(event.ns, &e)

	case DeleteEvent:
		w.bulkWrite.Delete(event.ns, &e)

	case ReplaceEvent:
		w.bulkWrite.Replace(event.ns, &e)
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

// flush executes the bulk write and updates metrics.
// Returns a non-nil error if the bulk write failed and the worker should stop.
// The error is also sent to errCh via reportError so the dispatcher can detect it.
func (w *worker) flush(ctx context.Context, lg log.Logger) error {
	start := time.Now()

	size, err := w.bulkWrite.Do(ctx, w.target)
	if err != nil {
		err = errors.Wrap(err, "bulk write")
		w.reportError(err)

		return err
	}

	if size > 0 {
		w.eventsApplied.Add(int64(size))
		metrics.AddEventsApplied(size)
		metrics.AddReplWorkerEventsApplied(w.id, size)
		metrics.ObserveReplWorkerFlushBatchSize(w.id, size)
		metrics.ObserveReplWorkerFlushDuration(w.id, time.Since(start))

		// Update the last committed timestamp
		ts := w.pendingTS
		w.lastTS.Store(&ts)

		lg.With(log.Int64("size", int64(size))).Trace("Flushed batch")
	}

	return nil
}

// drainRoutedEvents reads all buffered events from routedEventCh and adds them
// to the current batch, flushing when the batch is full. This must be called
// when handling a barrier request to ensure all events routed before the
// barrier are included in the bulk.
// Returns a non-nil error if a parse or flush error occurred and the worker should stop.
func (w *worker) drainRoutedEvents(ctx context.Context, lg log.Logger) error {
	for {
		select {
		case event := <-w.routedEventCh:
			err := w.addToBatch(event)
			if err != nil {
				w.reportError(err)

				return err
			}

			if w.bulkWrite.Full() {
				flushErr := w.flush(ctx, lg)
				if flushErr != nil {
					return flushErr
				}
			}
		default:
			return nil
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
		w := newWorker(i, opts, target, useCollectionBulk, useSimpleCollation, p.errCh)
		w.tickerOffset = time.Duration(i) * config.WorkerFlushInterval / time.Duration(numWorkers)
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
