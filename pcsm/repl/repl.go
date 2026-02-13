package repl

import (
	"context"
	"encoding/hex"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/log"
	"github.com/percona/percona-clustersync-mongodb/metrics"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/sel"
	"github.com/percona/percona-clustersync-mongodb/topo"
	"github.com/percona/percona-clustersync-mongodb/util"
)

// Catalog defines the catalog operations required by the repl.
type Catalog interface {
	catalog.BaseCatalog

	UUIDMap() catalog.UUIDMap
	DropDatabase(ctx context.Context, db string) error
	DropIndex(ctx context.Context, db, coll, index string) error
	Rename(ctx context.Context, db, coll, targetDB, targetColl string) error
	ModifyIndex(ctx context.Context, db, coll string, mods *catalog.ModifyIndexOption) error
	ModifyCappedCollection(ctx context.Context, db, coll string, sizeBytes, maxDocs *int64) error
	ModifyView(ctx context.Context, db, view, viewOn string, pipeline any) error
	ModifyChangeStreamPreAndPostImages(ctx context.Context, db, coll string, enabled bool) error
	ModifyValidation(
		ctx context.Context, db, coll string,
		validator *bson.Raw, validationLevel, validationAction *string,
	) error
}

var (
	ErrInvalidateEvent  = errors.New("invalidate")
	ErrOplogHistoryLost = errors.New("oplog history is lost")
)

const advanceTimePseudoEvent = "@tick"

// Options configures the replication behavior.
type Options struct {
	// UseCollectionBulkWrite indicates whether to use collection-level bulk write
	// instead of client bulk write. Default: false (use client bulk write).
	UseCollectionBulkWrite bool
}

// Repl handles replication from a source MongoDB to a target MongoDB.
type Repl struct {
	source *mongo.Client // Source MongoDB client
	target *mongo.Client // Target MongoDB client

	nsFilter sel.NSFilter // Namespace filter
	catalog  Catalog      // Catalog for managing collections and indexes

	options *Options // Replication options

	lastReplicatedOpTime bson.Timestamp

	lock sync.Mutex
	err  error

	eventsRead    atomic.Int64
	eventsApplied int64

	startTime time.Time
	pauseTime time.Time

	pausing bool
	pauseCh chan struct{}
	doneCh  chan struct{}

	pool *workerPool

	useCollectionBulk  bool
	useSimpleCollation bool
}

// Status represents the status of change replication.
type Status struct {
	StartTime time.Time
	PauseTime time.Time

	LastReplicatedOpTime bson.Timestamp // Last applied operation time
	EventsRead           int64          // Number of events read from the source
	EventsApplied        int64          // Number of events applied

	Err error
}

//go:inline
func (s *Status) IsStarted() bool {
	return !s.StartTime.IsZero()
}

//go:inline
func (s *Status) IsRunning() bool {
	return s.IsStarted() && !s.IsPaused()
}

//go:inline
func (s *Status) IsPaused() bool {
	return !s.PauseTime.IsZero()
}

// NewRepl creates a new Repl instance.
func NewRepl(
	source, target *mongo.Client,
	cat Catalog,
	nsFilter sel.NSFilter,
	opts *Options,
) *Repl {
	return &Repl{
		source:   source,
		target:   target,
		nsFilter: nsFilter,
		catalog:  cat,
		options:  opts,
		pauseCh:  make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// Checkpoint represents the checkpoint state for replication recovery.
type Checkpoint struct {
	StartTime            time.Time      `bson:"startTime,omitempty"`
	PauseTime            time.Time      `bson:"pauseTime,omitempty"`
	EventsRead           int64          `bson:"eventsRead,omitempty"`
	EventsApplied        int64          `bson:"events,omitempty"`
	LastReplicatedOpTime bson.Timestamp `bson:"lastOpTS,omitempty"`
	Error                string         `bson:"error,omitempty"`
	UseClientBulkWrite   bool           `bson:"clientBulk,omitempty"`
}

func (r *Repl) Checkpoint() *Checkpoint {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.startTime.IsZero() && r.err == nil {
		return nil
	}

	applied := r.eventsApplied
	if r.pool != nil {
		applied += r.pool.TotalEventsApplied()
	}

	cp := &Checkpoint{
		StartTime:            r.startTime,
		PauseTime:            r.pauseTime,
		EventsRead:           applied,
		EventsApplied:        applied,
		LastReplicatedOpTime: r.lastReplicatedOpTime,
		UseClientBulkWrite:   !r.useCollectionBulk,
	}

	if r.err != nil {
		cp.Error = r.err.Error()
	}

	return cp
}

func (r *Repl) Recover(cp *Checkpoint) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.err != nil {
		return errors.Wrap(r.err, "cannot recover due an existing error")
	}

	if !r.startTime.IsZero() {
		return errors.New("cannot recovery: already used")
	}

	pauseTime := cp.PauseTime
	if pauseTime.IsZero() {
		pauseTime = time.Now()
	}

	r.startTime = cp.StartTime
	r.pauseTime = pauseTime
	r.eventsApplied = cp.EventsApplied
	r.eventsRead.Store(cp.EventsApplied)
	r.lastReplicatedOpTime = cp.LastReplicatedOpTime

	targetVer, err := topo.Version(context.Background(), r.target)
	if err != nil {
		return errors.Wrap(err, "major version")
	}

	r.useCollectionBulk = !cp.UseClientBulkWrite
	r.useSimpleCollation = targetVer.Major() < 8 //nolint:mnd

	if cp.Error != "" {
		r.err = errors.New(cp.Error)
	}

	return nil
}

// Status returns the current replication status.
func (r *Repl) Status() Status {
	r.lock.Lock()
	defer r.lock.Unlock()

	applied := r.eventsApplied
	if r.pool != nil {
		applied += r.pool.TotalEventsApplied()
	}

	return Status{
		LastReplicatedOpTime: r.lastReplicatedOpTime,
		EventsRead:           r.eventsRead.Load(),
		EventsApplied:        applied,

		StartTime: r.startTime,
		PauseTime: r.pauseTime,

		Err: r.err,
	}
}

// ResetError clears any error stored in the Repl instance.
func (r *Repl) ResetError() {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.err = nil
}

func (r *Repl) Done() <-chan struct{} {
	r.lock.Lock()
	defer r.lock.Unlock()

	return r.doneCh
}

// Start begins the replication process from the specified start timestamp.
func (r *Repl) Start(ctx context.Context, startAt bson.Timestamp) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.err != nil {
		return errors.Wrap(r.err, "cannot start due an existing error")
	}

	if !r.startTime.IsZero() {
		return errors.New("already started")
	}

	targetVer, err := topo.Version(ctx, r.target)
	if err != nil {
		return errors.Wrap(err, "major version")
	}

	r.useCollectionBulk = !topo.Support(targetVer).ClientBulkWrite() || r.options.UseCollectionBulkWrite
	r.useSimpleCollation = targetVer.Major() < 8 //nolint:mnd

	if r.useCollectionBulk {
		log.New("repl").Debug("Use collection-level bulk write")
	}

	r.pool = newWorkerPool(context.Background(), 0, r.target, r.useCollectionBulk, r.useSimpleCollation)

	go r.run(options.ChangeStream().SetStartAtOperationTime(&startAt))

	r.startTime = time.Now()

	log.New("repl").With(log.OpTime(startAt.T, startAt.I)).
		Info("Change Replication started")

	return nil
}

// Pause pauses the change replication.
func (r *Repl) Pause(context.Context) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.startTime.IsZero() {
		return errors.New("not running")
	}

	if r.pausing {
		return errors.New("already pausing")
	}

	if !r.pauseTime.IsZero() {
		return errors.New("already paused")
	}

	r.doPause()

	return nil
}

func (r *Repl) doPause() {
	r.pausing = true
	doneCh := r.doneCh

	go func() {
		log.New("repl").Debug("Change Replication is pausing")

		r.pauseCh <- struct{}{}
		<-doneCh

		r.lock.Lock()
		r.pauseTime = time.Now()
		r.pausing = false
		optime := r.lastReplicatedOpTime
		r.lock.Unlock()

		log.New("repl").
			With(log.OpTime(optime.T, optime.I)).
			Info("Change Replication paused")
	}()
}

func (r *Repl) setFailed(err error, msg string) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.err = err

	log.New("repl").Error(err, msg)

	r.doPause()
}

func (r *Repl) Resume(context.Context) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	log.New("repl").Debug("Change Replication is resuming")

	if r.startTime.IsZero() {
		return errors.New("not started")
	}

	if r.pausing {
		return errors.New("pausing")
	}

	if r.pauseTime.IsZero() {
		return errors.New("not paused")
	}

	if r.lastReplicatedOpTime.IsZero() {
		return errors.New("missing optime")
	}

	r.pauseTime = time.Time{}
	r.doneCh = make(chan struct{})
	r.pool = newWorkerPool(context.Background(), 0, r.target, r.useCollectionBulk, r.useSimpleCollation)

	go r.run(options.ChangeStream().SetStartAtOperationTime(&r.lastReplicatedOpTime))

	log.New("repl").With(log.OpTime(r.lastReplicatedOpTime.T, r.lastReplicatedOpTime.I)).
		Info("Change Replication resumed")

	return nil
}

func (r *Repl) watchChangeEvents(
	ctx context.Context,
	streamOptions *options.ChangeStreamOptionsBuilder,
	changeCh chan<- *ChangeEvent,
) error {
	cur, err := r.source.Watch(ctx, mongo.Pipeline{},
		streamOptions.SetShowExpandedEvents(true).
			SetBatchSize(config.ChangeStreamBatchSize).
			SetMaxAwaitTime(config.ChangeStreamAwaitTime))
	if err != nil {
		return errors.Wrap(err, "open")
	}

	defer func() {
		err := util.CtxWithTimeout(context.Background(), config.CloseCursorTimeout, cur.Close)
		if err != nil {
			log.New("repl:watch").Error(err, "Close change stream cursor")
		}
	}()

	// txnOps stores transaction operations during processing.
	// This buffer is reused to minimize memory allocations.
	var txnOps []*ChangeEvent

	for {
		lastEventTS := bson.Timestamp{}
		sourceTS, err := topo.AdvanceClusterTime(ctx, r.source)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}

			log.New("watch").Error(err, "Unable to advance the source cluster time")
		}

		for cur.TryNext(ctx) {
			r.eventsRead.Add(1)
			metrics.IncEventsRead()

			change := &ChangeEvent{}
			err := parseChangeEvent(cur.Current, change)
			if err != nil {
				return err
			}

			if !change.IsTransaction() {
				changeCh <- change
				lastEventTS = change.ClusterTime

				continue
			}

			txn0 := change // the first transaction operation
			for txn0 != nil {
				for cur.TryNext(ctx) {
					r.eventsRead.Add(1)
					metrics.IncEventsRead()

					change = &ChangeEvent{}
					err := parseChangeEvent(cur.Current, change)
					if err != nil {
						return err
					}

					if txn0.IsSameTransaction(&change.EventHeader) {
						txnOps = append(txnOps, change)

						continue
					}

					// send the entire transaction for replication
					changeCh <- txn0
					for _, txn := range txnOps {
						changeCh <- txn
					}
					clear(txnOps)
					txnOps = txnOps[:0]

					if !change.IsTransaction() {
						changeCh <- change
						txn0 = nil // no more transaction

						break // return to non-transactional processing
					}

					txn0 = change // process the new transaction
				}

				err := cur.Err()
				if err != nil || cur.ID() == 0 {
					return errors.Wrap(err, "cursor")
				}

				if txn0 == nil {
					continue
				}

				// no event available. the entire transaction is received
				changeCh <- txn0
				for _, txn := range txnOps {
					changeCh <- txn
				}
				clear(txnOps)
				txnOps = txnOps[:0]
				txn0 = nil // return to non-transactional processing
			}
		}

		err = cur.Err()
		if err != nil || cur.ID() == 0 {
			return errors.Wrap(err, "cursor")
		}

		// no event available yet. progress pcsm time
		if sourceTS.After(lastEventTS) {
			changeCh <- &ChangeEvent{
				EventHeader: EventHeader{
					OperationType: advanceTimePseudoEvent,
					ClusterTime:   sourceTS,
				},
			}
		}
	}
}

func (r *Repl) run(opts *options.ChangeStreamOptionsBuilder) {
	defer func() {
		r.lock.Lock()
		r.eventsApplied += r.pool.TotalEventsApplied()
		r.lock.Unlock()

		r.pool.Stop()

		close(r.doneCh)
	}()

	ctx := context.Background()
	changeEventCh := make(chan *ChangeEvent, config.ReplQueueSize)

	go func() {
		defer close(changeEventCh)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func() {
			<-r.pauseCh
			cancel()
		}()

		err := r.watchChangeEvents(ctx, opts, changeEventCh)
		if err != nil && !errors.Is(err, context.Canceled) {
			if topo.IsChangeStreamHistoryLost(err) || topo.IsCappedPositionLost(err) {
				err = ErrOplogHistoryLost
			}

			r.setFailed(err, "Watch change stream")
		}
	}()

	uuidMap := r.catalog.UUIDMap()

	lg := log.New("repl")

	// lastRoutedTS tracks the ClusterTime of the last event routed to the pool.
	// Used with SafeCheckpoint() to determine if the pool is idle (equivalent of
	// the old bulkWrite.Empty() check).
	var lastRoutedTS bson.Timestamp

	// cpTicker triggers periodic advancement of lastReplicatedOpTime based on
	// the worker pool's SafeCheckpoint. Under sustained DML load, @tick
	// pseudo-events may be delayed (queued behind DML in changeCh), and
	// poolIdle() returns false because workers haven't caught up yet. Without
	// this ticker, lastReplicatedOpTime stalls and reported lag grows linearly
	// even though workers are making progress.
	cpTicker := time.NewTicker(time.Second)
	defer cpTicker.Stop()

	// pending holds an event read ahead during transaction collection that
	// needs to be processed in the next iteration.
	var pending *ChangeEvent

	for {
		var change *ChangeEvent

		if pending != nil {
			change = pending
			pending = nil
		} else {
			var ok bool

			select {
			case change, ok = <-changeEventCh:
				if !ok {
					return
				}
			case err := <-r.pool.Err():
				r.setFailed(err, "Worker error")

				return
			}
		}

		if change.OperationType == advanceTimePseudoEvent {
			lg.With(log.OpTime(change.ClusterTime.T, change.ClusterTime.I)).Trace("tick")

			r.lock.Lock()
			r.lastReplicatedOpTime = change.ClusterTime
			r.lock.Unlock()

			continue
		}

		if change.Namespace.Database == config.PCSMDatabase {
			if r.poolIdle(lastRoutedTS) {
				r.lock.Lock()
				r.lastReplicatedOpTime = change.ClusterTime
				r.eventsApplied++
				r.lock.Unlock()

				metrics.AddEventsApplied(1)
			} else {
				r.tryAdvanceOpTime(cpTicker)
			}

			continue
		}

		if !r.nsFilter(change.Namespace.Database, change.Namespace.Collection) {
			if r.poolIdle(lastRoutedTS) {
				r.lock.Lock()
				r.lastReplicatedOpTime = change.ClusterTime
				r.eventsApplied++
				r.lock.Unlock()

				metrics.AddEventsApplied(1)
			} else {
				r.tryAdvanceOpTime(cpTicker)
			}

			continue
		}

		switch change.OperationType { //nolint:exhaustive
		case Insert, Update, Delete, Replace:
			if change.IsTransaction() {
				pending = r.handleTransaction(ctx, change, changeEventCh, uuidMap)
				lastRoutedTS = bson.Timestamp{} // barrier flushed everything
			} else {
				ns := findNamespaceByUUID(uuidMap, change)
				r.pool.Route(change, ns)
				lastRoutedTS = change.ClusterTime

				r.tryAdvanceOpTime(cpTicker)
			}

		default:
			r.pool.Barrier()

			err := r.applyDDLChange(ctx, change)
			if err != nil {
				r.pool.ReleaseBarrier()
				r.setFailed(err, "Apply change")

				return
			}

			r.lock.Lock()
			r.lastReplicatedOpTime = change.ClusterTime
			r.eventsApplied++
			r.lock.Unlock()

			metrics.AddEventsApplied(1)

			switch change.OperationType { //nolint:exhaustive
			case Create, Rename, Drop, DropDatabase, ShardCollection:
				uuidMap = r.catalog.UUIDMap()
			}

			lastRoutedTS = bson.Timestamp{} // barrier flushed everything
			r.pool.ReleaseBarrier()
		}
	}
}

// handleTransaction collects all events belonging to the same transaction,
// applies them as a single ordered bulk write, and returns the first event
// that is not part of the transaction (or nil if the channel was closed).
func (r *Repl) handleTransaction(
	ctx context.Context,
	first *ChangeEvent,
	changeCh <-chan *ChangeEvent,
	uuidMap catalog.UUIDMap,
) *ChangeEvent {
	r.pool.Barrier()

	events := []*routedEvent{{
		change: first,
		ns:     findNamespaceByUUID(uuidMap, first),
	}}

	var overflow *ChangeEvent

	for next := range changeCh {
		if !first.IsSameTransaction(&next.EventHeader) {
			overflow = next

			break
		}

		events = append(events, &routedEvent{
			change: next,
			ns:     findNamespaceByUUID(uuidMap, next),
		})
	}

	err := applyTransaction(ctx, r.target, events, r.useCollectionBulk, r.useSimpleCollation)
	if err != nil {
		r.pool.ReleaseBarrier()
		r.setFailed(err, "Apply transaction")

		return nil
	}

	r.lock.Lock()
	r.lastReplicatedOpTime = first.ClusterTime
	r.eventsApplied += int64(len(events))
	r.lock.Unlock()

	metrics.AddEventsApplied(len(events))

	r.pool.ReleaseBarrier()

	return overflow
}

// tryAdvanceOpTime does a non-blocking check of cpTicker and, when fired,
// advances lastReplicatedOpTime to the worker pool's SafeCheckpoint. This
// ensures lag tracking stays current during sustained DML when @tick
// pseudo-events and poolIdle updates are insufficient.
func (r *Repl) tryAdvanceOpTime(cpTicker *time.Ticker) {
	select {
	case <-cpTicker.C:
		cp := r.pool.SafeCheckpoint()
		if cp.IsZero() {
			return
		}

		r.lock.Lock()
		if cp.After(r.lastReplicatedOpTime) {
			r.lastReplicatedOpTime = cp
		}
		r.lock.Unlock()
	default:
	}
}

// poolIdle returns true when no events are pending in the worker pool.
// This is the equivalent of the old bulkWrite.Empty() check: it compares
// the last routed timestamp against the minimum committed timestamp across
// all workers (SafeCheckpoint). When SafeCheckpoint >= lastRoutedTS, all
// routed events have been committed and the pool is idle.
func (r *Repl) poolIdle(lastRoutedTS bson.Timestamp) bool {
	if lastRoutedTS.IsZero() {
		return true
	}

	cp := r.pool.SafeCheckpoint()

	return !cp.IsZero() && !lastRoutedTS.After(cp)
}

//go:inline
func findNamespaceByUUID(uuidMap catalog.UUIDMap, change *ChangeEvent) catalog.Namespace {
	if change.CollectionUUID == nil {
		return change.Namespace
	}

	ns, ok := uuidMap[hex.EncodeToString(change.CollectionUUID.Data)]
	if !ok {
		return change.Namespace
	}

	return ns
}

// applyDDLChange applies a schema change to the target MongoDB.
func (r *Repl) applyDDLChange(ctx context.Context, change *ChangeEvent) error {
	lg := loggerForEvent(change)
	ctx = lg.WithContext(ctx)

	var err error

	switch change.OperationType { //nolint:exhaustive
	case Create:
		event := change.Event.(CreateEvent) //nolint:forcetypeassert
		if event.IsTimeseries() {
			lg.Warn("Timeseries is not supported. skipping")

			return nil
		}

		err = r.catalog.DropCollection(ctx,
			change.Namespace.Database,
			change.Namespace.Collection)
		if err != nil {
			err = errors.Wrap(err, "drop before create")

			break
		}

		err = r.catalog.CreateCollection(ctx,
			change.Namespace.Database,
			change.Namespace.Collection,
			&event.OperationDescription)
		if err != nil {
			err = errors.Wrap(err, "create")

			break
		}

		r.catalog.SetCollectionUUID(ctx,
			change.Namespace.Database,
			change.Namespace.Collection,
			change.CollectionUUID)

		lg.Infof("Collection %q has been created", change.Namespace)

	case Drop:
		err = r.catalog.DropCollection(ctx,
			change.Namespace.Database,
			change.Namespace.Collection)
		if err != nil {
			break
		}

		lg.Infof("Collection %q has been dropped", change.Namespace)

	case DropDatabase:
		err = r.catalog.DropDatabase(ctx, change.Namespace.Database)
		if err != nil {
			break
		}

		lg.Infof("Database %q has been dropped", change.Namespace)

	case CreateIndexes:
		event := change.Event.(CreateIndexesEvent) //nolint:forcetypeassert
		err = r.catalog.CreateIndexes(ctx,
			change.Namespace.Database,
			change.Namespace.Collection,
			event.OperationDescription.Indexes)

	case DropIndexes:
		event := change.Event.(DropIndexesEvent) //nolint:forcetypeassert
		for _, index := range event.OperationDescription.Indexes {
			err = r.catalog.DropIndex(ctx,
				change.Namespace.Database,
				change.Namespace.Collection,
				index.Name)
			if err != nil {
				lg.Error(err, "Drop index "+index.Name)
			}
		}

	case Modify:
		event := change.Event.(ModifyEvent) //nolint:forcetypeassert
		r.doModify(ctx, change.Namespace, &event)

	case Rename:
		event := change.Event.(RenameEvent) //nolint:forcetypeassert
		err = r.catalog.Rename(ctx,
			change.Namespace.Database,
			change.Namespace.Collection,
			event.OperationDescription.To.Database,
			event.OperationDescription.To.Collection)
		if err != nil {
			break
		}

		lg.Infof("Collection %q has been renamed to %q",
			change.Namespace, event.OperationDescription.To)

	case Invalidate:
		lg.Error(ErrInvalidateEvent, "")

		return ErrInvalidateEvent

	case ShardCollection:
		event := change.Event.(ShardCollectionEvent) //nolint:forcetypeassert
		err = r.catalog.ShardCollection(ctx,
			change.Namespace.Database,
			change.Namespace.Collection,
			event.OperationDescription.ShardKey,
			event.OperationDescription.Unique)

		lg.Infof("Collection %q has been sharded", change.Namespace)

	case ReshardCollection:
		fallthrough
	case RefineCollectionShardKey:
		fallthrough

	default:
		lg.Warn("Unsupported type: " + string(change.OperationType))

		return nil
	}

	if err != nil {
		return errors.Wrap(err, string(change.OperationType))
	}

	return nil
}

func (r *Repl) doModify(ctx context.Context, ns catalog.Namespace, event *ModifyEvent) {
	opts := event.OperationDescription

	if len(opts.Unknown) != 0 {
		log.Ctx(ctx).Warn("Unknown modify options")
	}

	if opts.ChangeStreamPreAndPostImages != nil {
		err := r.catalog.ModifyChangeStreamPreAndPostImages(ctx,
			ns.Database, ns.Collection, opts.ChangeStreamPreAndPostImages.Enabled)
		if err != nil {
			log.Ctx(ctx).Error(err, "Modify changeStreamPreAndPostImages")
		}
	}

	if opts.Validator != nil || opts.ValidationLevel != nil || opts.ValidationAction != nil {
		err := r.catalog.ModifyValidation(ctx,
			ns.Database, ns.Collection, opts.Validator, opts.ValidationLevel, opts.ValidationAction)
		if err != nil {
			log.Ctx(ctx).Error(err, "Modify validation")
		}
	}

	switch {
	case opts.Index != nil:
		err := r.catalog.ModifyIndex(ctx, ns.Database, ns.Collection, opts.Index)
		if err != nil {
			log.Ctx(ctx).Error(err, "Modify index: "+opts.Index.Name)

			return
		}

	case opts.CappedSize != nil || opts.CappedMax != nil:
		err := r.catalog.ModifyCappedCollection(ctx,
			ns.Database, ns.Collection, opts.CappedSize, opts.CappedMax)
		if err != nil {
			log.Ctx(ctx).Error(err, "Resize capped collection")

			return
		}

	case opts.ViewOn != "":
		if strings.HasPrefix(opts.ViewOn, catalog.TimeseriesPrefix) {
			log.Ctx(ctx).Warn("Timeseries is not supported. skipping")

			return
		}

		err := r.catalog.ModifyView(ctx, ns.Database, ns.Collection, opts.ViewOn, opts.Pipeline)
		if err != nil {
			log.Ctx(ctx).Error(err, "Modify view")

			return
		}

	case opts.ExpireAfterSeconds != nil:
		log.Ctx(ctx).Warn("Collection TTL modification is not supported")

	case opts.ChangeStreamPreAndPostImages != nil:
		log.Ctx(ctx).Warn("changeStreamPreAndPostImages is not supported")
	}
}

func loggerForEvent(change *ChangeEvent) log.Logger {
	return log.New("repl").With(
		log.OpTime(change.ClusterTime.T, change.ClusterTime.I),
		log.Op(string(change.OperationType)),
		log.NS(change.Namespace.Database, change.Namespace.Collection))
}
