package repl

import (
	"bytes"
	"context"
	"runtime"
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
	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/metrics"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/sel"
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

var ErrOplogHistoryLost = errors.New("oplog history is lost")

const advanceTimePseudoEvent = "@tick"

// movePrimarySentinelNS is the sentinel namespace used to signal a movePrimary-induced
// change stream invalidation on <8 mongos sources. See PCSM-249 / SERVER-120349.
var movePrimarySentinelNS = catalog.Namespace{ //nolint:gochecknoglobals
	Database:   "::movePrimary::",
	Collection: "sentinel",
}

// Options configures the replication behavior.
type Options struct {
	// UseCollectionBulkWrite indicates whether to use collection-level bulk write
	// instead of client bulk write. Default: false (use client bulk write).
	UseCollectionBulkWrite bool
	// NumWorkers is the number of replication workers.
	// 0 means auto (defaults to runtime.NumCPU()).
	NumWorkers int
	// ChangeStreamBatchSize is the batch size for MongoDB change streams.
	// 0 means auto (defaults to config.ChangeStreamBatchSize).
	ChangeStreamBatchSize int
	// EventQueueSize is the buffer size of the channel between the change stream
	// reader and the dispatcher.
	// 0 means auto (defaults to config.ReplQueueSize).
	EventQueueSize int
	// WorkerQueueSize is the per-worker routed event channel buffer size.
	// 0 means auto (defaults to config.ReplQueueSize).
	WorkerQueueSize int
	// BulkOpsSize is the maximum number of operations per bulk write.
	// 0 means auto (defaults to config.BulkOpsSize).
	BulkOpsSize int
	// WorkerFlushInterval is the maximum interval between worker bulk write flushes.
	// 0 means auto (defaults to config.WorkerFlushInterval).
	WorkerFlushInterval time.Duration
	// WorkerBulkQueueSize is the number of pending bulks per worker for async writes.
	// 0 means auto (defaults to config.WorkerBulkQueueSize).
	WorkerBulkQueueSize int
}

func (o *Options) applyDefaults() {
	if o.NumWorkers <= 0 {
		o.NumWorkers = runtime.NumCPU()
	}

	if o.ChangeStreamBatchSize <= 0 {
		o.ChangeStreamBatchSize = config.ChangeStreamBatchSize
	}

	if o.EventQueueSize <= 0 {
		o.EventQueueSize = config.ReplQueueSize
	}

	if o.WorkerQueueSize <= 0 {
		o.WorkerQueueSize = config.ReplQueueSize
	}

	if o.BulkOpsSize <= 0 {
		o.BulkOpsSize = config.BulkOpsSize
	}

	if o.WorkerFlushInterval <= 0 {
		o.WorkerFlushInterval = config.WorkerFlushInterval
	}

	if o.WorkerBulkQueueSize <= 0 {
		o.WorkerBulkQueueSize = config.WorkerBulkQueueSize
	}
}

// Repl handles replication from a source MongoDB to a target MongoDB.
type Repl struct {
	source *mongo.Client // Source MongoDB client
	target *mongo.Client // Target MongoDB client

	getCollectionShardingInfo func(context.Context, *mongo.Client, string, string) (*mdb.ShardingInfo, error)

	sourceVer mdb.ServerVersion

	nsFilter sel.NSFilter // Namespace filter
	catalog  Catalog      // Catalog for managing collections and indexes

	options *Options // Replication options

	lastReplicatedOpTime bson.Timestamp
	checkpointOpTime     bson.Timestamp // applied-only optime, never tick-driven

	lock sync.Mutex
	err  error

	eventsRead    atomic.Int64
	eventsApplied int64

	startTime time.Time
	pauseTime time.Time

	pausing bool
	pauseCh chan struct{}
	doneCh  chan struct{}

	pool workerBarrierPool

	movePrimaryMarker movePrimaryMarker
	sourceIsMongos    bool

	useCollectionBulk  bool
	useSimpleCollation bool
}

// Status represents the status of change replication.
type Status struct {
	StartTime time.Time
	PauseTime time.Time

	LastReplicatedOpTime bson.Timestamp // Last applied operation time
	CheckpointOpTime     bson.Timestamp // Applied-only optime, safe for resume
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
	sourceVer mdb.ServerVersion,
	sourceIsMongos bool,
) *Repl {
	opts.applyDefaults()

	lg := log.New("repl")
	lg.Infof("Config: NumWorkers: %d", opts.NumWorkers)
	lg.Infof("Config: UseCollectionBulkWrite: %t", opts.UseCollectionBulkWrite)
	lg.Infof("Config: ChangeStreamBatchSize: %d", opts.ChangeStreamBatchSize)
	lg.Infof("Config: EventQueueSize: %d", opts.EventQueueSize)
	lg.Infof("Config: WorkerQueueSize: %d", opts.WorkerQueueSize)
	lg.Infof("Config: BulkOpsSize: %d", opts.BulkOpsSize)
	lg.Infof("Config: WorkerFlushInterval: %s", opts.WorkerFlushInterval)
	lg.Infof("Config: WorkerBulkQueueSize: %d", opts.WorkerBulkQueueSize)

	return &Repl{
		source:                    source,
		target:                    target,
		getCollectionShardingInfo: mdb.GetCollectionShardingInfo,
		sourceVer:                 sourceVer,
		nsFilter:                  nsFilter,
		catalog:                   cat,
		options:                   opts,
		pauseCh:                   make(chan struct{}),
		doneCh:                    make(chan struct{}),
		movePrimaryMarker:         movePrimaryMarker{ns: make(map[string]struct{})},
		sourceIsMongos:            sourceIsMongos,
	}
}

// sourceIsPre8AndMongos returns true when the source is a mongos (sharded) and
// runs MongoDB 6.x or 7.x (pre-8). Used to gate invalidate-stream handling for
// movePrimary on older sharded topologies.
func (r *Repl) sourceIsPre8AndMongos() bool {
	return r.sourceIsMongos && r.sourceVer.Major() < 8
}

// Checkpoint represents the checkpoint state for replication recovery.
type Checkpoint struct {
	StartTime            time.Time      `bson:"startTime,omitempty"`
	PauseTime            time.Time      `bson:"pauseTime,omitempty"`
	EventsRead           int64          `bson:"eventsRead,omitempty"`
	EventsApplied        int64          `bson:"events,omitempty"`
	LastReplicatedOpTime bson.Timestamp `bson:"lastOpTS,omitempty"`
	CheckpointOpTime     bson.Timestamp `bson:"checkpointOpTS,omitempty"`
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
		CheckpointOpTime:     r.checkpointOpTime,
		UseClientBulkWrite:   !r.useCollectionBulk,
	}

	if r.err != nil {
		cp.Error = r.err.Error()
	}

	return cp
}

func (r *Repl) Recover(ctx context.Context, cp *Checkpoint) error {
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
	r.checkpointOpTime = cp.CheckpointOpTime
	// Fall back to the legacy single-optime field for checkpoints persisted
	// before checkpointOpTS existed. Pre-split, lastReplicatedOpTime was the
	// resume frontier, so using it here preserves prior behavior on upgrade.
	if r.checkpointOpTime.IsZero() {
		r.checkpointOpTime = r.lastReplicatedOpTime
	}

	targetVer, err := mdb.Version(ctx, r.target)
	if err != nil {
		return errors.Wrap(err, "target version")
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
		CheckpointOpTime:     r.checkpointOpTime,
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

	targetVer, err := mdb.Version(ctx, r.target)
	if err != nil {
		return errors.Wrap(err, "target version")
	}

	r.useCollectionBulk = !mdb.Support(targetVer).ClientBulkWrite() || r.options.UseCollectionBulkWrite
	r.useSimpleCollation = targetVer.Major() < 8 //nolint:mnd

	if r.useCollectionBulk {
		log.New("repl").Debug("Use collection-level bulk write")
	}

	r.pool = newWorkerPool(context.Background(), r.options, r.target,
		r.useCollectionBulk, r.useSimpleCollation, r.catalog.UUIDMap)

	r.checkpointOpTime = startAt

	go r.run(ctx, options.ChangeStream().SetStartAtOperationTime(&startAt))

	r.startTime = time.Now()

	log.New("repl").With(log.OpTime(startAt.T, startAt.I)).
		Info("Change Replication started")

	return nil
}

// Pause pauses the change replication.
func (r *Repl) Pause(_ context.Context) error {
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

func (r *Repl) Resume(ctx context.Context) error {
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

	if r.checkpointOpTime.IsZero() {
		return errors.New("missing optime")
	}

	r.pauseTime = time.Time{}
	r.doneCh = make(chan struct{})
	r.pool = newWorkerPool(context.Background(), r.options, r.target,
		r.useCollectionBulk, r.useSimpleCollation, r.catalog.UUIDMap)

	go r.run(ctx, options.ChangeStream().SetStartAtOperationTime(&r.checkpointOpTime))

	log.New("repl").With(log.OpTime(r.checkpointOpTime.T, r.checkpointOpTime.I)).
		Info("Change Replication resumed")

	return nil
}

func (r *Repl) watchWithRetry(
	ctx context.Context,
	opts *options.ChangeStreamOptionsBuilder,
	changeCh chan<- *ChangeEvent,
) error {
	currentOpts := opts
	var startAfter bson.Raw

	return mdb.RetryWithBackoff(ctx, func() error { //nolint:wrapcheck
		err := r.watchChangeEvents(ctx, currentOpts, changeCh)
		if err == nil {
			return nil
		}

		var invalidateErr changeStreamInvalidateError
		if r.sourceIsMongos && errors.As(err, &invalidateErr) && len(invalidateErr.token) > 0 {
			startAfter = append(startAfter[:0], invalidateErr.token...)
			currentOpts = options.ChangeStream().SetStartAfter(startAfter)
			log.New("repl:watch").With(
				log.OpTime(invalidateErr.clusterTime.T, invalidateErr.clusterTime.I),
			).Warn("change stream invalidated; reconnecting with startAfter")

			return err
		}

		if len(startAfter) > 0 {
			currentOpts = options.ChangeStream().SetStartAfter(startAfter)

			return err
		}

		r.lock.Lock()
		lastOpTime := r.checkpointOpTime
		r.lock.Unlock()

		currentOpts = options.ChangeStream().SetStartAtOperationTime(&lastOpTime)

		return err
	}, isChangeStreamUnrecoverable,
		mdb.DefaultRetryInterval, maxWatchDelay, 0,
	)
}

type changeStreamInvalidateError struct {
	token       bson.Raw
	clusterTime bson.Timestamp
}

func (e changeStreamInvalidateError) Error() string {
	return "change stream invalidated"
}

func isChangeStreamUnrecoverable(err error) bool {
	return mdb.IsChangeStreamHistoryLost(err) || mdb.IsCappedPositionLost(err)
}

const (
	maxWatchDelay      = 30 * time.Second
	maxWriteRetryDelay = 30 * time.Second
)

func isNonTransient(err error) bool {
	return !mdb.IsTransient(err)
}

func (r *Repl) watchChangeEvents(
	ctx context.Context,
	streamOptions *options.ChangeStreamOptionsBuilder,
	changeCh chan<- *ChangeEvent,
) error {
	cur, err := r.source.Watch(ctx, mongo.Pipeline{},
		streamOptions.SetShowExpandedEvents(true).
			SetBatchSize(int32(r.options.ChangeStreamBatchSize)). //nolint:gosec
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

	var invalidateErr *changeStreamInvalidateError

	for {
		lastEventTS := bson.Timestamp{}
		hasEvents := false

		for cur.TryNext(ctx) {
			hasEvents = true
			r.eventsRead.Add(1)
			metrics.IncEventsRead()

			change := &ChangeEvent{}
			change.RawData = append(change.RawData, cur.Current...)

			err := parseEventHeader(change.RawData, &change.EventHeader)
			if err != nil {
				return err
			}

			ts := change.ClusterTime
			changeCh <- change
			lastEventTS = ts

			if change.OperationType == Invalidate {
				invalidateErr = &changeStreamInvalidateError{
					token:       append(bson.Raw(nil), change.ID...),
					clusterTime: change.ClusterTime,
				}
			}
		}

		err = changeStreamCursorError(invalidateErr, cur.Err(), cur.ID())
		if err != nil {
			return err
		}

		// Only advance cluster time when the cursor had no events (truly idle).
		// Under sustained load, tryAdvanceOpTime + Checkpoint keep
		// lastReplicatedOpTime current without the appendOplogNote overhead.
		if !hasEvents {
			sourceTS, err := mdb.AdvanceClusterTime(ctx, r.source)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}

				log.New("watch").Error(err, "Unable to advance the source cluster time")
			}

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
}

func changeStreamCursorError(invalidateErr *changeStreamInvalidateError, cursorErr error, cursorID int64) error {
	if invalidateErr != nil {
		return *invalidateErr
	}

	if cursorErr != nil {
		return errors.Wrap(cursorErr, "cursor")
	}

	if cursorID == 0 {
		return errors.New("change stream cursor closed by server")
	}

	return nil
}

func (r *Repl) run(ctx context.Context, opts *options.ChangeStreamOptionsBuilder) {
	defer func() {
		r.lock.Lock()
		r.eventsApplied += r.pool.TotalEventsApplied()

		r.pool.Stop()
		r.advanceCheckpoint(r.pool.Checkpoint())

		r.pool = nil
		r.lock.Unlock()

		close(r.doneCh)
	}()

	changeEventCh := make(chan *ChangeEvent, r.options.EventQueueSize)

	go func() {
		defer close(changeEventCh)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func() {
			<-r.pauseCh
			cancel()
		}()

		err := r.watchWithRetry(ctx, opts, changeEventCh)
		if err != nil && !errors.Is(err, context.Canceled) {
			if mdb.IsChangeStreamHistoryLost(err) || mdb.IsCappedPositionLost(err) {
				err = ErrOplogHistoryLost
			}

			r.setFailed(err, "Watch change stream")
		}
	}()

	lg := log.New("repl")

	// lastRoutedTS tracks the ClusterTime of the last event routed to the pool.
	// Used with Checkpoint() to determine if the pool is idle.
	var lastRoutedTS bson.Timestamp

	// cpTicker triggers periodic advancement of lastReplicatedOpTime based on
	// the worker pool's Checkpoint. Under sustained DML load, @tick
	// pseudo-events may be delayed (queued behind DML in changeEventCh), and
	// poolIdle() returns false because workers haven't caught up yet. Without
	// this ticker, lastReplicatedOpTime stalls and reported lag grows linearly
	// even though workers are making progress.
	cpTicker := time.NewTicker(500 * time.Millisecond) //nolint:mnd
	defer cpTicker.Stop()

	for {
		var change *ChangeEvent

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

		metrics.SetReplEventQueueSize(len(changeEventCh))

		if change.OperationType == advanceTimePseudoEvent {
			lg.With(log.OpTime(change.ClusterTime.T, change.ClusterTime.I)).Trace("tick")

			r.lock.Lock()
			r.advanceReportedOpTime(change.ClusterTime)
			r.lock.Unlock()

			continue
		}

		if r.isReplay(change) {
			lg.With(
				log.NS(change.Namespace.Database, change.Namespace.Collection),
				log.OpTime(change.ClusterTime.T, change.ClusterTime.I),
			).Trace("replayed event skipped")

			continue
		}

		if change.Namespace.Database == config.PCSMDatabase {
			if r.poolIdle(lastRoutedTS) {
				r.lock.Lock()
				r.advanceCheckpoint(change.ClusterTime)
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
				r.advanceCheckpoint(change.ClusterTime)
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
			r.pool.Route(change)
			lastRoutedTS = change.ClusterTime

			r.tryAdvanceOpTime(cpTicker)

		case Invalidate:
			r.handleInvalidate(change)
			if r.err != nil {
				return
			}

			lastRoutedTS = bson.Timestamp{} // barrier flushed everything

		default:
			err := r.pool.Barrier()
			if err != nil {
				r.pool.ReleaseBarrier()
				r.setFailed(err, "Worker error during barrier")

				return
			}

			// DDL events need full parsing (rare path). The header is already
			// parsed; this additionally deserializes the typed event body
			// (e.g. CreateEvent, RenameEvent) needed by applyDDLChange.
			err = parseChangeEvent(change.RawData, change)
			if err != nil {
				r.pool.ReleaseBarrier()
				r.setFailed(err, "Parse DDL event")

				return
			}

			err = mdb.RetryWithBackoff(ctx, func() error {
				return r.applyDDLChange(ctx, change)
			}, isNonTransient, mdb.DefaultRetryInterval, maxWriteRetryDelay, 0)
			if err != nil {
				r.pool.ReleaseBarrier()
				r.setFailed(err, "Apply change")

				return
			}

			r.lock.Lock()
			r.advanceCheckpoint(change.ClusterTime)
			r.eventsApplied++
			r.lock.Unlock()

			metrics.AddEventsApplied(1)

			lastRoutedTS = bson.Timestamp{} // barrier flushed everything
			r.pool.ReleaseBarrier()
		}
	}
}

func (r *Repl) handleInvalidate(change *ChangeEvent) {
	err := r.pool.Barrier()
	if err != nil {
		r.pool.ReleaseBarrier()
		r.setFailed(err, "Worker error during barrier")

		return
	}

	if r.sourceIsPre8AndMongos() && r.movePrimaryMarker.Take(movePrimarySentinelNS) {
		r.lock.Lock()
		r.advanceCheckpoint(change.ClusterTime)
		r.eventsApplied++
		r.lock.Unlock()

		metrics.AddEventsApplied(1)
		r.pool.ReleaseBarrier()

		return
	}

	if r.sourceIsMongos {
		log.New("repl:invalidate").With(
			log.OpTime(change.ClusterTime.T, change.ClusterTime.I),
			log.NS(change.Namespace.Database, change.Namespace.Collection),
		).Warn("change stream invalidated; will reconnect from checkpoint")

		// Do not advance checkpoint; reconnect re-reads from the last applied event and idempotency handles duplicates.
		r.pool.ReleaseBarrier()

		return
	}

	r.pool.ReleaseBarrier()
	r.setFailed(errors.New("change stream invalidated unexpectedly"), "Invalidate event")
}

// tryAdvanceOpTime does a non-blocking check of cpTicker and, when fired,
// advances checkpointOpTime (and lastReplicatedOpTime) to the worker
// pool's Checkpoint. This ensures lag tracking stays current during
// sustained DML when @tick pseudo-events and poolIdle updates are insufficient.
func (r *Repl) tryAdvanceOpTime(cpTicker *time.Ticker) {
	select {
	case <-cpTicker.C:
		cp := r.pool.Checkpoint()
		if cp.IsZero() {
			return
		}

		r.lock.Lock()
		r.advanceCheckpoint(cp)
		r.lock.Unlock()
	default:
	}
}

// isReplay reports whether change is older than the applied checkpoint frontier.
// Keep strict `<` semantics: timestamp-only `<=` is unsafe because MongoDB can
// emit multiple distinct change stream events with the same clusterTime.
func (r *Repl) isReplay(change *ChangeEvent) bool {
	r.lock.Lock()
	checkpoint := r.checkpointOpTime
	r.lock.Unlock()

	return !checkpoint.IsZero() && change.ClusterTime.Before(checkpoint)
}

// advanceReportedOpTime updates lastReplicatedOpTime only. Used by the tick
// pseudo-event path where no real apply has happened.
// Caller must hold r.lock.
func (r *Repl) advanceReportedOpTime(ts bson.Timestamp) {
	if ts.After(r.lastReplicatedOpTime) {
		r.lastReplicatedOpTime = ts
	}
}

// advanceCheckpoint advances checkpointOpTime (the resume frontier driven
// by real applied events) and forwards lastReplicatedOpTime via
// advanceReportedOpTime so lag reporting stays current.
// Caller must hold r.lock.
func (r *Repl) advanceCheckpoint(ts bson.Timestamp) {
	if ts.After(r.checkpointOpTime) {
		r.checkpointOpTime = ts
	}

	r.advanceReportedOpTime(ts)
}

// poolIdle returns true when no events are pending in the worker pool.
// A zero lastRoutedTS means no events have been routed since the last
// barrier (which flushes everything), so the pool is trivially idle.
// Otherwise, it checks each worker's committed timestamp against its
// own last routed timestamp to determine if all dispatched events have
// been flushed.
func (r *Repl) poolIdle(lastRoutedTS bson.Timestamp) bool {
	if lastRoutedTS.IsZero() {
		return true
	}

	return r.pool.Idle()
}

// uuidEqual reports whether two BSON UUID binaries refer to the same UUID.
// Returns false if either pointer is nil.
func uuidEqual(a, b *bson.Binary) bool {
	if a == nil || b == nil {
		return false
	}

	return bytes.Equal(a.Data, b.Data)
}

// applyDDLChange applies a schema change to the target MongoDB.
func (r *Repl) applyDDLChange(ctx context.Context, change *ChangeEvent) error {
	lg := loggerForEvent(change)
	ctx = lg.WithContext(ctx)

	var err error

	switch change.OperationType { //nolint:exhaustive
	case Create:
		event := change.Event.(CreateEvent) //nolint:forcetypeassert
		err = r.applyCreateDDLChange(ctx, change, &event, lg)

	case Drop:
		db := change.Namespace.Database
		coll := change.Namespace.Collection
		eventUUID := change.CollectionUUID
		catalogUUID, _ := r.catalog.CollectionUUID(db, coll)

		// PCSM-249: stale phantom drop from movePrimary is suppressed.
		// The catalog has the new UUID (assigned by phantom create handler); this drop
		// carries the old UUID. SERVER-120349: phantom create is not marked fromMigrate.
		// UUID comparison only suppresses when both sides are present and differ; views
		// and other untracked namespaces fall through to the real drop, which is idempotent.
		if eventUUID != nil && catalogUUID != nil && !uuidEqual(catalogUUID, eventUUID) {
			lg.Infof("stale phantom drop suppressed (event UUID does not match catalog)")

			return nil
		}

		// On <8 mongos source, consume the movePrimary marker if present.
		// (marker signals that a real drop is part of the movePrimary invalidate sequence)
		if r.sourceIsPre8AndMongos() {
			r.movePrimaryMarker.Take(change.Namespace)
		}

		err = r.catalog.DropCollection(ctx, db, coll)
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
		err = r.doModify(ctx, change.Namespace, &event)

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

	// Invalidate handled at dispatch layer — see run() switch.
	// This case is intentionally absent; reaching it would be a programming error.
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
		// During change stream catchup after clone, DDL events may target
		// collections whose state has since changed on the target (dropped,
		// recreated as non-capped, etc.). These are benign — the clone applied
		// the definitive state and stale DDL events are safely skippable.
		// IsInvalidOptions covers collMod on collections that no longer match
		// the expected type (e.g. collMod capped on a non-capped collection).
		// IsDatabaseDropPending covers concurrent database drops during
		// change-stream catchup.
		if mdb.IsNamespaceNotFound(err) ||
			mdb.IsIndexNotFound(err) ||
			mdb.IsInvalidOptions(err) ||
			mdb.IsDatabaseDropPending(err) {
			lg.Warn(err.Error())

			return nil
		}

		return errors.Wrap(err, string(change.OperationType))
	}

	return nil
}

func (r *Repl) applyCreateDDLChange(
	ctx context.Context,
	change *ChangeEvent,
	event *CreateEvent,
	lg log.Logger,
) error {
	if event.IsTimeseries() {
		lg.Warn("Timeseries is not supported. skipping")

		return nil
	}

	db := change.Namespace.Database
	coll := change.Namespace.Collection
	eventUUID := change.CollectionUUID

	catalogUUID, exists := r.catalog.CollectionUUID(db, coll)
	switch {
	case exists && uuidEqual(catalogUUID, eventUUID):
		lg.Debug("create event replay detected, namespace already at this UUID; noop")
		if r.sourceIsPre8AndMongos() {
			r.movePrimaryMarker.Arm(change.Namespace)
			r.movePrimaryMarker.Arm(movePrimarySentinelNS)
		}
		if r.sourceIsMongos {
			err := r.repairCollectionShardingMetadata(ctx, db, coll)
			if err != nil {
				return errors.Wrap(err, "repair catalog sharding metadata")
			}
		}

		return nil

	case exists && eventUUID != nil && !uuidEqual(catalogUUID, eventUUID):
		lg.Info("phantom create from movePrimary, updating UUID; preserving Sharded/ShardKey/Indexes")
		if r.sourceIsPre8AndMongos() {
			r.movePrimaryMarker.Arm(change.Namespace)
			r.movePrimaryMarker.Arm(movePrimarySentinelNS)
		}

		r.catalog.SetCollectionUUID(ctx, db, coll, eventUUID)

		return nil
	}

	err := r.catalog.DropCollection(ctx, db, coll)
	if err != nil {
		return errors.Wrap(err, "drop before create")
	}

	err = r.catalog.CreateCollection(ctx, db, coll, &event.OperationDescription)
	if err != nil {
		return errors.Wrap(err, "create")
	}

	r.catalog.SetCollectionUUID(ctx, db, coll, eventUUID)
	lg.Infof("Collection %q has been created", change.Namespace)

	return nil
}

func (r *Repl) repairCollectionShardingMetadata(ctx context.Context, db, coll string) error {
	getShardingInfo := r.getCollectionShardingInfo
	if getShardingInfo == nil {
		getShardingInfo = mdb.GetCollectionShardingInfo
	}

	shInfo, err := getShardingInfo(ctx, r.source, db, coll)
	if err != nil {
		if errors.Is(err, mdb.ErrNotFound) {
			return nil
		}

		return errors.Wrap(err, "query source sharding info")
	}

	if shInfo == nil || !shInfo.IsSharded() {
		return nil
	}

	err = r.catalog.SetCollectionShardingMetadata(ctx, db, coll, shInfo.ShardKey)
	if err != nil && !errors.Is(err, mdb.ErrNotFound) {
		return errors.Wrap(err, "set catalog sharding metadata")
	}

	return nil
}

// alignCappedSize rounds size up to the nearest 256-byte boundary.
// MongoDB 6.x internally applies this rounding to capped collections;
// later versions do not. Use when replicating collMod cappedSize events
// from a 6.x source so the target stores an identical value.
func alignCappedSize(size int64) int64 {
	const alignment int64 = 256
	const alignmentOffset = alignment - 1

	return (size + alignmentOffset) / alignment * alignment
}

func (r *Repl) doModify(ctx context.Context, ns catalog.Namespace, event *ModifyEvent) error {
	opts := event.OperationDescription

	if len(opts.Unknown) != 0 {
		log.Ctx(ctx).Warn("Unknown modify options")
	}

	if opts.ChangeStreamPreAndPostImages != nil {
		err := r.catalog.ModifyChangeStreamPreAndPostImages(ctx,
			ns.Database, ns.Collection, opts.ChangeStreamPreAndPostImages.Enabled)
		if err != nil {
			return errors.Wrap(err, "Modify changeStreamPreAndPostImages")
		}
	}

	if opts.Validator != nil || opts.ValidationLevel != nil || opts.ValidationAction != nil {
		err := r.catalog.ModifyValidation(ctx,
			ns.Database, ns.Collection, opts.Validator, opts.ValidationLevel, opts.ValidationAction)
		if err != nil {
			return errors.Wrap(err, "Modify validation")
		}
	}

	switch {
	case opts.Index != nil:
		err := r.catalog.ModifyIndex(ctx, ns.Database, ns.Collection, opts.Index)
		if err != nil {
			return errors.Wrapf(err, "Modify index: %s", opts.Index.Name)
		}

	case opts.CappedSize != nil || opts.CappedMax != nil:
		cappedSize := opts.CappedSize
		if cappedSize != nil && r.sourceVer.Major() == 6 {
			// MongoDB 6.x rounds capped sizes to the nearest 256-byte boundary
			// internally. Replicate this alignment when applying collMod events
			// from a 6.x source so source and target store identical values.
			aligned := alignCappedSize(*cappedSize)
			cappedSize = &aligned
		}
		err := r.catalog.ModifyCappedCollection(ctx,
			ns.Database, ns.Collection, cappedSize, opts.CappedMax)
		if err != nil {
			return errors.Wrap(err, "Resize capped collection")
		}

	case opts.ViewOn != "":
		if strings.HasPrefix(opts.ViewOn, catalog.TimeseriesPrefix) {
			log.Ctx(ctx).Warn("Timeseries is not supported. skipping")

			return nil
		}

		err := r.catalog.ModifyView(ctx, ns.Database, ns.Collection, opts.ViewOn, opts.Pipeline)
		if err != nil {
			return errors.Wrap(err, "Modify view")
		}

	case opts.ExpireAfterSeconds != nil:
		log.Ctx(ctx).Warn("Collection TTL modification is not supported")

	case opts.ChangeStreamPreAndPostImages != nil:
		log.Ctx(ctx).Warn("changeStreamPreAndPostImages is not supported")
	}

	return nil
}

func loggerForEvent(change *ChangeEvent) log.Logger {
	return log.New("repl").With(
		log.OpTime(change.ClusterTime.T, change.ClusterTime.I),
		log.Op(string(change.OperationType)),
		log.NS(change.Namespace.Database, change.Namespace.Collection))
}
