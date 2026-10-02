package clone

import (
	"bytes"
	"cmp"
	"context"
	"encoding/hex"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/log"
	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/metrics"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/sel"
)

// Catalog defines the catalog operations required by the clone.
type Catalog interface {
	catalog.BaseCatalog

	AddIncompleteIndexes(ctx context.Context, db, coll string, indexes []*mdb.IndexSpecification)
	AddInconsistentIndexes(ctx context.Context, db, coll string, indexes []*mdb.IndexSpecification)
	SetCollectionTimestamp(ctx context.Context, db, coll string, ts bson.Timestamp)
}

// Options configures the clone behavior.
type Options struct {
	// Parallelism is the number of collections to clone in parallel.
	// Default: 2 (config.DefaultCloneNumParallelCollection)
	Parallelism int
	// ReadWorkers is the number of read workers during clone.
	// Default: auto (0 = runtime.NumCPU()/4)
	ReadWorkers int
	// InsertWorkers is the number of insert workers during clone.
	// Default: auto (0 = runtime.NumCPU()*2)
	InsertWorkers int
	// SegmentSizeBytes is the segment size for clone operations in bytes.
	// Default: auto (0 = calculated per collection)
	SegmentSizeBytes int64
	// ReadBatchSizeBytes is the read batch size during clone in bytes.
	// Default: ~47.5MB (config.DefaultCloneReadBatchSizeBytes)
	ReadBatchSizeBytes int32
	// SkipPresplit, when true, still shards the target collection with the source
	// key but skips pre-splitting and keeps the native chunk layout.
	// Default: false.
	SkipPresplit bool
}

// Clone handles the cloning of data from a source MongoDB to a target MongoDB.
type Clone struct {
	source   *mongo.Client // Source MongoDB client
	target   *mongo.Client // Target MongoDB client
	catalog  Catalog       // Catalog for managing collections and indexes
	nsFilter sel.NSFilter  // Namespace filter
	options  *Options      // Clone options

	targetIsSharded bool

	lock sync.Mutex
	err  error // Error encountered during the cloning process

	doneCh chan struct{}

	sizeMap    sizeMap
	totalSize  uint64        // Estimated total bytes to be cloned
	copiedSize atomic.Uint64 // Bytes copied so far

	// namespaces is the inventory, published once by the first attempt and
	// kept across a suspension: sizeMap shrinks as tasks complete and cannot
	// rebuild it. completed holds the finished tasks, keyed by taskKey.
	namespaces     []namespaceInfo
	inventoryReady bool
	completed      map[string]struct{}

	startTS  bson.Timestamp // source cluster timestamp when cloning started
	finishTS bson.Timestamp // source cluster timestamp when cloning completed

	startTime  time.Time
	finishTime time.Time

	// targetShardSizes tracks cumulative estimated bytes assigned to each target shard by pre-split.
	targetShardSizes *shardSizes
}

// Status represents the status of the cloning process.
type Status struct {
	EstimatedTotalSizeBytes uint64 // Estimated total bytes to be copied
	CopiedSizeBytes         uint64 // Bytes copied so far

	StartTS  bson.Timestamp
	FinishTS bson.Timestamp

	StartTime  time.Time
	FinishTime time.Time

	Err error // Error encountered during the cloning process
}

//go:inline
func (s *Status) IsStarted() bool {
	return !s.StartTime.IsZero()
}

//go:inline
func (s *Status) IsRunning() bool {
	return s.IsStarted() && !s.IsFinished()
}

//go:inline
func (s *Status) IsFinished() bool {
	return !s.FinishTime.IsZero()
}

// NewClone creates a new Clone instance with the given options.
func NewClone(
	source, target *mongo.Client,
	cat Catalog,
	nsFilter sel.NSFilter,
	opts *Options,
	targetIsSharded bool,
) *Clone {
	return &Clone{
		source:           source,
		target:           target,
		catalog:          cat,
		nsFilter:         nsFilter,
		options:          opts,
		doneCh:           make(chan struct{}),
		completed:        make(map[string]struct{}),
		targetIsSharded:  targetIsSharded,
		targetShardSizes: newShardSizes(),
	}
}

// Checkpoint represents the checkpoint state for clone recovery.
type Checkpoint struct {
	TotalSize  uint64 `bson:"totalSize,omitempty"`
	CopiedSize uint64 `bson:"copiedSize,omitempty"`

	StartTS  bson.Timestamp `bson:"startTS,omitempty"`
	FinishTS bson.Timestamp `bson:"finishTS,omitempty"`

	StartTime  time.Time `bson:"startTime,omitempty"`
	FinishTime time.Time `bson:"finishTime,omitempty"`

	Error string `bson:"error,omitempty"`
}

func (c *Clone) Checkpoint() *Checkpoint {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.startTime.IsZero() && c.err == nil {
		return nil
	}

	cp := &Checkpoint{
		TotalSize:  c.totalSize,
		CopiedSize: c.copiedSize.Load(),
		StartTS:    c.startTS,
		FinishTS:   c.finishTS,
		StartTime:  c.startTime,
		FinishTime: c.finishTime,
	}
	if c.err != nil {
		cp.Error = c.err.Error()
	}

	return cp
}

func (c *Clone) Recover(cp *Checkpoint) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if !c.startTS.IsZero() {
		return errors.New("cannot restore: already used")
	}

	c.totalSize = cp.TotalSize // XXX: re-calculate
	c.copiedSize.Store(cp.CopiedSize)
	c.startTS = cp.StartTS
	c.finishTS = cp.FinishTS
	c.startTime = cp.StartTime
	c.finishTime = cp.FinishTime

	if cp.Error != "" {
		c.err = errors.New(cp.Error)
	}

	return nil
}

// Status returns the current status of the cloning process.
func (c *Clone) Status() Status {
	c.lock.Lock()
	defer c.lock.Unlock()

	return Status{
		EstimatedTotalSizeBytes: c.totalSize,
		CopiedSizeBytes:         c.copiedSize.Load(),
		StartTS:                 c.startTS,
		FinishTS:                c.finishTS,
		StartTime:               c.startTime,
		FinishTime:              c.finishTime,
		Err:                     c.err,
	}
}

// ResetError clears any error stored in the Clone instance.
func (c *Clone) ResetError() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.err = nil
}

func (c *Clone) Done() <-chan struct{} {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.doneCh
}

// Start starts the cloning process.
func (c *Clone) Start(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	lg := log.New("clone")

	if c.err != nil {
		return errors.Wrap(c.err, "cannot start due an existing error")
	}

	if !c.finishTime.IsZero() {
		return errors.New("already completed")
	}

	if !c.startTime.IsZero() {
		return errors.New("already started")
	}

	lg.Info("Starting Data Clone")

	c.startTime = time.Now()

	go c.runAttempt(ctx)

	return nil
}

// Resume continues a clone whose previous attempt ended on cancellation. That
// attempt left startTS, the inventory and the completed tasks in place, so
// only the remaining tasks run; a collection that was in flight is copied
// again from scratch, replayed from the same startTS by change replication.
func (c *Clone) Resume(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.err != nil {
		return errors.Wrap(c.err, "cannot resume due an existing error")
	}

	if c.startTime.IsZero() {
		return errors.New("not started")
	}

	if !c.finishTime.IsZero() {
		return errors.New("already completed")
	}

	select {
	case <-c.doneCh:
	default:
		return errors.New("still running")
	}

	c.doneCh = make(chan struct{})

	log.New("clone").Info("Resuming Data Clone")

	go c.runAttempt(ctx)

	return nil
}

// runAttempt runs one attempt and publishes its outcome. A run ended by the
// caller's cancellation is a suspension: no error and no finish time are
// recorded, so Resume can continue it. A genuine error, even one racing the
// cancellation, fails the clone as before.
func (c *Clone) runAttempt(ctx context.Context) {
	lg := log.New("clone")

	err := c.run(ctx)

	c.lock.Lock()
	defer c.lock.Unlock()

	if err != nil && ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		lg.With(log.Size(c.copiedSize.Load())).Info("Data Clone suspended")
		close(c.doneCh)

		return
	}

	if err != nil {
		c.err = err
	}

	close(c.doneCh)

	c.finishTime = time.Now()
	elapsed := c.finishTime.Sub(c.startTime)

	if err != nil {
		lg.With(log.Elapsed(elapsed)).
			Errorf(err, "Data Clone has failed: %s in %s",
				humanize.Bytes(c.copiedSize.Load()), elapsed.Round(time.Second))

		return
	}

	lg.With(log.Elapsed(elapsed), log.Size(c.copiedSize.Load())).
		Infof("Data Clone completed: %s in %s",
			humanize.Bytes(c.copiedSize.Load()), elapsed.Round(time.Second))
}

// taskKey identifies an inventory entry across attempts: the source UUID
// where there is one, the namespace for views.
func taskKey(ns namespaceInfo) string {
	if ns.UUID != nil {
		return hex.EncodeToString(ns.UUID.Data)
	}

	return ns.String()
}

// remainingNamespaces returns the inventory minus the completed tasks. The
// caller holds c.lock.
func (c *Clone) remainingNamespaces() []namespaceInfo {
	remaining := make([]namespaceInfo, 0, len(c.namespaces))

	for _, ns := range c.namespaces {
		if _, done := c.completed[taskKey(ns)]; !done {
			remaining = append(remaining, ns)
		}
	}

	return remaining
}

func (c *Clone) run(ctx context.Context) error {
	lg := log.New("clone")
	ctx = lg.WithContext(ctx)

	err := c.prepare(ctx)
	if err != nil {
		return err
	}

	c.lock.Lock()
	remaining := c.remainingNamespaces()
	inventory := len(c.namespaces)
	c.lock.Unlock()

	switch {
	case len(remaining) != 0:
		err = c.doClone(ctx, remaining)
		if err != nil {
			return errors.Wrap(err, "copy")
		}
	case inventory == 0:
		lg.Warn("No collection to clone")
	}

	finishTS, err := mdb.ClusterTime(ctx, c.source)
	if err != nil {
		return errors.Wrap(err, "finishTS: get source cluster time")
	}

	c.lock.Lock()
	c.finishTS = finishTS
	c.lock.Unlock()

	return nil
}

// prepare captures the oplog anchor and the inventory. Both are taken once: a
// resumed attempt keeps startTS as the replay anchor of every collection, and
// keeps the inventory because sizeMap shrinks as tasks complete.
func (c *Clone) prepare(ctx context.Context) error {
	lg := log.Ctx(ctx)

	c.lock.Lock()
	startTS := c.startTS
	ready := c.inventoryReady
	c.lock.Unlock()

	if startTS.IsZero() {
		// Use appendOplogNote, not ping: it waits until all in-flight writes are
		// durable so startTS anchors to a real oplog event. ping would return before
		// those writes commit, so an earlier write could be missed by both the clone
		// scan and the change stream that starts at startTS. See PCSM-241.
		ts, err := mdb.AdvanceClusterTime(ctx, c.source)
		if err != nil {
			return errors.Wrap(err, "startTS: advance source cluster time")
		}

		c.lock.Lock()
		c.startTS = ts
		c.lock.Unlock()
	}

	if ready {
		return nil
	}

	err := c.collectSizeMap(ctx)
	if err != nil {
		return errors.Wrap(err, "get size map")
	}

	// init metrics
	metrics.AddCopyReadSize(0)
	metrics.AddCopyInsertSize(0)
	metrics.AddCopyReadDocumentCount(0)
	metrics.AddCopyInsertDocumentCount(0)
	metrics.SetCopyReadBatchDurationSeconds(0)
	metrics.SetCopyInsertBatchDurationSeconds(0)
	metrics.SetEstimatedTotalSizeBytes(c.totalSize)

	lg.With(log.Size(c.totalSize)).
		Infof("Estimated Total Size %s", humanize.Bytes(c.totalSize))

	namespaces, err := c.listPrioritizedNamespaces()
	if err != nil {
		return errors.Wrap(err, "list prioritized namespaces")
	}

	c.lock.Lock()
	c.namespaces = namespaces
	c.inventoryReady = true
	c.lock.Unlock()

	return nil
}

func (c *Clone) doClone(ctx context.Context, namespaces []namespaceInfo) error {
	cloneLogger := log.Ctx(ctx)

	numParallelCollections := c.options.Parallelism
	if numParallelCollections < 1 {
		numParallelCollections = config.DefaultCloneNumParallelCollection
	}

	cloneLogger.Debugf("NumParallelCollections: %d", numParallelCollections)

	// One attempt-wide context for the manager and every collection. Inserts
	// run on the manager's context, not the collection's, so the first fatal
	// error cancels them too and the failing task can drain its progress and
	// return. The run's own context is left alone: cancellation there is a
	// suspension, cancellation here is a failure.
	attemptCtx, cancelAttempt := context.WithCancel(ctx)
	defer cancelAttempt()

	copyManager := NewCopyManager(attemptCtx, c.source, c.target, CopyManagerOptions{
		NumReadWorkers:     c.options.ReadWorkers,
		NumInsertWorkers:   c.options.InsertWorkers,
		SegmentSizeBytes:   c.options.SegmentSizeBytes,
		ReadBatchSizeBytes: c.options.ReadBatchSizeBytes,
	})
	defer copyManager.Close()

	eg, grpCtx := errgroup.WithContext(attemptCtx)
	eg.SetLimit(numParallelCollections)

	for _, task := range namespaces {
		eg.Go(func() error {
			lg := cloneLogger.With(log.NS(task.Database, task.Collection))

			return c.cloneTask(lg.WithContext(grpCtx), copyManager, cancelAttempt, task)
		})
	}

	err := eg.Wait()

	return err //nolint:wrapcheck
}

// cloneTask copies one inventory entry to completion, following a rename of
// the source collection, and commits the task's accounting only once the
// whole task succeeded. On failure the bytes it added to copiedSize are rolled
// back, so a redo after a suspension counts them once.
func (c *Clone) cloneTask(
	ctx context.Context, copyManager *CopyManager, abort func(), task namespaceInfo,
) error {
	lg := log.Ctx(ctx)
	ns := task

	var copied uint64

	// Adding the two's complement subtracts copied atomically.
	rollback := func() { c.copiedSize.Add(^(copied - 1)) }

	for {
		// A collection copied again (resume after a suspension, or a rename)
		// is dropped and sharded again; its earlier placement charge goes.
		c.targetShardSizes.release(ns.String())

		n, err := c.doCollectionClone(ctx, copyManager, abort, ns)
		copied += n

		if err != nil && !errors.As(err, &NamespaceNotFoundError{}) {
			rollback()

			return errors.Wrap(err, ns.String())
		}

		// check if the collection was renamed during clone.

		if ns.UUID == nil { // view cannot be renamed
			break
		}

		name, err := mdb.GetCollectionNameByUUID(ctx, c.source, ns.Database, *ns.UUID)
		if err != nil {
			if errors.Is(err, mdb.ErrNotFound) { // dropped
				lg.Warnf("Collection %s not found", ns.Namespace)

				break
			}

			rollback()

			return errors.Wrapf(err, "get collection name by uuid: %s", ns)
		}

		if name == ns.Collection {
			break // OK: collection has not been renamed
		}

		prevNS := ns
		ns.Collection = name

		c.lock.Lock()
		elem := c.sizeMap[prevNS.String()]
		delete(c.sizeMap, prevNS.String())
		c.sizeMap[ns.String()] = elem
		c.lock.Unlock()

		lg.Infof("Collection %s was renamed to %s. Retrying to clone the collection",
			prevNS.Namespace, ns.Namespace)

		err = c.catalog.DropCollection(ctx, prevNS.Database, prevNS.Collection)
		if err != nil {
			rollback()

			return errors.Wrapf(err, "drop collection %q", prevNS.Namespace)
		}

		// The copy under the previous name is gone from the target, and so are
		// its bytes and its placement charge.
		c.targetShardSizes.release(prevNS.String())
		c.copiedSize.Add(^(n - 1))
		copied -= n

		lg.Infof("Previous collection %s was dropped", prevNS.Namespace)
	}

	c.commitTask(ctx, task, ns, copied)

	return nil
}

// commitTask settles a finished task: it corrects the estimate with the bytes
// the collection really had, removes its sizeMap entry and marks it completed
// so a resumed attempt skips it.
func (c *Clone) commitTask(ctx context.Context, task, final namespaceInfo, copied uint64) {
	c.lock.Lock()
	diff := c.sizeMap[final.String()].Size - copied
	c.totalSize -= diff // adjust
	totalSize := c.totalSize
	delete(c.sizeMap, final.String())
	c.completed[taskKey(task)] = struct{}{}
	c.lock.Unlock()

	metrics.SetEstimatedTotalSizeBytes(totalSize)

	if diff != 0 {
		log.Ctx(ctx).With(log.Size(totalSize)).
			Infof("Estimated Total Size %s [updated]", humanize.Bytes(totalSize))
	}
}

// shardCollection replicates the source's sharding for ns onto the target:
// it shards the target collection with the same key and, for ranged keys,
// pre-splits the empty collection unless pre-splitting is disabled.
func (c *Clone) shardCollection(ctx context.Context, ns catalog.Namespace) error {
	lg := log.Ctx(ctx).With(log.NS(ns.Database, ns.Collection))

	shInfo, err := mdb.GetCollectionShardingInfo(ctx, c.source, ns.Database, ns.Collection)
	if err != nil && !errors.Is(err, mdb.ErrNotFound) {
		return errors.Wrap(err, "get sharding info")
	}

	if shInfo == nil || !shInfo.IsSharded() {
		return nil // source collection is unsharded — nothing to replicate
	}

	err = c.catalog.ShardCollection(ctx, ns.Database, ns.Collection, shInfo.ShardKey, shInfo.Unique)
	if err != nil {
		return errors.Wrap(err, "shard collection")
	}

	lg.Infof("Collection %q sharded", ns.String())

	if c.options.SkipPresplit {
		lg.Infof("Pre-split of %q skipped by configuration, keeping the native chunk layout", ns.String())

		return nil
	}

	err = presplit(ctx, c.source, c.target, ns, shInfo, c.targetShardSizes)
	if err != nil {
		if ctx.Err() != nil {
			return errors.Wrap(err, "presplit chunks")
		}

		// Pre-split is a placement optimization. The target keeps its existing layout,
		// possibly partially split or moved, and the balancer evens it out so cloning
		// can continue.
		lg.Warnf("Pre-split of %q failed, keeping the native chunk layout: %v", ns.String(), err)
	}

	return nil
}

// doCollectionClone copies one collection and returns the bytes it added to
// copiedSize, so the caller can roll them back when the task fails. On the
// first fatal progress error it aborts the attempt but keeps draining the
// progress channel to closure, so no producer blocks on a send.
func (c *Clone) doCollectionClone(
	ctx context.Context,
	copyManager *CopyManager,
	abort func(),
	task namespaceInfo,
) (uint64, error) {
	copyLogger := log.Ctx(ctx)
	ns := task.Namespace

	lg := copyLogger.With(log.NS(ns.Database, ns.Collection))

	var startedAt time.Time
	var totalCopiedCount int64
	var totalCopiedSizeBytes uint64

	var lastLogAt time.Time
	var copiedCountSinceLastLog int64
	var copiedSizeBytesSinceLastLog uint64

	c.lock.Lock()
	nsSize := c.sizeMap[ns.String()]
	c.lock.Unlock()

	lg.With(log.Count(nsSize.Count), log.Size(nsSize.Size)).
		Debugf("Starting %q collection clone: %d documents (%s)",
			ns, nsSize.Count, humanize.Bytes(nsSize.Size))

	startedAt = time.Now()

	capturedAt, err := mdb.ClusterTime(ctx, c.source)
	if err != nil {
		return 0, errors.Wrap(err, "get source cluster time")
	}

	spec, err := mdb.GetCollectionSpec(ctx, c.source, ns.Database, ns.Collection)
	if err != nil {
		if errors.Is(err, mdb.ErrNotFound) {
			return 0, NamespaceNotFoundError{ns.Database, ns.Collection}
		}

		return 0, errors.Wrap(err, "$collStats")
	}

	// The task is the source UUID: a different collection living at this
	// name now is a different generation. Leave it to the caller's UUID
	// lookup, which decides between renamed and dropped; replay creates the
	// new one from its own events.
	if task.UUID != nil && spec.UUID != nil && !bytes.Equal(spec.UUID.Data, task.UUID.Data) {
		return 0, NamespaceNotFoundError{ns.Database, ns.Collection}
	}

	if spec.Type == mdb.TypeTimeseries {
		return 0, catalog.ErrTimeseriesUnsupported
	}

	err = c.createCollection(ctx, ns, spec)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			lg.Errorf(err, "Failed to create %q collection", ns.String())
		}

		return 0, errors.Wrap(err, "createCollection")
	}

	if spec.Type == mdb.TypeCollection {
		err = c.createIndexes(ctx, ns)
		if err != nil {
			return 0, errors.Wrap(err, "create indexes")
		}
	}

	lg.Infof("Collection %q created", ns.String())

	if c.targetIsSharded {
		err = c.shardCollection(ctx, ns)
		if err != nil {
			return 0, err
		}
	}

	c.catalog.SetCollectionTimestamp(ctx, ns.Database, ns.Collection, capturedAt)

	if spec.UUID != nil {
		c.catalog.SetCollectionUUID(ctx, ns.Database, ns.Collection, spec.UUID)
	}

	lastLogAt = time.Now() // init

	progressUpdateCh := copyManager.Start(ctx, ns, spec)

	// After the first fatal error the attempt is aborted and the channel is
	// drained to closure: a producer parked on a send would otherwise pin the
	// manager's collectionsWg and Close would never return. Later errors are
	// the abort's own fallout; later byte counts are inserts that landed and
	// are rolled back by the caller.
	var fatal error

	for progressUpdate := range progressUpdateCh {
		err := progressUpdate.Err
		if err != nil && fatal != nil {
			continue
		}

		if err != nil {
			switch {
			case mdb.IsCollectionDropped(err):
				lg.Warnf("Collection %q has been dropped during clone: %s", ns, err)

				err := c.catalog.DropCollection(ctx, ns.Database, ns.Collection)
				if err != nil {
					lg.Errorf(err, "Drop collection %q", ns)
				} else {
					lg.Infof("Collection %q has been dropped on target", ns)
				}

				// update estimated size
				c.lock.Lock()
				c.totalSize -= c.sizeMap[ns.String()].Size
				totalSize := c.totalSize
				delete(c.sizeMap, ns.String())
				c.lock.Unlock()

				metrics.SetEstimatedTotalSizeBytes(totalSize)

				copyLogger.With(log.Size(totalSize)).
					Infof("Estimated Total Size %s [updated]", humanize.Bytes(totalSize))

			case mdb.IsCollectionRenamed(err):
				lg.Warnf("Collection %q has been renamed during clone: %s", ns, err)

			case errors.Is(err, catalog.ErrTimeseriesUnsupported):
				lg.Warnf("Timeseries is not supported (%q)", ns)

			default:
				updateLog := lg.With(
					log.Size(progressUpdate.SizeBytes),
					log.Count(int64(progressUpdate.Count)),
					log.Elapsed(time.Since(lastLogAt)),
				)

				if errors.Is(err, context.Canceled) {
					updateLog.Warnf("Copy documents for collection %q is canceled: %s", ns, err)
				} else {
					updateLog.Errorf(err, "Failed to copy documents for collection %q", ns)
				}

				fatal = errors.Wrap(err, ns.Collection)
				abort()
			}
		}

		totalCopiedCount += int64(progressUpdate.Count)
		totalCopiedSizeBytes += progressUpdate.SizeBytes
		c.copiedSize.Add(progressUpdate.SizeBytes)

		copiedCountSinceLastLog += int64(progressUpdate.Count)
		copiedSizeBytesSinceLastLog += progressUpdate.SizeBytes

		if copiedSizeBytesSinceLastLog >= humanize.GByte {
			now := time.Now()
			lg.With(
				log.Size(copiedSizeBytesSinceLastLog),
				log.Count(copiedCountSinceLastLog),
				log.Elapsed(now.Sub(lastLogAt)),
			).Debugf("copied %s (%d documents) for %q",
				humanize.Bytes(copiedSizeBytesSinceLastLog), copiedCountSinceLastLog, ns)

			copiedSizeBytesSinceLastLog = 0
			lastLogAt = now
		}
	}

	if copiedSizeBytesSinceLastLog > 0 {
		lg.With(
			log.Size(copiedSizeBytesSinceLastLog),
			log.Count(copiedCountSinceLastLog),
			log.Elapsed(time.Since(lastLogAt)),
		).Debugf("copied %s (%d documents) for %q",
			humanize.Bytes(copiedSizeBytesSinceLastLog), copiedCountSinceLastLog, ns)
	}

	if fatal != nil {
		return totalCopiedSizeBytes, fatal
	}

	elapsed := time.Since(startedAt)
	lg.With(
		log.Size(totalCopiedSizeBytes),
		log.Count(totalCopiedCount),
		log.Elapsed(elapsed),
	).Infof("Collection %q cloned: %s in %s (%d documents)",
		ns, humanize.Bytes(totalCopiedSizeBytes),
		elapsed.Round(time.Second), totalCopiedCount)

	return totalCopiedSizeBytes, nil
}

type sizeMap map[string]sizeMapElem

type sizeMapElem struct {
	UUID  *bson.Binary
	Size  uint64
	Count int64
}

func (c *Clone) collectSizeMap(ctx context.Context) error {
	lg := log.Ctx(ctx)

	databases, err := mdb.ListDatabaseNames(ctx, c.source)
	if err != nil {
		return errors.Wrap(err, "list database names")
	}

	dbGrp, dbGrpCtx := errgroup.WithContext(ctx)
	dbGrp.SetLimit(runtime.NumCPU() * 2) //nolint:mnd

	mu := &sync.Mutex{}
	sm := make(sizeMap)
	total := uint64(0)

	for _, db := range databases {
		if db == config.PCSMDatabase {
			continue
		}

		dbGrp.Go(func() error {
			collSpecs, err := mdb.ListCollectionSpecs(dbGrpCtx, c.source, db)
			if err != nil {
				return errors.Wrap(err, "listCollections")
			}

			collGrp, collGrpCtx := errgroup.WithContext(dbGrpCtx)
			collGrp.SetLimit(runtime.NumCPU() * 2) //nolint:mnd

			for _, spec := range collSpecs {
				if spec.Type == mdb.TypeTimeseries {
					lg.With(log.NS(db, spec.Name)).
						Warnf("Timeseries is not supported: %q. skipping", db+"."+spec.Name)

					continue
				}

				if !c.nsFilter(db, spec.Name) {
					lg.With(log.NS(db, spec.Name)).Infof("Namespace %q excluded", db+"."+spec.Name)

					continue
				}

				lg.With(log.NS(db, spec.Name)).Infof("Namespace %q included", db+"."+spec.Name)

				collGrp.Go(func() error {
					ns := db + "." + spec.Name
					if spec.Type == mdb.TypeView {
						mu.Lock()
						sm[ns] = sizeMapElem{}
						mu.Unlock()

						return nil
					}

					stats, err := mdb.GetCollStats(collGrpCtx, c.source, db, spec.Name)
					if err != nil {
						if errors.Is(err, mdb.ErrNotFound) {
							return nil
						}

						return errors.Wrapf(err, "get collection stats for %q", ns)
					}

					mu.Lock()
					sm[ns] = sizeMapElem{
						UUID:  spec.UUID,
						Size:  uint64(stats.Size), //nolint:gosec
						Count: stats.Count,
					}
					total += uint64(stats.Size) //nolint:gosec
					mu.Unlock()

					return nil
				})
			}

			err = collGrp.Wait()
			if err != nil {
				return errors.Wrapf(err, "collect collections for %q", db)
			}

			return nil
		})
	}

	err = dbGrp.Wait()
	if err != nil {
		return errors.Wrap(err, "collect databases")
	}

	c.lock.Lock()
	c.sizeMap = sm
	c.totalSize = total
	c.lock.Unlock()

	return nil
}

type namespaceInfo struct {
	catalog.Namespace

	UUID *bson.Binary
}

func (c *Clone) listPrioritizedNamespaces() ([]namespaceInfo, error) {
	namespaces := []namespaceInfo{}

	for ns, elem := range c.sizeMap {
		namespace, err := catalog.ParseNamespace(ns)
		if err != nil {
			return nil, errors.Wrapf(err, "parse namespace %q", ns)
		}

		namespaces = append(namespaces, namespaceInfo{
			Namespace: namespace,
			UUID:      elem.UUID,
		})
	}

	// sort from larger to smaller
	slices.SortFunc(namespaces, func(a, b namespaceInfo) int {
		return cmp.Compare(c.sizeMap[b.Namespace.String()].Size, c.sizeMap[a.Namespace.String()].Size)
	})

	return namespaces, nil
}

// NamespaceNotFoundError indicates a collection was not found.
type NamespaceNotFoundError struct {
	Database   string
	Collection string
}

func (e NamespaceNotFoundError) Error() string {
	return "collection not found: " + e.Database + "." + e.Collection
}

func (c *Clone) createCollection(
	ctx context.Context,
	ns catalog.Namespace,
	spec *mdb.CollectionSpecification,
) error {
	if spec.Type == mdb.TypeTimeseries {
		return catalog.ErrTimeseriesUnsupported
	}

	var createOptions catalog.CreateCollectionOptions

	err := bson.Unmarshal(spec.Options, &createOptions)
	if err != nil {
		return errors.Wrap(err, "unmarshal options")
	}

	err = c.catalog.DropCollection(ctx, ns.Database, ns.Collection)
	if err != nil {
		return errors.Wrap(err, "ensure no collection before create")
	}

	err = c.catalog.CreateCollection(ctx, ns.Database, ns.Collection, &createOptions)
	if err != nil {
		return errors.Wrap(err, "create collection")
	}

	return nil
}

func (c *Clone) createIndexes(ctx context.Context, ns catalog.Namespace) error {
	indexes, err := mdb.ListIndexes(ctx, c.source, ns.Database, ns.Collection)
	if err != nil {
		return errors.Wrap(err, "list indexes")
	}

	unfinishedBuilds, err := mdb.ListInProgressIndexBuilds(ctx,
		c.source, ns.Database, ns.Collection)
	if err != nil {
		return errors.Wrap(err, "list in-progress index builds")
	}

	inconsistentIndexes, err := mdb.ListInconsistentIndexes(ctx,
		c.source, ns.Database, ns.Collection)
	if err != nil {
		return errors.Wrap(err, "list inconsistent indexes")
	}

	log.Ctx(ctx).Debugf("Indexes to create for %q: total=%d, unfinished=%d, inconsistent=%d",
		ns.String(), len(indexes), len(unfinishedBuilds), len(inconsistentIndexes))

	if len(unfinishedBuilds) == 0 && len(inconsistentIndexes) == 0 {
		err = c.catalog.CreateIndexes(ctx, ns.Database, ns.Collection, indexes)
		if err != nil {
			return errors.Wrap(err, "create indexes")
		}

		return nil
	}

	// Inconsistent index specs come from $indexStats, which sees indexes
	// that mongos `listIndexes` may hide. Skip those names when partitioning
	// the mongos-visible list so we don't add the same index twice.
	inconsistentNames := make(map[string]struct{}, len(inconsistentIndexes))
	for _, idx := range inconsistentIndexes {
		inconsistentNames[idx.Name] = struct{}{}
	}

	builtIndexesCap := max(len(indexes)-len(unfinishedBuilds)-len(inconsistentNames), 0)

	builtIndexes := make([]*mdb.IndexSpecification, 0, builtIndexesCap)
	incompleteIndexes := make([]*mdb.IndexSpecification, 0, len(unfinishedBuilds))

	for _, index := range indexes {
		if slices.Contains(unfinishedBuilds, index.Name) {
			incompleteIndexes = append(incompleteIndexes, index)

			continue
		}

		if _, ok := inconsistentNames[index.Name]; ok {
			continue
		}

		builtIndexes = append(builtIndexes, index)
	}

	if len(builtIndexes) != 0 {
		err = c.catalog.CreateIndexes(ctx, ns.Database, ns.Collection, builtIndexes)
		if err != nil {
			return errors.Wrap(err, "create indexes")
		}
	}

	if len(incompleteIndexes) != 0 {
		c.catalog.AddIncompleteIndexes(ctx, ns.Database, ns.Collection, incompleteIndexes)
	}

	if len(inconsistentIndexes) != 0 {
		c.catalog.AddInconsistentIndexes(ctx, ns.Database, ns.Collection, inconsistentIndexes)
	}

	return nil
}
