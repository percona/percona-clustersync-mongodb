package clone

import (
	"cmp"
	"context"
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
	"github.com/percona/percona-clustersync-mongodb/metrics"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/sel"
	"github.com/percona/percona-clustersync-mongodb/topo"
)

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
}

// Clone handles the cloning of data from a source MongoDB to a target MongoDB.
type Clone struct {
	source   *mongo.Client    // Source MongoDB client
	target   *mongo.Client    // Target MongoDB client
	catalog  *catalog.Catalog // Catalog for managing collections and indexes
	nsFilter sel.NSFilter     // Namespace filter
	options  *Options         // Clone options

	lock sync.Mutex
	err  error // Error encountered during the cloning process

	doneCh chan struct{}

	sizeMap    sizeMap
	totalSize  uint64        // Estimated total bytes to be cloned
	copiedSize atomic.Uint64 // Bytes copied so far

	startTS  bson.Timestamp // source cluster timestamp when cloning started
	finishTS bson.Timestamp // source cluster timestamp when cloning completed

	startTime  time.Time
	finishTime time.Time
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
func (cs *Status) IsStarted() bool {
	return !cs.StartTime.IsZero()
}

//go:inline
func (cs *Status) IsRunning() bool {
	return cs.IsStarted() && !cs.IsFinished()
}

//go:inline
func (cs *Status) IsFinished() bool {
	return !cs.FinishTime.IsZero()
}

// NewClone creates a new Clone instance with the given options.
func NewClone(
	source, target *mongo.Client,
	cat *catalog.Catalog,
	nsFilter sel.NSFilter,
	opts *Options,
) *Clone {
	return &Clone{
		source:   source,
		target:   target,
		catalog:  cat,
		nsFilter: nsFilter,
		options:  opts,
		doneCh:   make(chan struct{}),
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
		FinishTS:   bson.Timestamp{},
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
func (c *Clone) Start(context.Context) error {
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

	go func() {
		err := c.run()

		c.lock.Lock()
		defer c.lock.Unlock()

		if err != nil {
			c.err = err
		}

		select {
		case <-c.doneCh:
		default:
			close(c.doneCh)
		}

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
	}()

	return nil
}

func (c *Clone) run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lg := log.New("clone")
	ctx = lg.WithContext(ctx)

	startTS, err := topo.ClusterTime(ctx, c.source)
	if err != nil {
		return errors.Wrap(err, "startTS: get source cluster time")
	}

	c.lock.Lock()
	c.startTS = startTS
	c.lock.Unlock()

	err = c.collectSizeMap(ctx)
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

	if len(namespaces) != 0 {
		err = c.doClone(ctx, namespaces)
		if err != nil {
			return errors.Wrap(err, "copy")
		}
	} else {
		lg.Warn("No collection to clone")
	}

	finishTS, err := topo.ClusterTime(ctx, c.source)
	if err != nil {
		return errors.Wrap(err, "finishTS: get source cluster time")
	}

	c.lock.Lock()
	c.finishTS = finishTS
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

	copyManager := NewCopyManager(c.source, c.target, CopyManagerOptions{
		NumReadWorkers:     c.options.ReadWorkers,
		NumInsertWorkers:   c.options.InsertWorkers,
		SegmentSizeBytes:   c.options.SegmentSizeBytes,
		ReadBatchSizeBytes: c.options.ReadBatchSizeBytes,
	})
	defer copyManager.Close()

	eg, grpCtx := errgroup.WithContext(ctx)
	eg.SetLimit(numParallelCollections)

	for _, ns := range namespaces {
		eg.Go(func() error {
			ns := ns
			lg := cloneLogger.With(log.NS(ns.Database, ns.Collection))
			ctx := lg.WithContext(grpCtx)

			for {
				err := c.doCollectionClone(ctx, copyManager, ns.Namespace)
				if err != nil && !errors.As(err, &NamespaceNotFoundError{}) {
					return errors.Wrap(err, ns.String())
				}

				// check if the collection was renamed during clone.

				if ns.UUID == nil { // view cannot be renamed
					return nil
				}

				name, err := topo.GetCollectionNameByUUID(ctx, c.source, ns.Database, *ns.UUID)
				if err != nil {
					if errors.Is(err, topo.ErrNotFound) { // dropped
						lg.Warnf("Collection %s not found", ns.Namespace)

						return nil
					}

					return errors.Wrapf(err, "get collection name by uuid: %s", ns)
				}

				if name == ns.Collection {
					return nil // OK: collection has not been renamed
				}

				prevNS := ns
				ns = namespaceInfo{
					Namespace: catalog.Namespace{Database: prevNS.Database, Collection: name},
					UUID:      prevNS.UUID,
				}

				c.lock.Lock()
				elem := c.sizeMap[prevNS.String()]
				delete(c.sizeMap, prevNS.String())
				c.sizeMap[prevNS.String()] = elem
				c.lock.Unlock()

				lg.Infof("Collection %s was renamed to %s. Retrying to clone the collection",
					prevNS.Namespace, ns.Namespace)

				err = c.catalog.DropCollection(ctx, prevNS.Database, prevNS.Collection)
				if err != nil {
					return errors.Wrapf(err, "drop collection %q", prevNS.Namespace)
				}

				lg.Infof("Previous collection %s was dropped", prevNS.Namespace)
			}
		})
	}

	err := eg.Wait()

	return err //nolint:wrapcheck
}

func (c *Clone) doCollectionClone(
	ctx context.Context,
	copyManager *CopyManager,
	ns catalog.Namespace,
) error {
	copyLogger := log.Ctx(ctx)

	nsCtx, cancel := context.WithCancel(ctx)
	defer cancel()

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

	capturedAt, err := topo.ClusterTime(ctx, c.source)
	if err != nil {
		return errors.Wrap(err, "get source cluster time")
	}

	spec, err := topo.GetCollectionSpec(ctx, c.source, ns.Database, ns.Collection)
	if err != nil {
		if errors.Is(err, topo.ErrNotFound) {
			return NamespaceNotFoundError{ns.Database, ns.Collection}
		}

		return errors.Wrap(err, "$collStats")
	}

	if spec.Type == topo.TypeTimeseries {
		return catalog.ErrTimeseriesUnsupported
	}

	err = c.createCollection(ctx, ns, spec)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			lg.Errorf(err, "Failed to create %q collection", ns.String())
		}

		return errors.Wrap(err, "createCollection")
	}

	if spec.Type == topo.TypeCollection {
		err = c.createIndexes(ctx, ns)
		if err != nil {
			return errors.Wrap(err, "create indexes")
		}
	}

	lg.Infof("Collection %q created", ns.String())

	shInfo, err := topo.GetCollectionShardingInfo(ctx, c.source, ns.Database, ns.Collection)
	if err != nil && !errors.Is(err, topo.ErrNotFound) {
		return errors.Wrap(err, "get sharding info")
	}

	if shInfo != nil && shInfo.IsSharded() {
		err := c.catalog.ShardCollection(ctx, ns.Database, ns.Collection, shInfo.ShardKey, shInfo.Unique)
		if err != nil {
			return errors.Wrap(err, "shard collection")
		}

		lg.Infof("Collection %q sharded", ns.String())
	}

	c.catalog.SetCollectionTimestamp(ctx, ns.Database, ns.Collection, capturedAt)

	if spec.UUID != nil {
		c.catalog.SetCollectionUUID(ctx, ns.Database, ns.Collection, spec.UUID)
	}

	lastLogAt = time.Now() // init

	progressUpdateCh := copyManager.Start(nsCtx, ns, spec)

	for progressUpdate := range progressUpdateCh {
		err := progressUpdate.Err
		if err != nil {
			switch {
			case topo.IsCollectionDropped(err):
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

			case topo.IsCollectionRenamed(err):
				lg.Warnf("Collection %q has been renamed during clone: %s", ns, err)

			case errors.Is(err, catalog.ErrTimeseriesUnsupported):
				lg.Warnf("Timeseries is not supported (%q)", ns)

			default:
				updateLog := lg.With(
					log.Size(progressUpdate.SizeBytes),
					log.Count(int64(progressUpdate.Count)),
					log.Elapsed(time.Since(lastLogAt)))

				if errors.Is(err, context.Canceled) {
					updateLog.Errorf(err, "Copy documents for collection %q is canceled", ns)
				} else {
					updateLog.Errorf(err, "Failed to copy documents for collection %q", ns)
				}

				return errors.Wrap(err, ns.Collection)
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

	c.lock.Lock()
	diff := c.sizeMap[ns.String()].Size - totalCopiedSizeBytes
	c.totalSize -= diff // adjust
	totalSize := c.totalSize
	delete(c.sizeMap, ns.String())
	c.lock.Unlock()

	metrics.SetEstimatedTotalSizeBytes(totalSize)

	elapsed := time.Since(startedAt)
	lg.With(
		log.Size(totalCopiedSizeBytes),
		log.Count(totalCopiedCount),
		log.Elapsed(elapsed),
	).Infof("Collection %q cloned: %s in %s (%d documents)",
		ns, humanize.Bytes(totalCopiedSizeBytes),
		elapsed.Round(time.Second), totalCopiedCount)

	if diff != 0 {
		copyLogger.With(log.Size(totalSize)).
			Infof("Estimated Total Size %s [updated]", humanize.Bytes(totalSize))
	}

	return nil
}

type sizeMap map[string]sizeMapElem

type sizeMapElem struct {
	UUID  *bson.Binary
	Size  uint64
	Count int64
}

func (c *Clone) collectSizeMap(ctx context.Context) error {
	lg := log.Ctx(ctx)

	databases, err := topo.ListDatabaseNames(ctx, c.source)
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
			collSpecs, err := topo.ListCollectionSpecs(dbGrpCtx, c.source, db)
			if err != nil {
				return errors.Wrap(err, "listCollections")
			}

			collGrp, collGrpCtx := errgroup.WithContext(dbGrpCtx)
			collGrp.SetLimit(runtime.NumCPU() * 2) //nolint:mnd

			for _, spec := range collSpecs {
				if spec.Type == topo.TypeTimeseries {
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
					if spec.Type == topo.TypeView {
						mu.Lock()
						sm[ns] = sizeMapElem{}
						mu.Unlock()

						return nil
					}

					stats, err := topo.GetCollStats(collGrpCtx, c.source, db, spec.Name)
					if err != nil {
						if errors.Is(err, topo.ErrNotFound) {
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
	spec *topo.CollectionSpecification,
) error {
	if spec.Type == topo.TypeTimeseries {
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
	indexes, err := topo.ListIndexes(ctx, c.source, ns.Database, ns.Collection)
	if err != nil {
		return errors.Wrap(err, "list indexes")
	}

	unfinishedBuilds, err := topo.ListInProgressIndexBuilds(ctx,
		c.source, ns.Database, ns.Collection)
	if err != nil {
		return errors.Wrap(err, "list in-progress index builds")
	}

	inconsistentIndexes, err := topo.ListInconsistentIndexes(ctx,
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

	builtIndexesCap := max(len(indexes)-len(unfinishedBuilds)-len(inconsistentIndexes), 0)

	builtIndexes := make([]*topo.IndexSpecification, 0, builtIndexesCap)
	incompleteIndexes := make([]*topo.IndexSpecification, 0, len(unfinishedBuilds))
	inconsistentIdxSpecs := make([]*topo.IndexSpecification, 0, len(inconsistentIndexes))

	for _, index := range indexes {
		switch {
		case slices.Contains(unfinishedBuilds, index.Name):
			incompleteIndexes = append(incompleteIndexes, index)
		case slices.Contains(inconsistentIndexes, index.Name):
			inconsistentIdxSpecs = append(inconsistentIdxSpecs, index)
		default:
			builtIndexes = append(builtIndexes, index)
		}
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

	if len(inconsistentIdxSpecs) != 0 {
		c.catalog.AddInconsistentIndexes(ctx, ns.Database, ns.Collection, inconsistentIdxSpecs)
	}

	return nil
}
