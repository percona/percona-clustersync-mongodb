package mongolink

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"golang.org/x/sync/errgroup"

	"github.com/percona-lab/percona-mongolink/config"
	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
	"github.com/percona-lab/percona-mongolink/sel"
	"github.com/percona-lab/percona-mongolink/topo"
)

// Clone handles the cloning of data from a source MongoDB to a target MongoDB.
type Clone struct {
	source   *mongo.Client // Source MongoDB client
	target   *mongo.Client // Target MongoDB client
	catalog  *Catalog      // Catalog for managing collections and indexes
	nsFilter sel.NSFilter  // Namespace filter

	lock sync.Mutex
	err  error // Error encountered during the cloning process

	doneSig chan struct{}

	totalSize  int64        // Estimated total bytes to be cloned
	clonedSize atomic.Int64 // Bytes cloned so far

	startTS  bson.Timestamp // source cluster timestamp when cloning started
	finishTS bson.Timestamp // source cluster timestamp when cloning completed

	startTime  time.Time
	finishTime time.Time
}

// CloneStatus represents the status of the cloning process.
type CloneStatus struct {
	EstimatedTotalSize int64 // Estimated total bytes to be copied
	CopiedSize         int64 // Bytes copied so far

	StartTS  bson.Timestamp
	FinishTS bson.Timestamp

	StartTime  time.Time
	FinishTime time.Time

	Err error // Error encountered during the cloning process
}

//go:inline
func (cs *CloneStatus) IsStarted() bool {
	return !cs.StartTime.IsZero()
}

//go:inline
func (cs *CloneStatus) IsRunning() bool {
	return cs.IsStarted() && !cs.IsFinished()
}

//go:inline
func (cs *CloneStatus) IsFinished() bool {
	return !cs.FinishTime.IsZero()
}

func NewClone(source, target *mongo.Client, catalog *Catalog, nsFilter sel.NSFilter) *Clone {
	return &Clone{
		source:   source,
		target:   target,
		catalog:  catalog,
		nsFilter: nsFilter,
		doneSig:  make(chan struct{}),
	}
}

// Status returns the current status of the cloning process.
func (c *Clone) Status() CloneStatus {
	c.lock.Lock()
	defer c.lock.Unlock()

	return CloneStatus{
		EstimatedTotalSize: c.totalSize,
		CopiedSize:         c.clonedSize.Load(),
		StartTS:            c.startTS,
		FinishTS:           c.finishTS,
		StartTime:          c.startTime,
		FinishTime:         c.finishTime,
		Err:                c.err,
	}
}

func (c *Clone) Done() <-chan struct{} {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.doneSig
}

// Start starts the cloning process.
func (c *Clone) Start(_ context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	lg := log.New("clone:start")

	if c.err != nil {
		return errors.Wrap(c.err, "cannot start due an existing error")
	}

	if !c.finishTime.IsZero() {
		return errors.New("already completed")
	}

	if !c.startTime.IsZero() {
		return errors.New("already started")
	}

	lg.Info("Starting data cloning")

	c.startTime = time.Now()

	go func() {
		err := c.run()

		c.lock.Lock()
		defer c.lock.Unlock()

		if err != nil && !errors.Is(err, ErrStopSignal) {
			c.err = err
		}

		select {
		case <-c.doneSig:
		default:
			close(c.doneSig)
		}

		c.finishTime = time.Now()

		lg.With(log.Elapsed(c.finishTime.Sub(c.startTime))).
			Info("Data cloning completed")
	}()

	return nil
}

var ErrStopSignal = errors.New("stop")

func (c *Clone) run() error {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	go func() {
		select {
		case <-ctx.Done():
			return

		case <-c.doneSig:
			cancel(ErrStopSignal)
		}
	}()

	lg := log.New("clone:run")

	startedTS, err := topo.ClusterTime(ctx, c.source)
	if err != nil {
		return errors.Wrap(err, "startetTS: get source cluster time")
	}

	c.lock.Lock()
	c.startTS = startedTS
	c.lock.Unlock()

	databases, err := topo.ListDatabaseNames(ctx, c.source)
	if err != nil {
		return errors.Wrap(err, "list databases")
	}

	// XXX: Should the total size be adjusted on collection/database drop?
	// FIXME: calculate differently for selective
	totalSize, err := c.getTotalSize(ctx, databases)
	if err != nil {
		return errors.Wrap(err, "get database stats")
	}

	c.lock.Lock()
	c.totalSize = totalSize
	c.lock.Unlock()

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.SetLimit(runtime.GOMAXPROCS(0))

	for _, db := range databases {
		collections, err := topo.ListCollectionNames(grpCtx, c.source, db)
		if err != nil {
			return errors.Wrap(err, "list collection names")
		}

		for _, collName := range collections {
			if strings.HasPrefix(collName, SystemPrefix) {
				continue
			}

			if !c.nsFilter(db, collName) {
				lg.With(log.NS(db, collName)).Debug("not selected")

				continue
			}

			grp.Go(func() error {
				return c.cloneCollection(grpCtx, db, collName)
			})
		}
	}

	err = grp.Wait()
	if err != nil {
		return errors.Wrap(err, "cloning")
	}

	completedTS, err := topo.ClusterTime(ctx, c.source)
	if err != nil {
		return errors.Wrap(err, "completedAt: get source cluster timestamp")
	}

	c.lock.Lock()
	c.finishTS = completedTS
	c.lock.Unlock()

	return nil
}

// getTotalSize calculates the total size of the databases to be cloned.
func (c *Clone) getTotalSize(ctx context.Context, databases []string) (int64, error) {
	var total atomic.Int64

	eg, grpCtx := errgroup.WithContext(ctx)

	for _, db := range databases {
		eg.Go(func() error {
			dbStats, err := topo.GetDBStats(grpCtx, c.source, db)
			if err != nil {
				return errors.Wrap(err, db)
			}

			total.Add(dbStats.DataSize)

			return nil
		})
	}

	err := eg.Wait()

	return total.Load(), err //nolint:wrapcheck
}

// cloneCollection clones a collection (or view) from the source to the target.
func (c *Clone) cloneCollection(ctx context.Context, db, coll string) error {
	lg := log.Ctx(ctx).With(log.NS(db, coll))
	lg.Infof("Starting Cloning %s.%s", db, coll)

	spec, err := topo.GetCollectionSpec(ctx, c.source, db, coll)
	if err != nil {
		return errors.Wrap(err, "collection not found")
	}

	startedAt := time.Now()

	if spec.Type == "timeseries" {
		lg.Warn("Timeseries is not supported. skipping")

		return nil
	}

	err = c.catalog.DropCollection(ctx, db, spec.Name)
	if err != nil {
		return errors.Wrap(err, "ensure no collection before create")
	}

	var options CreateCollectionOptions

	err = bson.Unmarshal(spec.Options, &options)
	if err != nil {
		return errors.Wrap(err, "unmarshal options")
	}

	err = c.catalog.CreateCollection(ctx, db, spec.Name, &options)
	if err != nil {
		return errors.Wrap(err, "create collection")
	}

	if spec.Type == "view" {
		lg.With(log.Elapsed(time.Since(startedAt))).
			Infof("Cloned %s.%s", db, spec.Name)

		return nil
	}

	indexes, err := topo.ListIndexes(ctx, c.source, db, spec.Name)
	if err != nil {
		return errors.Wrap(err, "list indexes")
	}

	err = c.catalog.CreateIndexes(ctx, db, spec.Name, indexes)
	if err != nil {
		return errors.Wrap(err, "build collection indexes")
	}

	cur, err := c.source.Database(db).Collection(spec.Name).Find(ctx, bson.D{})
	if err != nil {
		return errors.Wrap(err, "find")
	}

	defer func() {
		err := cur.Close(ctx)
		if err != nil {
			lg.Error(err, "Close find cursor")
		}
	}()

	const initialBufferSize = 1000
	docs := make([]any, 0, initialBufferSize)
	batch := 1
	batchSize := 0

	targetColl := c.target.Database(db).Collection(spec.Name)

	for cur.Next(ctx) {
		if batchSize+len(cur.Current) > config.MaxCollectionCloneBatchSize {
			_, err = targetColl.InsertMany(ctx, docs)
			if err != nil {
				return errors.Wrap(err, "insert documents")
			}

			c.clonedSize.Add(int64(batchSize))

			lg.Unwrap().Trace().
				Int("count", len(docs)).
				Int("size", batchSize).
				Msgf("insert batch #%d", batch)

			docs = docs[:0]
			batch++
			batchSize = 0
		}

		docs = append(docs, cur.Current)
		batchSize += len(cur.Current)
	}

	err = cur.Err()
	if err != nil {
		return errors.Wrapf(err, "cloning failed %s.%s", db, spec.Name)
	}

	if len(docs) > 0 {
		_, err = targetColl.InsertMany(ctx, docs)
		if err != nil {
			return errors.Wrap(err, "insert documents")
		}

		c.clonedSize.Add(int64(batchSize))

		lg.Unwrap().Trace().
			Int("count", len(docs)).
			Int("size", batchSize).
			Msgf("insert batch #%d", batch)
	}

	lg.With(log.Elapsed(time.Since(startedAt))).
		Infof("Cloned %s.%s", db, spec.Name)

	return nil
}
