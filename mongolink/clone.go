package mongolink

import (
	"context"
	"fmt"
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
	Source *mongo.Client // Source MongoDB client
	Target *mongo.Client // Target MongoDB client

	NSFilter sel.NSFilter // Namespace filter
	Catalog  *Catalog     // Catalog for managing collections and indexes

	totalSize  int64        // Estimated total bytes to be cloned
	clonedSize atomic.Int64 // Bytes cloned so far

	err       error // Error encountered during the cloning process
	completed bool  // Indicates if the cloning process is completed

	mu sync.Mutex
}

// CloneStatus represents the status of the cloning process.
type CloneStatus struct {
	EstimatedTotalSize int64 // Estimated total bytes to be copied
	CopiedSize         int64 // Bytes copied so far

	Error     error // Error encountered during the cloning process
	Completed bool  // Indicates if the cloning process is completed
}

// Status returns the current status of the cloning process.
func (c *Clone) Status() CloneStatus {
	c.mu.Lock()
	defer c.mu.Unlock()

	return CloneStatus{
		EstimatedTotalSize: c.totalSize,
		CopiedSize:         c.clonedSize.Load(),

		Error:     c.err,
		Completed: c.completed,
	}
}

// Clone starts the cloning process.
func (c *Clone) Clone(ctx context.Context) error {
	lg := log.New("clone")
	ctx = lg.WithContext(ctx)

	databases, err := c.Source.ListDatabaseNames(ctx, bson.D{
		{"name", bson.D{{"$nin", bson.A{"local", "admin", "config"}}}},
	})
	if err != nil {
		return errors.Wrap(err, "list databases")
	}

	// XXX: Should the total size be adjusted on collection/database drop?
	totalSize, err := c.getTotalSize(ctx, databases)
	if err != nil {
		return errors.Wrap(err, "get database stats")
	}

	c.mu.Lock()
	c.totalSize = totalSize
	c.mu.Unlock()

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.SetLimit(runtime.GOMAXPROCS(0))

	for _, db := range databases {
		colls, err := c.Source.Database(db).ListCollectionSpecifications(grpCtx, bson.D{})
		if err != nil {
			return errors.Wrap(err, "list collections")
		}

		for _, spec := range colls {
			if strings.HasPrefix(spec.Name, "system.") {
				continue
			}

			if !c.NSFilter(db, spec.Name) {
				lg.With(log.NS(db, spec.Name)).Debug("not selected")

				continue
			}

			grp.Go(func() error {
				return c.cloneNamespace(ctx, db, &spec)
			})
		}
	}

	err = grp.Wait()

	c.mu.Lock()
	c.err = err
	c.completed = true
	c.mu.Unlock()

	return err //nolint:wrapcheck
}

// getTotalSize calculates the total size of the databases to be cloned.
func (c *Clone) getTotalSize(ctx context.Context, databases []string) (int64, error) {
	var total atomic.Int64

	eg, grpCtx := errgroup.WithContext(ctx)

	for _, db := range databases {
		eg.Go(func() error {
			dbStats, err := topo.GetDBStats(grpCtx, c.Source, db)
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

// cloneNamespace clones a specific namespace (collection or view) from the source to the target.
func (c *Clone) cloneNamespace(
	ctx context.Context,
	db string,
	spec *mongo.CollectionSpecification,
) error {
	lg := log.Ctx(ctx).With(log.NS(db, spec.Name))
	lg.Infof("Cloning %s.%s", db, spec.Name)

	var err error

	startedAt := time.Now()

	switch spec.Type {
	case "collection":
		err = c.cloneCollection(lg.WithContext(ctx), db, spec)
	case "view":
		err = c.cloneView(lg.WithContext(ctx), db, spec)
	case "timeseries":
		lg.Warn("Timeseries is not supported. skipping")
	}

	if err != nil {
		return errors.Wrapf(err, "clone %s.%s", db, spec.Name)
	}

	lg.InfoWith(fmt.Sprintf("Cloned %s.%s", db, spec.Name), log.Elapsed(time.Since(startedAt)))

	return nil
}

// cloneCollection clones a collection from the source to the target.
func (c *Clone) cloneCollection(
	ctx context.Context,
	db string,
	spec *mongo.CollectionSpecification,
) error {
	lg := log.Ctx(ctx)

	cur, err := c.Source.Database(db).Collection(spec.Name).Indexes().List(ctx)
	if err != nil {
		return errors.Wrap(err, "list indexes")
	}

	var indexes []*IndexSpecification

	err = cur.All(ctx, &indexes)
	if err != nil {
		return errors.Wrap(err, "decode indexes")
	}

	err = c.Target.Database(db).Collection(spec.Name).Drop(ctx)
	if err != nil {
		return errors.Wrap(err, "drop")
	}

	var options createCollectionOptions

	err = bson.Unmarshal(spec.Options, &options)
	if err != nil {
		return errors.Wrap(err, "unmarshal options")
	}

	err = c.Catalog.CreateCollection(ctx, c.Target, db, spec.Name, &options)
	if err != nil {
		return errors.Wrap(err, "create collection")
	}

	err = c.Catalog.CreateIndexes(ctx, c.Target, db, spec.Name, indexes)
	if err != nil {
		return errors.Wrap(err, "build collection indexes")
	}

	cur, err = c.Source.Database(db).Collection(spec.Name).Find(ctx, bson.D{})
	if err != nil {
		return errors.Wrap(err, "find")
	}
	defer cur.Close(ctx)

	docs := make([]interface{}, 0, 1000)
	batch := 0
	batchSize := 0

	targetColl := c.Target.Database(db).Collection(spec.Name)

	for cur.Next(ctx) {
		if batchSize >= config.MaxCollectionCloneBatchSize {
			_, err = targetColl.InsertMany(ctx, docs)
			if err != nil {
				return errors.Wrap(err, "insert documents")
			}

			c.clonedSize.Add(int64(batchSize))
			lg.Debugf("cloning collection: %s.%s, inserted docs batch: %d, batch size: %d",
				db, spec.Name, batch, batchSize)

			docs = docs[:0]
			batch++
			batchSize = 0

			continue
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
		lg.Debugf("cloning collection: %s.%s, inserted last docs batch: %d, batch size: %d",
			db, spec.Name, batch, batchSize)
	}

	return nil
}

// cloneView clones a view from the source to the target.
func (c *Clone) cloneView(
	ctx context.Context,
	db string,
	spec *mongo.CollectionSpecification,
) error {
	err := c.Target.Database(db).Collection(spec.Name).Drop(ctx)
	if err != nil {
		return errors.Wrap(err, "drop")
	}

	var options createCollectionOptions

	err = bson.Unmarshal(spec.Options, &options)
	if err != nil {
		return errors.Wrap(err, "unmarshal options")
	}

	err = c.Catalog.CreateView(ctx, c.Target, db, spec.Name, &options)
	if err != nil {
		return errors.Wrap(err, "create view")
	}

	return nil
}
