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

	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
	"github.com/percona-lab/percona-mongolink/sel"
)

// Clone handles the cloning of data from a source MongoDB to a target MongoDB.
type Clone struct {
	Source *mongo.Client // Source MongoDB client
	Target *mongo.Client // Target MongoDB client

	NSFilter sel.NSFilter // Namespace filter
	Catalog  *Catalog     // Catalog for managing collections and indexes

	estimatedTotalBytes  atomic.Int64 // Estimated total bytes to be cloned
	estimatedClonedBytes atomic.Int64 // Estimated bytes cloned so far

	err      error // Error encountered during the cloning process
	finished bool  // Indicates if the cloning process is finished

	mu sync.Mutex
}

// CloneStatus represents the status of the cloning process.
type CloneStatus struct {
	EstimatedTotalBytes  int64 // Estimated total bytes to be cloned
	EstimatedClonedBytes int64 // Estimated bytes cloned so far

	Error    error // Error encountered during the cloning process
	Finished bool  // Indicates if the cloning process is finished
}

// Status returns the current status of the cloning process.
func (c *Clone) Status() CloneStatus {
	c.mu.Lock()
	defer c.mu.Unlock()

	return CloneStatus{
		EstimatedTotalBytes:  c.estimatedTotalBytes.Load(),
		EstimatedClonedBytes: c.estimatedClonedBytes.Load(),
		Error:                c.err,
		Finished:             c.finished,
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

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.SetLimit(runtime.NumCPU())

	for _, db := range databases {
		rawDBStats, err := c.Source.Database(db).RunCommand(grpCtx, bson.D{{"dbStats", 1}}).Raw()
		if err != nil {
			return errors.Wrap(err, "get database stats")
		}

		dbSize, ok := rawDBStats.Lookup("dataSize").AsInt64OK()
		if ok {
			c.estimatedTotalBytes.Add(dbSize)
		}

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
				lg := lg.With(log.NS(db, spec.Name))
				lg.Infof("cloning %s.%s", db, spec.Name)

				startedAt := time.Now()

				var err error

				switch spec.Type {
				case "collection":
					err = c.cloneCollection(lg.WithContext(grpCtx), db, &spec)
				case "view":
					err = c.cloneView(lg.WithContext(grpCtx), db, &spec)
				case "timeseries":
					lg.Warn("timeseries is not supported. skip")
				}

				if err != nil {
					return errors.Wrapf(err, "clone %s.%s", db, spec.Name)
				}

				lg.InfoWith(fmt.Sprintf("cloned %s.%s", db, spec.Name),
					log.Elapsed(time.Since(startedAt)))

				return nil
			})
		}
	}

	err = grp.Wait()

	c.mu.Lock()
	c.err = err
	c.finished = true
	c.mu.Unlock()

	return err //nolint:wrapcheck
}

// cloneCollection clones a collection from the source to the target.
func (c *Clone) cloneCollection(
	ctx context.Context,
	db string,
	spec *mongo.CollectionSpecification,
) error {
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

	targetColl := c.Target.Database(db).Collection(spec.Name)
	for cur.Next(ctx) {
		_, err = targetColl.InsertOne(ctx, cur.Current)
		if err != nil {
			return errors.Wrap(err, "insert one")
		}

		c.estimatedClonedBytes.Add(int64(len(cur.Current)))
	}

	err = cur.Err()
	if err != nil {
		return errors.Wrapf(err, "cloning failed %s.%s", db, spec.Name)
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
