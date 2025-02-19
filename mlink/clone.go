package mlink

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"golang.org/x/sync/errgroup"

	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
	"github.com/percona-lab/percona-mongolink/util"
)

type Clone struct {
	Source   *mongo.Client
	Target   *mongo.Client
	Drop     bool
	NSFilter util.NSFilter
	Catalog  *Catalog

	estimatedTotalBytes  atomic.Int64
	estimatedClonedBytes atomic.Int64
	finished             bool

	mu sync.Mutex
}

type CloneStatus struct {
	Finished             bool
	EstimatedTotalBytes  int64
	EstimatedClonedBytes int64
}

func (c *Clone) Status() CloneStatus {
	c.mu.Lock()
	defer c.mu.Unlock()

	return CloneStatus{
		Finished:             c.finished,
		EstimatedTotalBytes:  c.estimatedTotalBytes.Load(),
		EstimatedClonedBytes: c.estimatedClonedBytes.Load(),
	}
}

func (c *Clone) Clone(ctx context.Context) error {
	ctx = log.WithAttrs(ctx, log.Scope("clone"))

	databases, err := c.Source.ListDatabaseNames(ctx, bson.D{
		{"name", bson.D{{"$nin", bson.A{"local", "admin", "config"}}}},
	})
	if err != nil {
		return errors.Wrap(err, "list databases")
	}

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.SetLimit(runtime.NumCPU())

	for _, db := range databases {
		res := c.Source.Database(db).RunCommand(ctx, bson.D{{"dbStats", 1}})
		b, err := res.Raw()
		if err != nil {
			return errors.Wrap(err, "get database stats")
		}

		dbSize := b.Lookup("dataSize").AsInt64()
		c.estimatedTotalBytes.Add(dbSize)

		colls, err := c.Source.Database(db).ListCollectionSpecifications(ctx, bson.D{})
		if err != nil {
			return errors.Wrap(err, "list collections")
		}

		for _, spec := range colls {
			if strings.HasPrefix(spec.Name, "system.") {
				continue
			}

			if !c.NSFilter(db, spec.Name) {
				log.Trace(log.WithAttrs(ctx, log.NS(db, spec.Name)), "not selected")
				continue
			}

			grp.Go(func() error {
				ctx := log.WithAttrs(grpCtx, log.NS(db, spec.Name))
				log.Trace(ctx, "")

				var err error
				switch spec.Type {
				case "collection":
					err = c.cloneCollection(ctx, db, &spec)
				case "view":
					err = c.cloneView(ctx, db, &spec)
				case "timeseries":
					log.Warn(ctx, "timeseries is not supported. skip")
				}
				if err != nil {
					return errors.Wrapf(err, "clone %s.%s", db, spec.Name)
				}

				return nil
			})
		}
	}

	err = grp.Wait() //nolint:wrapcheck
	if err != nil {
		return errors.Wrap(err, "wait")
	}

	c.mu.Lock()
	c.finished = true
	c.mu.Unlock()
	return nil
}

func (c *Clone) cloneCollection(
	ctx context.Context,
	db string,
	spec *mongo.CollectionSpecification,
) error {
	log.Debug(ctx, "cloning collection")

	cur, err := c.Source.Database(db).Collection(spec.Name).Indexes().List(ctx)
	if err != nil {
		return errors.Wrap(err, "list indexes")
	}

	var indexes []*IndexSpecification
	err = cur.All(ctx, &indexes)
	if err != nil {
		return errors.Wrap(err, "decode indexes")
	}

	if c.Drop {
		err := c.Target.Database(db).Collection(spec.Name).Drop(ctx)
		if err != nil {
			return errors.Wrap(err, "drop")
		}
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

	log.Info(ctx, "cloned collection")
	return nil
}

func (c *Clone) cloneView(
	ctx context.Context,
	db string,
	spec *mongo.CollectionSpecification,
) error {
	log.Debug(ctx, "cloning view")

	if c.Drop {
		err := c.Target.Database(db).Collection(spec.Name).Drop(ctx)
		if err != nil {
			return errors.Wrap(err, "drop")
		}
	}

	var options createCollectionOptions
	err := bson.Unmarshal(spec.Options, &options)
	if err != nil {
		return errors.Wrap(err, "unmarshal options")
	}

	err = c.Catalog.CreateView(ctx, c.Target, db, spec.Name, &options)
	if err != nil {
		return errors.Wrap(err, "create view")
	}

	log.Info(ctx, "created view")
	return nil
}
