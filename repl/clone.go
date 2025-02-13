package repl

import (
	"context"
	"runtime"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/sync/errgroup"

	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
)

type DataCloner struct {
	Source   *mongo.Client
	Target   *mongo.Client
	Drop     bool
	NSFilter NSFilter
	Catalog  *Catalog
}

func (c *DataCloner) Clone(ctx context.Context) error {
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
				log.Tracef(ctx, "")

				var err error
				switch spec.Type {
				case "collection":
					err = c.cloneCollection(ctx, db, spec)
				case "view":
					err = c.cloneView(ctx, db, spec)
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

	return grp.Wait() //nolint:wrapcheck
}

func (c *DataCloner) cloneCollection(
	ctx context.Context,
	db string,
	spec *mongo.CollectionSpecification,
) error {
	log.Debug(ctx, "cloning collection")

	cur, err := c.Source.Database(db).Collection(spec.Name).Indexes().List(ctx)
	if err != nil {
		return errors.Wrap(err, "list indexes")
	}

	var indexes []IndexSpecification
	err = cur.All(ctx, &indexes)
	if err != nil {
		return errors.Wrap(err, "decode indexes")
	}

	for i := range indexes {
		if spec.IDIndex == nil {
			if indexes[i].isClustered() {
				continue
			}
		} else if spec.IDIndex.Name == indexes[i].Name {
			continue
		}

		c.Catalog.CreateIndex(db, spec.Name, indexes[i])
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

	err = createCollection(ctx, c.Target, db, spec.Name, &options)
	if err != nil {
		return errors.Wrap(err, "create collection")
	}

	err = c.Catalog.BuildCollectionIndexes(ctx, c.Target, db, spec.Name)
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
	}

	err = cur.Err()
	if err != nil {
		return errors.Wrapf(err, "cloning failed %s.%s", db, spec.Name)
	}

	log.Info(ctx, "cloned collection")
	return nil
}

func (c *DataCloner) cloneView(
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

	err = createView(ctx, c.Target, db, spec.Name, &options)
	if err != nil {
		return errors.Wrap(err, "create view")
	}

	log.Info(ctx, "cloned view")
	return nil
}
