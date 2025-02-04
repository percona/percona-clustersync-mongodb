package repl

import (
	"context"
	"runtime"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"

	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
	"github.com/percona-lab/percona-mongolink/topo"
)

type collSpec struct {
	DB string
	mongo.CollectionSpecification
}

type IndexSpecification struct {
	Name               string   `bson:"name"`
	Namespace          string   `bson:"ns"`
	KeysDocument       bson.Raw `bson:"key"`
	Version            int32    `bson:"v"`
	ExpireAfterSeconds *int32   `bson:"expireAfterSeconds,omitempty"`
	Sparse             *bool    `bson:"sparse,omitempty"`
	Unique             *bool    `bson:"unique,omitempty"`
	Clustered          *bool    `bson:"clustered,omitempty"`
	Hidden             *bool    `bson:"hidden,omitempty"`

	PartialFilterExpression any `bson:"partialFilterExpression,omitempty"`

	Collation *options.Collation `bson:"collation,omitempty"`
}

func (s *IndexSpecification) isClustered() bool {
	return s.Clustered != nil && *s.Clustered
}

func (s *collSpec) ns() string {
	return s.DB + "." + s.Name
}

type dataCloner struct {
	Source *mongo.Client
	Target *mongo.Client

	Drop         bool
	IsSelected   FilterFunc
	IndexCatalog *IndexCatalog

	specs     map[string][]*collSpec
	startedAt primitive.Timestamp

	mu sync.Mutex
}

func (c *dataCloner) StartedAt() primitive.Timestamp {
	c.mu.Lock()
	rv := c.startedAt
	c.mu.Unlock()
	return rv
}

func (c *dataCloner) init(ctx context.Context) error {
	ctx = log.WithAttrs(ctx, log.Scope("dataCloner.init"))
	var err error
	c.startedAt, err = topo.ClusterTime(ctx, c.Source)
	if err != nil {
		return errors.Wrap(err, "get cluster time")
	}

	databases, err := c.Source.ListDatabases(ctx, bson.D{})
	if err != nil {
		return errors.Wrap(err, "list databases")
	}

	nsCatalog := make(map[string][]*collSpec)

	mu := sync.Mutex{}
	grp, grpCtx := errgroup.WithContext(ctx)
	grp.SetLimit(runtime.NumCPU())

	for _, db := range databases.Databases {
		switch db.Name {
		case "admin", "config", "local":
			continue
		}

		grp.Go(func() error {
			colls, err := c.Source.Database(db.Name).ListCollectionSpecifications(grpCtx, bson.D{})
			if err != nil {
				return errors.Wrap(err, "list collections")
			}

			dbColls := make([]*collSpec, 0, len(colls))
			for _, coll := range colls {
				if strings.HasPrefix(coll.Name, "system.") {
					continue
				}
				if !c.IsSelected(db.Name, coll.Name) {
					log.Trace(log.WithAttrs(ctx, log.NS(db.Name, coll.Name)), "not selected")
					continue
				}

				dbColls = append(dbColls, &collSpec{
					DB:                      db.Name,
					CollectionSpecification: *coll,
				})

				if coll.Type != "collection" {
					continue
				}

				cur, err := c.Source.Database(db.Name).Collection(coll.Name).Indexes().List(grpCtx)
				if err != nil {
					return errors.Wrap(err, "list indexes")
				}

				var indexes []IndexSpecification
				err = cur.All(grpCtx, &indexes)
				if err != nil {
					return errors.Wrap(err, "decode indexes")
				}

				if coll.IDIndex == nil {
					// make sure no TTL is set up for clustered index.
					// reason: change stream does not provide TTL value for the clustered index.
					for i := range indexes {
						if idx := indexes[i]; idx.isClustered() && idx.ExpireAfterSeconds != nil {
							log.Warn(ctx, "clustered index with time-to-live is not supported. "+
								"creating clustered index without ttl value")
							idx.ExpireAfterSeconds = nil
						}
					}
				}

				c.IndexCatalog.CreateIndexes(db.Name, coll.Name, indexes)
			}

			mu.Lock()
			nsCatalog[db.Name] = dbColls
			mu.Unlock()
			return nil
		})
	}

	err = grp.Wait()
	if err != nil {
		return errors.Wrap(err, "get namespaces specs")
	}

	c.mu.Lock()
	c.specs = nsCatalog
	c.mu.Unlock()

	return nil
}

func (c *dataCloner) Clone(ctx context.Context) error {
	ctx = log.WithAttrs(ctx, log.Scope("dataCloner.Clone"))
	err := c.init(ctx)
	if err != nil {
		return errors.Wrap(err, "init")
	}

	errGrp, grpCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(runtime.NumCPU())

	for _, dbSpecs := range c.specs {
		for _, spec := range dbSpecs {
			errGrp.Go(func() error {
				ctx := log.WithAttrs(grpCtx, log.NS(spec.DB, spec.Name))
				log.Tracef(ctx, "")

				var err error
				switch spec.Type {
				case "collection":
					err = c.cloneCollection(grpCtx, spec)
				case "view":
					err = c.cloneView(ctx, spec)
				case "timeseries":
					log.Warn(ctx, "timeseries is not supported. skip")
				}
				if err != nil {
					return errors.Wrap(err, "clone "+spec.ns())
				}

				return nil
			})
		}
	}

	return errGrp.Wait() //nolint:wrapcheck
}

func (c *dataCloner) BuildIndexes(ctx context.Context) error {
	for _, dbSpecs := range c.specs {
		for _, spec := range dbSpecs {
			for index := range c.IndexCatalog.CollectionIndexes(spec.DB, spec.Name) {
				if spec.IDIndex == nil {
					if index.isClustered() {
						continue
					}
				} else if spec.IDIndex.Name == index.Name {
					continue
				}

				model := mongo.IndexModel{
					Keys: index.KeysDocument,
					Options: &options.IndexOptions{
						Name:    &index.Name,
						Version: &index.Version,
						Unique:  index.Unique,
						Sparse:  index.Sparse,
						Hidden:  index.Hidden,

						PartialFilterExpression: index.PartialFilterExpression,

						Collation: index.Collation,
					},
				}

				_, err := c.Target.Database(spec.DB).Collection(spec.Name).
					Indexes().CreateOne(ctx, model)
				if err != nil {
					return errors.Wrap(err, "create index: "+index.Name)
				}
			}
		}
	}

	return nil
}

func (c *dataCloner) FinalizeIndexes(ctx context.Context) error {
	for _, dbSpecs := range c.specs {
		for _, spec := range dbSpecs {
			for index := range c.IndexCatalog.CollectionIndexes(spec.DB, spec.Name) {
				if index.ExpireAfterSeconds == nil || *index.ExpireAfterSeconds <= 0 {
					continue
				}
				if index.isClustered() {
					continue // clustered index with ttl is not supported
				}

				res := c.Target.Database(spec.DB).RunCommand(ctx, bson.D{
					{"collMod", spec.Name},
					{"index", bson.D{
						{"name", index.Name},
						{"expireAfterSeconds", *index.ExpireAfterSeconds},
					}},
				})
				if err := res.Err(); err != nil {
					return errors.Wrap(err, "convert index: "+index.Name)
				}
			}
		}
	}

	return nil
}

func (c *dataCloner) cloneCollection(ctx context.Context, spec *collSpec) error {
	log.Debug(ctx, "cloning collection")

	if c.Drop {
		err := c.Target.Database(spec.DB).Collection(spec.Name).Drop(ctx)
		if err != nil {
			return errors.Wrap(err, "drop")
		}
	}

	var options createEventOptions
	err := bson.Unmarshal(spec.Options, &options)
	if err != nil {
		return errors.Wrap(err, "unmarshal options")
	}

	err = createCollection(ctx, c.Target, spec.DB, spec.Name, &options)
	if err != nil {
		return errors.Wrap(err, "create collection")
	}

	cur, err := c.Source.Database(spec.DB).Collection(spec.Name).
		Find(ctx, bson.D{})
	if err != nil {
		return errors.Wrap(err, "find")
	}
	defer cur.Close(ctx)

	targetColl := c.Target.Database(spec.DB).Collection(spec.Name)
	for cur.Next(ctx) {
		_, err = targetColl.InsertOne(ctx, cur.Current)
		if err != nil {
			return errors.Wrap(err, "insert one")
		}
	}

	err = cur.Err()
	if err != nil {
		return errors.Wrap(err, "cloning failed "+spec.ns())
	}

	log.Info(ctx, "cloned collection")
	return nil
}

func (c *dataCloner) cloneView(ctx context.Context, spec *collSpec) error {
	log.Debug(ctx, "cloning view")

	if c.Drop {
		err := c.Target.Database(spec.DB).Collection(spec.Name).Drop(ctx)
		if err != nil {
			return errors.Wrap(err, "drop")
		}
	}

	var options createEventOptions
	err := bson.Unmarshal(spec.Options, &options)
	if err != nil {
		return errors.Wrap(err, "unmarshal options")
	}

	err = createView(ctx, c.Target, spec.DB, spec.Name, &options)
	if err != nil {
		return errors.Wrap(err, "create view")
	}

	log.Info(ctx, "cloned view")
	return nil
}
