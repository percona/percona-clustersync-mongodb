package repl

import (
	"context"
	"math"
	"slices"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona-lab/percona-mongolink/errors"
)

type (
	DBName   = string
	CollName = string
)

type IndexSpecification struct {
	Name         CollName `bson:"name"`
	Namespace    string   `bson:"ns"`
	KeysDocument bson.Raw `bson:"key"`
	Version      int32    `bson:"v"`
	ExpireAfter  *int32   `bson:"expireAfterSeconds,omitempty"`
	Sparse       *bool    `bson:"sparse,omitempty"`
	Unique       *bool    `bson:"unique,omitempty"`
	Clustered    *bool    `bson:"clustered,omitempty"`
	Hidden       *bool    `bson:"hidden,omitempty"`

	Weights          any                `bson:"weights,omitempty"`
	DefaultLanguage  *string            `bson:"default_language,omitempty"`
	LanguageOverride *string            `bson:"language_override,omitempty"`
	TextVersion      *int32             `bson:"textIndexVersion,omitempty"`
	Collation        *options.Collation `bson:"collation,omitempty"`

	PartialFilterExpression any `bson:"partialFilterExpression,omitempty"`
}

func (s *IndexSpecification) isClustered() bool {
	return s.Clustered != nil && *s.Clustered
}

type Catalog struct {
	mu sync.Mutex

	cat map[DBName]map[CollName][]IndexSpecification
}

func NewCatalog() *Catalog {
	return &Catalog{cat: make(map[DBName]map[CollName][]IndexSpecification)}
}

func (c *Catalog) CreateIndexes(db DBName, coll CollName, indexes []IndexSpecification) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.cat[db]; !ok {
		c.cat[db] = make(map[CollName][]IndexSpecification)
	}

	c.cat[db][coll] = append(c.cat[db][coll], indexes...)
}

func (c *Catalog) CreateIndex(db DBName, coll CollName, index IndexSpecification) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.cat[db]; !ok {
		c.cat[db] = make(map[CollName][]IndexSpecification)
	}

	c.cat[db][coll] = append(c.cat[db][coll], index)
}

func (c *Catalog) DropIndex(db DBName, coll CollName, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.cat[db]; !ok {
		return
	}

	c.cat[db][coll] = slices.DeleteFunc(c.cat[db][coll], func(index IndexSpecification) bool {
		return index.Name == name
	})
}

func (c *Catalog) DropCollection(db DBName, coll CollName) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.cat[db], coll)
}

func (c *Catalog) DropDatabase(db DBName) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.cat, db)
}

func (c *Catalog) BuildCollectionIndexes(
	ctx context.Context,
	m *mongo.Client,
	db DBName,
	coll CollName,
) error {
	return buildIndexes(ctx, m, db, coll, c.cat[db][coll])
}

func (c *Catalog) FinalizeIndexes(ctx context.Context, m *mongo.Client) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for db, colls := range c.cat {
		for coll, indexes := range colls {
			for _, index := range indexes {
				if index.ExpireAfter == nil {
					continue
				}
				if index.isClustered() {
					continue // clustered index with ttl is not supported
				}

				res := m.Database(db).RunCommand(ctx, bson.D{
					{"collMod", coll},
					{"index", bson.D{
						{"name", index.Name},
						{"expireAfterSeconds", *index.ExpireAfter},
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

type InvalidFieldError struct {
	Name string
}

func (e InvalidFieldError) Error() string {
	return "invalid field: " + e.Name
}

type TimeseriesError struct {
	NS Namespace
}

func (e TimeseriesError) Error() string {
	return "unsupported timeseries: " + e.NS.String()
}

func createView(
	ctx context.Context,
	m *mongo.Client,
	db DBName,
	viewName CollName,
	opts *createCollectionOptions,
) error {
	if strings.HasPrefix(opts.ViewOn, "system.buckets.") {
		return TimeseriesError{Namespace{db, viewName}}
	}

	err := m.Database(db).CreateView(ctx,
		viewName,
		opts.ViewOn,
		opts.Pipeline,
		options.CreateView().SetCollation(opts.Collation))
	return errors.Wrap(err, "create view")
}

func createCollection(
	ctx context.Context,
	m *mongo.Client,
	db DBName,
	collName CollName,
	opts *createCollectionOptions,
) error {
	cmd := bson.D{{"create", collName}}
	if opts.ClusteredIndex != nil {
		cmd = append(cmd, bson.E{"clusteredIndex", opts.ClusteredIndex})
	} else {
		cmd = append(cmd, bson.E{"idIndex", opts.IDIndex})
	}

	if opts.Capped {
		cmd = append(cmd, bson.E{"capped", opts.Capped})
		if opts.Size != 0 {
			cmd = append(cmd, bson.E{"size", opts.Size})
		}
		if opts.Max != 0 {
			cmd = append(cmd, bson.E{"max", opts.Max})
		}
	}

	if opts.Collation != nil {
		cmd = append(cmd, bson.E{"collation", opts.Collation.ToDocument()}) //nolint:staticcheck
	}

	res := m.Database(db).RunCommand(ctx, cmd)
	return errors.Wrap(res.Err(), "create collection")
}

func dropCollection(ctx context.Context, m *mongo.Client, db DBName, collName string) error {
	err := m.Database(db).Collection(collName).Drop(ctx)
	return errors.Wrap(err, "drop collection")
}

func buildIndexes(
	ctx context.Context,
	m *mongo.Client,
	db DBName,
	coll CollName,
	indexes []IndexSpecification,
) error {
	models := make([]mongo.IndexModel, len(indexes))
	for i, index := range indexes {
		var expireAfter *int32
		if index.ExpireAfter != nil {
			maxInt32 := int32(math.MaxInt32)
			expireAfter = &maxInt32
		}

		models[i] = mongo.IndexModel{
			Keys: index.KeysDocument,
			Options: &options.IndexOptions{
				Name:    &index.Name,
				Version: &index.Version,
				Unique:  index.Unique,
				Sparse:  index.Sparse,
				Hidden:  index.Hidden,

				ExpireAfterSeconds:      expireAfter,
				PartialFilterExpression: index.PartialFilterExpression,

				Weights:          index.Weights,
				DefaultLanguage:  index.DefaultLanguage,
				LanguageOverride: index.LanguageOverride,
				TextVersion:      index.TextVersion,
				Collation:        index.Collation,
			},
		}
	}

	if len(models) == 0 {
		return nil
	}

	_, err := m.Database(db).Collection(coll).Indexes().CreateMany(ctx, models)
	if err != nil {
		return err //nolint:wrapcheck
	}

	return nil
}
