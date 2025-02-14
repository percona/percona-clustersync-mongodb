package repl

import (
	"context"
	"math"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona-lab/percona-mongolink/errors"
)

type (
	DBName    = string
	CollName  = string
	IndexName = string
)

type IndexSpecification struct {
	Name               CollName `bson:"name"`
	Namespace          string   `bson:"ns"`
	KeysDocument       bson.Raw `bson:"key"`
	Version            int32    `bson:"v"`
	Sparse             *bool    `bson:"sparse,omitempty"`
	Hidden             *bool    `bson:"hidden,omitempty"`
	Unique             *bool    `bson:"unique,omitempty"`
	PrepareUnique      *bool    `bson:"prepareUnique,omitempty"`
	Clustered          *bool    `bson:"clustered,omitempty"`
	ExpireAfterSeconds *int64   `bson:"expireAfterSeconds,omitempty"`

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

	cat map[DBName]map[CollName]map[IndexName]*IndexSpecification
}

func NewCatalog() *Catalog {
	return &Catalog{cat: make(map[DBName]map[CollName]map[IndexName]*IndexSpecification)}
}

func (c *Catalog) CreateCollection(
	ctx context.Context,
	m *mongo.Client,
	db DBName,
	collName CollName,
	opts *createCollectionOptions,
) error {
	cmd := bson.D{{"create", collName}}
	if opts.ClusteredIndex != nil {
		cmd = append(cmd, bson.E{"clusteredIndex", opts.ClusteredIndex})
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

	err := m.Database(db).RunCommand(ctx, cmd).Err()
	if err != nil {
		return errors.Wrap(err, "create collection")
	}

	return nil
}

func (c *Catalog) CreateView(
	ctx context.Context,
	m *mongo.Client,
	db DBName,
	viewName CollName,
	opts *createCollectionOptions,
) error {
	if strings.HasPrefix(opts.ViewOn, "system.buckets.") {
		return errors.New("unsupported timeseries: " + db + "." + viewName)
	}

	err := m.Database(db).CreateView(ctx,
		viewName,
		opts.ViewOn,
		opts.Pipeline,
		options.CreateView().SetCollation(opts.Collation))
	return errors.Wrap(err, "create view")
}

func (c *Catalog) DropCollection(ctx context.Context, m *mongo.Client, db DBName, coll CollName) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := m.Database(db).Collection(coll).Drop(ctx)
	if err != nil {
		return errors.Wrap(err, "drop collection")
	}

	delete(c.cat[db], coll)
	return nil
}

func (c *Catalog) DropDatabase(ctx context.Context, m *mongo.Client, db DBName) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := m.Database(db).Drop(ctx)
	if err != nil {
		return err //nolint:wrapcheck
	}

	delete(c.cat, db)
	return nil
}

func (c *Catalog) CreateIndexes(
	ctx context.Context,
	m *mongo.Client,
	db DBName,
	coll CollName,
	indexes []*IndexSpecification,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var prepareUnique []*IndexSpecification

	models := make([]mongo.IndexModel, len(indexes))
	for i, index := range indexes {
		var expireAfter *int32
		if index.ExpireAfterSeconds != nil {
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

		if index.PrepareUnique != nil && *index.PrepareUnique {
			prepareUnique = append(prepareUnique, index)
		}
	}

	_, err := m.Database(db).Collection(coll).Indexes().CreateMany(ctx, models)
	if err != nil {
		return errors.Wrap(err, "create indexes")
	}

	colls := c.cat[db]
	if colls == nil {
		colls = make(map[CollName]map[IndexName]*IndexSpecification)
		c.cat[db] = colls
	}

	collIndexes := colls[coll]
	if collIndexes == nil {
		collIndexes = make(map[IndexName]*IndexSpecification)
		colls[coll] = collIndexes
	}

	for _, index := range indexes {
		collIndexes[index.Name] = index
	}

	for _, index := range prepareUnique {
		mod := modifyIndexOption{
			Name:          index.Name,
			PrepareUnique: index.PrepareUnique,
		}
		err = c.modifyIndexImpl(ctx, m, db, coll, &mod)
		if err != nil {
			return errors.Wrap(err, db+"."+coll+": "+index.Name)
		}
	}

	return nil
}

func (c *Catalog) ModifyCappedCollection(
	ctx context.Context,
	m *mongo.Client,
	db DBName,
	coll CollName,
	size *int64,
	max *int64,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	cmd := bson.D{{"collMod", coll}}
	if size != nil {
		cmd = append(cmd, bson.E{"cappedSize", size})
	}
	if max != nil {
		cmd = append(cmd, bson.E{"cappedMax", max})
	}

	err := m.Database(db).RunCommand(ctx, cmd).Err()
	return err //nolint:wrapcheck
}

func (c *Catalog) ModifyView(
	ctx context.Context,
	m *mongo.Client,
	db DBName,
	view CollName,
	viewOn string,
	pipeline any,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	cmd := bson.D{{"collMod", view}, {"viewOn", viewOn}, {"pipeline", pipeline}}
	err := m.Database(db).RunCommand(ctx, cmd).Err()
	return err //nolint:wrapcheck
}

func (c *Catalog) ModifyIndex(
	ctx context.Context,
	m *mongo.Client,
	db DBName,
	coll CollName,
	modOpts *modifyIndexOption,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.modifyIndexImpl(ctx, m, db, coll, modOpts)
}

func (c *Catalog) modifyIndexImpl(
	ctx context.Context,
	m *mongo.Client,
	db DBName,
	coll CollName,
	modOpts *modifyIndexOption,
) error {
	mods := bson.D{{"name", modOpts.Name}}
	if modOpts.PrepareUnique != nil {
		mods = append(mods, primitive.E{"prepareUnique", modOpts.PrepareUnique})
	}
	if modOpts.Unique != nil {
		mods = append(mods, primitive.E{"unique", modOpts.Unique})
	}
	if modOpts.Hidden != nil {
		mods = append(mods, primitive.E{"hidden", modOpts.Hidden})
	}

	if len(mods) != 1 {
		cmd := bson.D{{"collMod", coll}, {"index", mods}}
		err := m.Database(db).RunCommand(ctx, cmd).Err()
		if err != nil {
			return errors.Wrap(err, "modify index: "+modOpts.Name)
		}
	}

	index := c.cat[db][coll][modOpts.Name]
	if modOpts.PrepareUnique != nil {
		index.PrepareUnique = modOpts.PrepareUnique
	}
	if modOpts.Unique != nil {
		index.PrepareUnique = nil
		index.Unique = modOpts.Unique
	}
	if modOpts.Hidden != nil {
		index.Hidden = modOpts.Hidden
	}
	if modOpts.ExpireAfterSeconds != nil {
		index.ExpireAfterSeconds = modOpts.ExpireAfterSeconds
	}
	c.cat[db][coll][modOpts.Name] = index

	return nil
}

func (c *Catalog) DropIndex(ctx context.Context, m *mongo.Client, db DBName, coll CollName, name IndexName) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := m.Database(db).Collection(coll).Indexes().DropOne(ctx, name)
	if err != nil {
		if !isIndexNotFound(err) {
			if c.cat[db] != nil {
				delete(c.cat[db][coll], name)
			}
		}

		return err //nolint:wrapcheck
	}

	if c.cat[db] != nil {
		delete(c.cat[db][coll], name)
	}

	return nil
}

func (c *Catalog) FinalizeIndexes(ctx context.Context, m *mongo.Client) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for db, colls := range c.cat {
		for coll, indexes := range colls {
			for _, index := range indexes {
				if index.ExpireAfterSeconds == nil {
					continue
				}
				if index.isClustered() {
					continue // clustered index with ttl is not supported
				}

				res := m.Database(db).RunCommand(ctx, bson.D{
					{"collMod", coll},
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

func isIndexNotFound(err error) bool {
	for ; err != nil; err = errors.Unwrap(err) {
		le, ok := err.(mongo.CommandError) //nolint:errorlint
		if ok && le.Name == "IndexNotFound" {
			return true
		}
	}
	return false
}
