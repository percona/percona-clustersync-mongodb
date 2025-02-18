package repl

import (
	"context"
	"math"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
)

const IDIndex IndexName = "_id_"

type (
	DBName    = string
	CollName  = string
	IndexName = string
)

// IndexSpecification contains all index options.
//
// NOTE: Indexes().Create*() uses [mongo.IndexModel] which does not support `prepareUnique`.
// GeoHaystack indexes cannot be created in version 5.0 and above (`bucketSize` field)
type IndexSpecification struct {
	Name               IndexName `bson:"name"`
	Namespace          string    `bson:"ns"`
	KeysDocument       bson.Raw  `bson:"key"`
	Version            int32     `bson:"v"`
	Sparse             *bool     `bson:"sparse,omitempty"`
	Hidden             *bool     `bson:"hidden,omitempty"`
	Unique             *bool     `bson:"unique,omitempty"`
	PrepareUnique      *bool     `bson:"prepareUnique,omitempty"`
	Clustered          *bool     `bson:"clustered,omitempty"`
	ExpireAfterSeconds *int64    `bson:"expireAfterSeconds,omitempty"`

	Weights          any      `bson:"weights,omitempty"`
	DefaultLanguage  *string  `bson:"default_language,omitempty"`
	LanguageOverride *string  `bson:"language_override,omitempty"`
	TextVersion      *int32   `bson:"textIndexVersion,omitempty"`
	Collation        bson.Raw `bson:"collation,omitempty"`

	WildcardProjection      any `bson:"wildcardProjection,omitempty"`
	PartialFilterExpression any `bson:"partialFilterExpression,omitempty"`

	Bits      *int32   `bson:"bits,omitempty"`
	Min       *float64 `bson:"min,omitempty"`
	Max       *float64 `bson:"max,omitempty"`
	GeoIdxVer *int32   `bson:"2dsphereIndexVersion,omitempty"`
}

func (s *IndexSpecification) isClustered() bool {
	return s.Clustered != nil && *s.Clustered
}

type Catalog struct {
	mu sync.Mutex

	databases map[DBName]map[CollName]map[string]*IndexSpecification
}

func NewCatalog() *Catalog {
	return &Catalog{databases: make(map[DBName]map[CollName]map[string]*IndexSpecification)}
}

func (c *Catalog) CreateCollection(
	ctx context.Context,
	m *mongo.Client,
	db DBName,
	coll CollName,
	opts *createCollectionOptions,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	cmd := bson.D{{"create", coll}}
	if opts.ClusteredIndex != nil {
		cmd = append(cmd, bson.E{"clusteredIndex", opts.ClusteredIndex})
	}

	if opts.Capped != nil {
		cmd = append(cmd, bson.E{"capped", opts.Capped})
		if opts.Size != nil {
			cmd = append(cmd, bson.E{"size", opts.Size})
		}
		if opts.Max != nil {
			cmd = append(cmd, bson.E{"max", opts.Max})
		}
	}

	if opts.Collation != nil {
		cmd = append(cmd, bson.E{"collation", opts.Collation})
	}

	if opts.StorageEngine != nil {
		cmd = append(cmd, bson.E{"storageEngine", opts.StorageEngine})
	}
	if opts.IndexOptionDefaults != nil {
		cmd = append(cmd, bson.E{"indexOptionDefaults", opts.IndexOptionDefaults})
	}

	err := m.Database(db).RunCommand(ctx, cmd).Err()
	if err != nil {
		return errors.Wrap(err, "create collection")
	}

	c.ensureCollectionEntry(db, coll)
	return nil
}

func (c *Catalog) CreateView(
	ctx context.Context,
	m *mongo.Client,
	db DBName,
	view CollName,
	opts *createCollectionOptions,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if strings.HasPrefix(opts.ViewOn, "system.buckets.") {
		return errors.New("unsupported timeseries: " + db + "." + view)
	}

	cmd := bson.D{
		{"create", view},
		{"viewOn", opts.ViewOn},
		{"pipeline", opts.Pipeline},
	}

	if opts.Collation != nil {
		cmd = append(cmd, bson.E{"collation", opts.Collation})
	}

	err := m.Database(db).RunCommand(ctx, cmd).Err()
	if err != nil {
		return errors.Wrap(err, "create view")
	}

	c.ensureCollectionEntry(db, view)
	return nil
}

func (c *Catalog) DropCollection(
	ctx context.Context,
	m *mongo.Client,
	db DBName,
	coll CollName,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := m.Database(db).Collection(coll).Drop(ctx)
	if err != nil {
		return err //nolint:wrapcheck
	}

	c.deleteCollectionEntry(db, coll)
	return nil
}

func (c *Catalog) DropDatabase(ctx context.Context, m *mongo.Client, db DBName) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for coll := range c.databases[db] {
		err := m.Database(db).Collection(coll).Drop(ctx)
		if err != nil {
			return errors.Wrapf(err, "drop namespace %s.%s", db, coll)
		}

		log.Infof(ctx, "dropped %s.%s", db, coll)
		c.deleteCollectionEntry(db, coll)
	}

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

	if len(indexes) == 0 {
		log.Warn(ctx, "createIndexes: no indexes")
		return nil
	}

	idxs := make([]*IndexSpecification, 0, len(indexes)-1) //-1 for ID index
	for _, index := range indexes {
		if index.Name == IDIndex {
			continue // already created
		}

		switch { // unique and prepareUnique are mutually exclusive.
		case index.Unique != nil && *index.Unique:
			idxCopy := *index
			idxCopy.Unique = nil
			index = &idxCopy
			log.Info(ctx, "create unique index as non-unique: "+index.Name)

		case index.PrepareUnique != nil && *index.PrepareUnique:
			idxCopy := *index
			idxCopy.PrepareUnique = nil
			index = &idxCopy
			log.Info(ctx, "create prepareUnique index as non-unique: "+index.Name)
		}

		if index.ExpireAfterSeconds != nil {
			maxDuration := int64(math.MaxInt32)
			idxCopy := *index
			idxCopy.ExpireAfterSeconds = &maxDuration
			index = &idxCopy
			log.Info(ctx, "create TTL index with modified expireAfterSeconds value: "+index.Name)
		}

		if index.Hidden != nil && *index.Hidden {
			idxCopy := *index
			idxCopy.Hidden = nil
			index = &idxCopy
			log.Info(ctx, "create hidden index as unhidden: "+index.Name)
		}

		idxs = append(idxs, index)
	}
	if len(idxs) == 0 {
		return nil
	}

	// NOTE: Indexes().CreateMany() uses [mongo.IndexModel] which does not support `prepareUnique`.
	res := m.Database(db).RunCommand(ctx, bson.D{{"createIndexes", coll}, {"indexes", idxs}})
	if err := res.Err(); err != nil {
		if isIndexOptionsConflict(err) {
			log.Error(ctx, err, "")
			return nil
		}
		return err //nolint:wrapcheck
	}

	c.addIndexEntries(db, coll, indexes)
	return nil
}

func (c *Catalog) ModifyCappedCollection(
	ctx context.Context,
	m *mongo.Client,
	db DBName,
	coll CollName,
	sizeBytes *int64,
	maxDocs *int64,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	cmd := bson.D{{"collMod", coll}}
	if sizeBytes != nil {
		cmd = append(cmd, bson.E{"cappedSize", sizeBytes})
	}
	if maxDocs != nil {
		cmd = append(cmd, bson.E{"cappedMax", maxDocs})
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

	cmd := bson.D{
		{"collMod", view},
		{"viewOn", viewOn},
		{"pipeline", pipeline},
	}
	err := m.Database(db).RunCommand(ctx, cmd).Err()
	return err //nolint:wrapcheck
}

func (c *Catalog) ModifyIndex(
	ctx context.Context,
	m *mongo.Client,
	db DBName,
	coll CollName,
	mods *modifyIndexOption,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if mods.ExpireAfterSeconds != nil {
		cmd := bson.D{
			{"collMod", coll},
			{"index", bson.D{
				{"name", mods.Name},
				{"expireAfterSeconds", math.MaxInt32},
			}},
		}
		err := m.Database(db).RunCommand(ctx, cmd).Err()
		if err != nil {
			return errors.Wrap(err, "modify index: "+mods.Name)
		}
	}

	index := c.getIndexEntry(db, coll, mods.Name)
	if index == nil {
		return nil
	}

	// update in-place by ptr
	if mods.PrepareUnique != nil {
		index.PrepareUnique = mods.PrepareUnique
	}
	if mods.Unique != nil {
		index.PrepareUnique = nil
		index.Unique = mods.Unique
	}
	if mods.Hidden != nil {
		index.Hidden = mods.Hidden
	}
	if mods.ExpireAfterSeconds != nil {
		index.ExpireAfterSeconds = mods.ExpireAfterSeconds
	}

	return nil
}

func (c *Catalog) DropIndex(
	ctx context.Context,
	m *mongo.Client,
	db DBName,
	coll CollName,
	name IndexName,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := m.Database(db).Collection(coll).Indexes().DropOne(ctx, name)
	if err != nil {
		if isIndexNotFound(err) {
			log.Warn(ctx, err.Error())
			c.deleteIndexEntry(db, coll, name) // make sure no index stored
			return nil
		}

		return err //nolint:wrapcheck
	}

	c.deleteIndexEntry(db, coll, name)
	return nil
}

func (c *Catalog) FinalizeIndexes(ctx context.Context, m *mongo.Client) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	ctx = log.WithAttrs(ctx, log.Scope("catalog:finalize"))

	for db, dbEntry := range c.databases {
		for coll, collEntry := range dbEntry {
			for _, index := range collEntry {
				if index.isClustered() {
					log.Warn(ctx, "clustered index with ttl is not supported")
					continue
				}

				// restore properties
				switch { // unique and prepareUnique are mutually exclusive.
				case index.Unique != nil && *index.Unique:
					log.Info(ctx, "convert index to prepareUnique: "+index.Name)
					err := modifyIndexProp(ctx, m, db, coll, index.Name, "prepareUnique", true)
					if err != nil {
						return errors.Wrap(err, "convert to unique: prepareUnique: "+index.Name)
					}

					log.Info(ctx, "convert prepareUnique index to unique: "+index.Name)
					err = modifyIndexProp(ctx, m, db, coll, index.Name, "unique", true)
					if err != nil {
						return errors.Wrap(err, "convert to unique: "+index.Name)
					}

				case index.PrepareUnique != nil && *index.PrepareUnique:
					log.Info(ctx, "convert prepareUnique index to unique: "+index.Name)
					err := modifyIndexProp(ctx, m, db, coll, index.Name, "prepareUnique", true)
					if err != nil {
						return errors.Wrap(err, "convert to prepareUnique: "+index.Name)
					}
				}

				if index.ExpireAfterSeconds != nil {
					log.Info(ctx, "modify index expireAfterSeconds: "+index.Name)
					err := modifyIndexProp(ctx,
						m, db, coll, index.Name, "expireAfterSeconds", *index.ExpireAfterSeconds)
					if err != nil {
						return errors.Wrap(err, "modify expireAfterSeconds: "+index.Name)
					}
				}

				if index.Hidden != nil {
					log.Info(ctx, "modify index hidden: "+index.Name)
					err := modifyIndexProp(ctx, m, db, coll, index.Name, "hidden", index.Hidden)
					if err != nil {
						return errors.Wrap(err, "modify hidden: "+index.Name)
					}
				}
			}
		}
	}

	return nil
}

func (c *Catalog) getIndexEntry(db DBName, coll CollName, name IndexName) *IndexSpecification {
	dbEntry := c.databases[db]
	if len(dbEntry) == 0 {
		return nil
	}

	collEntry := dbEntry[coll]
	if len(collEntry) == 0 {
		return nil
	}

	for _, idx := range collEntry {
		if idx.Name == name {
			return idx
		}
	}

	return nil
}

func (c *Catalog) addIndexEntries(db DBName, coll CollName, indexes []*IndexSpecification) {
	collEntry := c.ensureCollectionEntry(db, coll)
	for _, index := range indexes {
		collEntry[index.Name] = index
	}
}

func (c *Catalog) deleteIndexEntry(db DBName, coll CollName, name IndexName) {
	dbEntry := c.databases[db]
	if len(dbEntry) == 0 {
		return
	}

	delete(dbEntry[coll], name)
}

func (c *Catalog) ensureCollectionEntry(db DBName, coll CollName) map[string]*IndexSpecification {
	dbEntry := c.databases[db]
	if dbEntry == nil {
		dbEntry = make(map[CollName]map[string]*IndexSpecification)
		c.databases[db] = dbEntry
	}

	collEntry := dbEntry[coll]
	if collEntry == nil {
		collEntry = make(map[string]*IndexSpecification)
		dbEntry[coll] = collEntry
	}

	return collEntry
}

func (c *Catalog) deleteCollectionEntry(db DBName, coll CollName) {
	delete(c.databases[db], coll)
}

func isIndexNotFound(err error) bool {
	return isMongoError(err, "IndexNotFound")
}

func isIndexOptionsConflict(err error) bool {
	return isMongoError(err, "IndexOptionsConflict")
}

func isMongoError(err error, name string) bool {
	if err == nil {
		return false
	}

	le, ok := err.(mongo.CommandError) //nolint:errorlint
	if ok && le.Name == name {
		return true
	}

	return isIndexNotFound(errors.Unwrap(err))
}

func modifyIndexProp(
	ctx context.Context,
	m *mongo.Client,
	db DBName,
	coll CollName,
	index IndexName,
	name string,
	value any,
) error {
	res := m.Database(db).RunCommand(ctx, bson.D{
		{"collMod", coll},
		{"index", bson.D{
			{"name", index},
			{name, value},
		}},
	})
	return res.Err() //nolint:wrapcheck
}
