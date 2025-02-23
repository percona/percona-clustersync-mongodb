package mongolink

import (
	"context"
	"math"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

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
// GeoHaystack indexes cannot be created in version 5.0 and above (`bucketSize` field).
type IndexSpecification struct {
	Name               IndexName `bson:"name"`                         // Index name
	Namespace          string    `bson:"ns"`                           // Namespace
	KeysDocument       bson.Raw  `bson:"key"`                          // Keys document
	Version            int32     `bson:"v"`                            // Version
	Sparse             *bool     `bson:"sparse,omitempty"`             // Sparse index
	Hidden             *bool     `bson:"hidden,omitempty"`             // Hidden index
	Unique             *bool     `bson:"unique,omitempty"`             // Unique index
	PrepareUnique      *bool     `bson:"prepareUnique,omitempty"`      // Prepare unique index
	Clustered          *bool     `bson:"clustered,omitempty"`          // Clustered index
	ExpireAfterSeconds *int64    `bson:"expireAfterSeconds,omitempty"` // Expire after seconds

	Weights          any      `bson:"weights,omitempty"`           // Weights
	DefaultLanguage  *string  `bson:"default_language,omitempty"`  // Default language
	LanguageOverride *string  `bson:"language_override,omitempty"` // Language override
	TextVersion      *int32   `bson:"textIndexVersion,omitempty"`  // Text index version
	Collation        bson.Raw `bson:"collation,omitempty"`         // Collation

	WildcardProjection      any `bson:"wildcardProjection,omitempty"`      // Wildcard projection
	PartialFilterExpression any `bson:"partialFilterExpression,omitempty"` // Partial filter expression

	Bits      *int32   `bson:"bits,omitempty"`                 // Bits
	Min       *float64 `bson:"min,omitempty"`                  // Min
	Max       *float64 `bson:"max,omitempty"`                  // Max
	GeoIdxVer *int32   `bson:"2dsphereIndexVersion,omitempty"` // Geo index version
}

// isClustered checks if the index is clustered.
func (s *IndexSpecification) isClustered() bool {
	return s.Clustered != nil && *s.Clustered
}

// Catalog manages the MongoDB catalog.
type Catalog struct {
	mu sync.Mutex

	databases map[DBName]map[CollName]map[string]*IndexSpecification // Databases map
}

// NewCatalog creates a new Catalog.
func NewCatalog() *Catalog {
	return &Catalog{databases: make(map[DBName]map[CollName]map[string]*IndexSpecification)}
}

// CreateCollection creates a new collection in the target MongoDB.
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

// CreateView creates a new view in the target MongoDB.
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

// DropCollection drops a collection in the target MongoDB.
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

// DropDatabase drops a database in the target MongoDB.
func (c *Catalog) DropDatabase(ctx context.Context, m *mongo.Client, db DBName) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	lg := log.Ctx(ctx)

	for coll := range c.databases[db] {
		err := m.Database(db).Collection(coll).Drop(ctx)
		if err != nil {
			return errors.Wrapf(err, "drop namespace %s.%s", db, coll)
		}

		lg.Infof("Dropped %s.%s", db, coll)
		c.deleteCollectionEntry(db, coll)
	}

	return nil
}

// CreateIndexes creates indexes in the target MongoDB.
func (c *Catalog) CreateIndexes(
	ctx context.Context,
	m *mongo.Client,
	db DBName,
	coll CollName,
	indexes []*IndexSpecification,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	lg := log.Ctx(ctx)

	if len(indexes) == 0 {
		lg.Error(nil, "No indexes to create")

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
			lg.Info("Create unique index as non-unique: " + index.Name)

		case index.PrepareUnique != nil && *index.PrepareUnique:
			idxCopy := *index
			idxCopy.PrepareUnique = nil
			index = &idxCopy
			lg.Info("Create prepareUnique index as non-unique: " + index.Name)
		}

		if index.ExpireAfterSeconds != nil {
			maxDuration := int64(math.MaxInt32)
			idxCopy := *index
			idxCopy.ExpireAfterSeconds = &maxDuration
			index = &idxCopy
			lg.Info("Create TTL index with modified expireAfterSeconds value: " + index.Name)
		}

		if index.Hidden != nil && *index.Hidden {
			idxCopy := *index
			idxCopy.Hidden = nil
			index = &idxCopy
			lg.Info("Create hidden index as unhidden: " + index.Name)
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
			lg.Error(err, "")

			return nil
		}

		return err //nolint:wrapcheck
	}

	c.addIndexEntries(db, coll, indexes)

	return nil
}

// ModifyCappedCollection modifies a capped collection in the target MongoDB.
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

// ModifyView modifies a view in the target MongoDB.
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

// ModifyIndex modifies an index in the target MongoDB.
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

// DropIndex drops an index in the target MongoDB.
func (c *Catalog) DropIndex(
	ctx context.Context,
	m *mongo.Client,
	db DBName,
	coll CollName,
	name IndexName,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := m.Database(db).Collection(coll).Indexes().DropOne(ctx, name)
	if err != nil {
		if isIndexNotFound(err) {
			log.Ctx(ctx).Debug(err.Error())

			c.deleteIndexEntry(db, coll, name) // make sure no index stored

			return nil
		}

		return err //nolint:wrapcheck
	}

	c.deleteIndexEntry(db, coll, name)

	return nil
}

// FinalizeIndexes finalizes the indexes in the target MongoDB.
func (c *Catalog) FinalizeIndexes(ctx context.Context, m *mongo.Client) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	lg := log.Ctx(ctx)

	for db, dbEntry := range c.databases {
		for coll, collEntry := range dbEntry {
			for _, index := range collEntry {
				if index.isClustered() {
					lg.Warn("Clustered index with TTL is not supported")

					continue
				}

				// restore properties
				switch { // unique and prepareUnique are mutually exclusive.
				case index.Unique != nil && *index.Unique:
					lg.Info("Convert index to prepareUnique: " + index.Name)

					err := modifyIndexProp(ctx, m, db, coll, index.Name, "prepareUnique", true)
					if err != nil {
						return errors.Wrap(err, "convert to unique: prepareUnique: "+index.Name)
					}

					lg.Info("Convert prepareUnique index to unique: " + index.Name)

					err = modifyIndexProp(ctx, m, db, coll, index.Name, "unique", true)
					if err != nil {
						return errors.Wrap(err, "convert to unique: "+index.Name)
					}

				case index.PrepareUnique != nil && *index.PrepareUnique:
					lg.Info("Convert prepareUnique index to unique: " + index.Name)

					err := modifyIndexProp(ctx, m, db, coll, index.Name, "prepareUnique", true)
					if err != nil {
						return errors.Wrap(err, "convert to prepareUnique: "+index.Name)
					}
				}

				if index.ExpireAfterSeconds != nil {
					lg.Info("Modify index expireAfterSeconds: " + index.Name)

					err := modifyIndexProp(ctx,
						m, db, coll, index.Name, "expireAfterSeconds", *index.ExpireAfterSeconds)
					if err != nil {
						return errors.Wrap(err, "modify expireAfterSeconds: "+index.Name)
					}
				}

				if index.Hidden != nil {
					lg.Info("Modify index hidden: " + index.Name)

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

// getIndexEntry gets an index entry from the catalog.
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

// addIndexEntries adds index entries to the catalog.
func (c *Catalog) addIndexEntries(db DBName, coll CollName, indexes []*IndexSpecification) {
	collEntry := c.ensureCollectionEntry(db, coll)
	for _, index := range indexes {
		collEntry[index.Name] = index
	}
}

// deleteIndexEntry deletes an index entry from the catalog.
func (c *Catalog) deleteIndexEntry(db DBName, coll CollName, name IndexName) {
	dbEntry := c.databases[db]
	if len(dbEntry) == 0 {
		return
	}

	delete(dbEntry[coll], name)
}

// ensureCollectionEntry ensures a collection entry exists in the catalog.
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

// deleteCollectionEntry deletes a collection entry from the catalog.
func (c *Catalog) deleteCollectionEntry(db DBName, coll CollName) {
	delete(c.databases[db], coll)
}

// isIndexNotFound checks if an error is an index not found error.
func isIndexNotFound(err error) bool {
	return isMongoError(err, "IndexNotFound")
}

// isIndexOptionsConflict checks if an error is an index options conflict error.
func isIndexOptionsConflict(err error) bool {
	return isMongoError(err, "IndexOptionsConflict")
}

// isMongoError checks if an error is a MongoDB error with the specified name.
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

// modifyIndexProp modifies an index property in the target MongoDB.
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
