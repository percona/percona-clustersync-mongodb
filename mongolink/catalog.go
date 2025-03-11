package mongolink

import (
	"context"
	"math"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"golang.org/x/sync/errgroup"

	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
	"github.com/percona-lab/percona-mongolink/topo"
)

// IDIndex is the name of the default index.
const IDIndex IndexName = "_id_"

type (
	NSName    = string // namespace name
	DBName    = string // database name
	CollName  = string // collection name
	IndexName = string // index name
)

const (
	// SystemPrefix is the prefix for system collections.
	SystemPrefix = "system."
	// TimeseriesPrefix is the prefix for timeseries buckets.
	TimeseriesPrefix = "system.buckets."
)

// CreateCollectionOptions represents the options that can be used to create a collection.
type CreateCollectionOptions struct {
	// ClusteredIndex is the clustered index for the collection.
	ClusteredIndex bson.D `bson:"clusteredIndex,omitempty"`

	// Capped  is if the collection is capped.
	Capped *bool `bson:"capped,omitempty"`
	// Size is the maximum size, in bytes, for a capped collection.
	Size *int32 `bson:"size,omitempty"`
	// int32 is the maximum number of documents allowed in a capped collection.
	Max *int32 `bson:"max,omitempty"`

	// ViewOn is the source collection or view for a view.
	ViewOn string `bson:"viewOn,omitempty"`
	// Pipeline is the aggregation pipeline for a view.
	Pipeline any `bson:"pipeline,omitempty"`

	// Collation is the collation options for the collection.
	Collation bson.Raw `bson:"collation,omitempty"`

	// StorageEngine is the storage engine options for the collection.
	StorageEngine bson.Raw `bson:"storageEngine,omitempty"`
	// IndexOptionDefaults is the default options for indexes on the collection.
	IndexOptionDefaults bson.Raw `bson:"indexOptionDefaults,omitempty"`
}

// ModifyIndexOption represents options for modifying an index in MongoDB.
type ModifyIndexOption struct {
	// Name is the name of the index.
	Name string `bson:"name"`
	// Hidden indicates whether the index is hidden.
	Hidden *bool `bson:"hidden,omitempty"`
	// Unique indicates whether the index enforces a unique constraint.
	Unique *bool `bson:"unique,omitempty"`
	// PrepareUnique indicates whether the index is being prepared to enforce a unique constraint.
	PrepareUnique *bool `bson:"prepareUnique,omitempty"`
	// ExpireAfterSeconds specifies the time to live for documents in the collection.
	ExpireAfterSeconds *int64 `bson:"expireAfterSeconds,omitempty"`
}

// Catalog manages the MongoDB catalog.
type Catalog struct {
	lock   sync.RWMutex
	target *mongo.Client
	cat    map[DBName]map[CollName]map[string]*topo.IndexSpecification
}

// NewCatalog creates a new Catalog.
func NewCatalog(target *mongo.Client) *Catalog {
	return &Catalog{
		target: target,
		cat:    make(map[DBName]map[CollName]map[string]*topo.IndexSpecification),
	}
}

type catalogCheckpoint struct {
	Catalog map[DBName]map[CollName]map[string]*topo.IndexSpecification `bson:"catalog"`
}

func (c *Catalog) LockWrite() {
	c.lock.RLock()
}

func (c *Catalog) UnlockWrite() {
	c.lock.RUnlock()
}

func (c *Catalog) Checkpoint() *catalogCheckpoint { //nolint:revive
	c.lock.RLock()
	defer c.lock.RUnlock()

	if len(c.cat) == 0 {
		return nil
	}

	return &catalogCheckpoint{Catalog: c.cat}
}

func (c *Catalog) Recover(cp *catalogCheckpoint) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if len(c.cat) != 0 {
		return errors.New("cannot restore")
	}

	c.cat = cp.Catalog

	return nil
}

func (c *Catalog) CreateCollection(
	ctx context.Context,
	db DBName,
	coll CollName,
	opts *CreateCollectionOptions,
) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if opts.ViewOn != "" {
		if strings.HasPrefix(opts.ViewOn, TimeseriesPrefix) {
			return errors.New("timeseries is not supported: " + db + "." + coll)
		}

		return c.doCreateView(ctx, db, coll, opts)
	}

	return c.doCreateCollection(ctx, db, coll, opts)
}

// doCreateCollection creates a new collection in the target MongoDB.
func (c *Catalog) doCreateCollection(
	ctx context.Context,
	db DBName,
	coll CollName,
	opts *CreateCollectionOptions,
) error {
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

	err := c.target.Database(db).RunCommand(ctx, cmd).Err()
	if err != nil {
		return errors.Wrap(err, "create collection")
	}

	return nil
}

// doCreateView creates a new view in the target MongoDB.
func (c *Catalog) doCreateView(
	ctx context.Context,
	db DBName,
	view CollName,
	opts *CreateCollectionOptions,
) error {
	cmd := bson.D{
		{"create", view},
		{"viewOn", opts.ViewOn},
		{"pipeline", opts.Pipeline},
	}

	if opts.Collation != nil {
		cmd = append(cmd, bson.E{"collation", opts.Collation})
	}

	err := c.target.Database(db).RunCommand(ctx, cmd).Err()
	if err != nil {
		return errors.Wrap(err, "create view")
	}

	return nil
}

// DropCollection drops a collection in the target MongoDB.
func (c *Catalog) DropCollection(ctx context.Context, db DBName, coll CollName) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	err := c.target.Database(db).Collection(coll).Drop(ctx)
	if err != nil {
		return err //nolint:wrapcheck
	}

	c.deleteCollectionEntry(db, coll)

	return nil
}

// DropDatabase drops a database in the target MongoDB.
func (c *Catalog) DropDatabase(ctx context.Context, db DBName) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	lg := log.Ctx(ctx)

	collNames, err := topo.ListCollectionNames(ctx, c.target, db)
	if err != nil {
		return errors.Wrap(err, "list collection names")
	}

	eg, grpCtx := errgroup.WithContext(ctx)

	for _, coll := range collNames {
		eg.Go(func() error {
			err := c.target.Database(db).Collection(coll).Drop(grpCtx)
			if err != nil {
				return errors.Wrapf(err, "drop namespace %s.%s", db, coll)
			}

			lg.Debugf("Dropped %s.%s", db, coll)

			return nil
		})
	}

	c.deleteDatabaseEntry(db)

	return eg.Wait() //nolint:wrapcheck
}

// CreateIndexes creates indexes in the target MongoDB.
func (c *Catalog) CreateIndexes(
	ctx context.Context,
	db DBName,
	coll CollName,
	indexes []*topo.IndexSpecification,
) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	lg := log.Ctx(ctx)

	if len(indexes) == 0 {
		lg.Error(nil, "No indexes to create")

		return nil
	}

	idxs := make([]*topo.IndexSpecification, 0, len(indexes)-1) // -1 for ID index

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

	// NOTE: [mongo.IndexView.CreateMany] uses [mongo.IndexModel]
	// which does not support `prepareUnique`.
	res := c.target.Database(db).RunCommand(ctx, bson.D{
		{"createIndexes", coll},
		{"indexes", idxs},
	})
	if err := res.Err(); err != nil {
		if topo.IsIndexOptionsConflict(err) {
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
	db DBName,
	coll CollName,
	sizeBytes *int64,
	maxDocs *int64,
) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	cmd := bson.D{{"collMod", coll}}
	if sizeBytes != nil {
		cmd = append(cmd, bson.E{"cappedSize", sizeBytes})
	}

	if maxDocs != nil {
		cmd = append(cmd, bson.E{"cappedMax", maxDocs})
	}

	err := c.target.Database(db).RunCommand(ctx, cmd).Err()

	return err //nolint:wrapcheck
}

// ModifyView modifies a view in the target MongoDB.
func (c *Catalog) ModifyView(
	ctx context.Context,
	db DBName,
	view CollName,
	viewOn CollName,
	pipeline any,
) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	cmd := bson.D{
		{"collMod", view},
		{"viewOn", viewOn},
		{"pipeline", pipeline},
	}
	err := c.target.Database(db).RunCommand(ctx, cmd).Err()

	return err //nolint:wrapcheck
}

// ModifyIndex modifies an index in the target MongoDB.
func (c *Catalog) ModifyIndex(
	ctx context.Context,
	db DBName,
	coll CollName,
	mods *ModifyIndexOption,
) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if mods.ExpireAfterSeconds != nil {
		cmd := bson.D{
			{"collMod", coll},
			{"index", bson.D{
				{"name", mods.Name},
				{"expireAfterSeconds", math.MaxInt32},
			}},
		}

		err := c.target.Database(db).RunCommand(ctx, cmd).Err()
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

func (c *Catalog) Rename(
	ctx context.Context,
	db DBName,
	coll CollName,
	targetDB DBName,
	targetColl CollName,
) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	opts := bson.D{
		{"renameCollection", db + "." + coll},
		{"to", targetDB + "." + targetColl},
		{"dropTarget", true},
	}

	err := c.target.Database("admin").RunCommand(ctx, opts).Err()
	if err != nil {
		return errors.Wrap(err, "rename collection")
	}

	c.renameCollectionEntry(db, coll, targetColl)

	return nil
}

// DropIndex drops an index in the target MongoDB.
func (c *Catalog) DropIndex(ctx context.Context, db DBName, coll CollName, name IndexName) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	err := c.target.Database(db).Collection(coll).Indexes().DropOne(ctx, name)
	if err != nil {
		if topo.IsIndexNotFound(err) {
			log.Ctx(ctx).Debug(err.Error())

			c.deleteIndexEntry(db, coll, name) // make sure no index stored

			return nil
		}

		return err //nolint:wrapcheck
	}

	c.deleteIndexEntry(db, coll, name)

	return nil
}

// Finalize finalizes the indexes in the target MongoDB.
func (c *Catalog) Finalize(ctx context.Context) error {
	c.lock.RLock()
	defer c.lock.RUnlock()

	lg := log.Ctx(ctx)

	for db, colls := range c.cat {
		for coll, indexes := range colls {
			for _, index := range indexes {
				if index.IsClustered() {
					lg.Warn("Clustered index with TTL is not supported")

					continue
				}

				// restore properties
				switch { // unique and prepareUnique are mutually exclusive.
				case index.Unique != nil && *index.Unique:
					lg.Info("Convert index to prepareUnique: " + index.Name)

					err := c.modifyIndexOption(ctx, db, coll, index.Name, "prepareUnique", true)
					if err != nil {
						return errors.Wrap(err, "convert to unique: prepareUnique: "+index.Name)
					}

					lg.Info("Convert prepareUnique index to unique: " + index.Name)

					err = c.modifyIndexOption(ctx, db, coll, index.Name, "unique", true)
					if err != nil {
						return errors.Wrap(err, "convert to unique: "+index.Name)
					}

				case index.PrepareUnique != nil && *index.PrepareUnique:
					lg.Info("Convert prepareUnique index to unique: " + index.Name)

					err := c.modifyIndexOption(ctx, db, coll, index.Name, "prepareUnique", true)
					if err != nil {
						return errors.Wrap(err, "convert to prepareUnique: "+index.Name)
					}
				}

				if index.ExpireAfterSeconds != nil {
					lg.Info("Modify index expireAfterSeconds: " + index.Name)

					err := c.modifyIndexOption(ctx,
						db, coll, index.Name, "expireAfterSeconds", *index.ExpireAfterSeconds)
					if err != nil {
						return errors.Wrap(err, "modify expireAfterSeconds: "+index.Name)
					}
				}

				if index.Hidden != nil {
					lg.Info("Modify index hidden: " + index.Name)

					err := c.modifyIndexOption(ctx, db, coll, index.Name, "hidden", index.Hidden)
					if err != nil {
						return errors.Wrap(err, "modify hidden: "+index.Name)
					}
				}
			}
		}
	}

	return nil
}

// modifyIndexOption modifies an index property in the target MongoDB.
func (c *Catalog) modifyIndexOption(
	ctx context.Context,
	db DBName,
	coll CollName,
	index IndexName,
	propName string,
	value any,
) error {
	res := c.target.Database(db).RunCommand(ctx, bson.D{
		{"collMod", coll},
		{"index", bson.D{
			{"name", index},
			{propName, value},
		}},
	})

	return res.Err() //nolint:wrapcheck
}

// getIndexEntry gets an index entry from the catalog.
func (c *Catalog) getIndexEntry(db DBName, coll CollName, name IndexName) *topo.IndexSpecification {
	databaseEntry := c.cat[db]
	if len(databaseEntry) == 0 {
		return nil
	}

	collectionEntry := databaseEntry[coll]
	if len(collectionEntry) == 0 {
		return nil
	}

	return collectionEntry[name]
}

// addIndexEntries adds index entries to the catalog.
func (c *Catalog) addIndexEntries(db DBName, coll CollName, indexes []*topo.IndexSpecification) {
	databaseEntry := c.cat[db]
	if databaseEntry == nil {
		databaseEntry = make(map[CollName]map[string]*topo.IndexSpecification)
		c.cat[db] = databaseEntry
	}

	collectionEntry := databaseEntry[coll]
	if collectionEntry == nil {
		collectionEntry = make(map[string]*topo.IndexSpecification)
		databaseEntry[coll] = collectionEntry
	}

	for _, index := range indexes {
		collectionEntry[index.Name] = index
	}

	databaseEntry[coll] = collectionEntry
	c.cat[db] = databaseEntry
}

// deleteIndexEntry deletes an index entry from the catalog.
func (c *Catalog) deleteIndexEntry(db DBName, coll CollName, name IndexName) {
	databaseEntry := c.cat[db]
	if databaseEntry == nil {
		return
	}

	collectionEntry := databaseEntry[coll]
	if collectionEntry == nil {
		return
	}

	delete(collectionEntry, name)
}

// deleteCollectionEntry deletes a collection entry from the catalog.
func (c *Catalog) deleteCollectionEntry(db DBName, coll CollName) {
	databaseEntry := c.cat[db]

	if len(databaseEntry) == 0 {
		return
	}

	delete(databaseEntry, coll)
}

func (c *Catalog) deleteDatabaseEntry(dbName string) {
	delete(c.cat, dbName)
}

func (c *Catalog) renameCollectionEntry(db DBName, coll, targetColl CollName) {
	databaseEntry := c.cat[db]
	if len(databaseEntry) == 0 {
		log.New("catalog:rename").Errorf(nil, "database %q is empty", db)

		databaseEntry = make(map[CollName]map[string]*topo.IndexSpecification)
		c.cat[db] = databaseEntry
	}

	collectionEntry := databaseEntry[coll]
	if len(collectionEntry) == 0 {
		log.New("catalog:rename").Errorf(nil, `collection "%s.%s" is empty`, db, coll)
	}

	databaseEntry[targetColl] = databaseEntry[coll]
	delete(databaseEntry, coll)
}
