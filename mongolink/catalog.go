package mongolink

import (
	"context"
	"math"
	"slices"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"golang.org/x/sync/errgroup"

	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
	"github.com/percona-lab/percona-mongolink/topo"
)

// IDIndex is the name of the "_id" index.
const IDIndex = "_id_"

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
	Size *int64 `bson:"size,omitempty"`
	// Max is the maximum number of documents allowed in a capped collection.
	Max *int32 `bson:"max,omitempty"`

	// ViewOn is the source collection or view for a view.
	ViewOn string `bson:"viewOn,omitempty"`
	// Pipeline is the aggregation pipeline for a view.
	Pipeline bson.A `bson:"pipeline,omitempty"`

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
	lock      sync.RWMutex
	target    *mongo.Client
	Databases map[string]databaseCatalog
}

type databaseCatalog struct {
	Collections map[string]collectionCatalog
}

type collectionCatalog struct {
	AddedAt bson.Timestamp
	Indexes []*topo.IndexSpecification
}

// NewCatalog creates a new Catalog.
func NewCatalog(target *mongo.Client) *Catalog {
	return &Catalog{
		target:    target,
		Databases: make(map[string]databaseCatalog),
	}
}

type catalogCheckpoint struct {
	Catalog map[string]databaseCatalog `bson:"catalog"`
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

	if len(c.Databases) == 0 {
		return nil
	}

	return &catalogCheckpoint{Catalog: c.Databases}
}

func (c *Catalog) Recover(cp *catalogCheckpoint) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if len(c.Databases) != 0 {
		return errors.New("cannot restore")
	}

	c.Databases = cp.Catalog

	return nil
}

func (c *Catalog) CreateCollection(
	ctx context.Context,
	db string,
	coll string,
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
	db string,
	coll string,
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

	c.addCollectionToCatalog(ctx, db, coll)

	return nil
}

// doCreateView creates a new view in the target MongoDB.
func (c *Catalog) doCreateView(
	ctx context.Context,
	db string,
	view string,
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
func (c *Catalog) DropCollection(ctx context.Context, db, coll string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	err := c.target.Database(db).Collection(coll).Drop(ctx)
	if err != nil {
		return err //nolint:wrapcheck
	}

	c.deleteCollectionFromCatalog(db, coll)

	return nil
}

// DropDatabase drops a database in the target MongoDB.
func (c *Catalog) DropDatabase(ctx context.Context, db string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	lg := log.Ctx(ctx)

	colls, err := topo.ListCollectionNames(ctx, c.target, db)
	if err != nil {
		return errors.Wrap(err, "list collection names")
	}

	eg, grpCtx := errgroup.WithContext(ctx)

	for _, coll := range colls {
		eg.Go(func() error {
			err := c.target.Database(db).Collection(coll).Drop(grpCtx)
			if err != nil {
				return errors.Wrapf(err, "drop namespace %s.%s", db, coll)
			}

			lg.Debugf("Dropped %s.%s", db, coll)

			return nil
		})
	}

	c.deleteDatabaseFromCatalog(db)

	return eg.Wait() //nolint:wrapcheck
}

// CreateIndexes creates indexes in the target MongoDB.
func (c *Catalog) CreateIndexes(
	ctx context.Context,
	db string,
	coll string,
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

	c.addIndexesToCatalog(ctx, db, coll, indexes)

	return nil
}

// ModifyCappedCollection modifies a capped collection in the target MongoDB.
func (c *Catalog) ModifyCappedCollection(
	ctx context.Context,
	db string,
	coll string,
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
func (c *Catalog) ModifyView(ctx context.Context, db, view, viewOn string, pipeline any) error {
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
func (c *Catalog) ModifyIndex(ctx context.Context, db, coll string, mods *ModifyIndexOption) error {
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

	index := c.getIndexFromCatalog(db, coll, mods.Name)
	if index == nil {
		log.Ctx(ctx).Errorf(nil, "index %q not found", mods.Name)

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

func (c *Catalog) Rename(ctx context.Context, db, coll, targetDB, targetColl string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	opts := bson.D{
		{"renameCollection", db + "." + coll},
		{"to", targetDB + "." + targetColl},
		{"dropTarget", true},
	}

	err := c.target.Database("admin").RunCommand(ctx, opts).Err()
	if err != nil {
		if topo.IsNamespaceNotFound(err) {
			log.Ctx(ctx).Errorf(err, "")

			return nil
		}

		return errors.Wrap(err, "rename collection")
	}

	c.renameCollectionInCatalog(ctx, db, coll, targetDB, targetColl)

	return nil
}

// DropIndex drops an index in the target MongoDB.
func (c *Catalog) DropIndex(ctx context.Context, db, coll, index string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	err := c.target.Database(db).Collection(coll).Indexes().DropOne(ctx, index)
	if err != nil {
		if !topo.IsIndexNotFound(err) {
			return err //nolint:wrapcheck
		}

		log.Ctx(ctx).Warn(err.Error())
	}

	c.removeIndexFromCatalog(ctx, db, coll, index)

	return nil
}

func (c *Catalog) SetCollectionTimestamp(ctx context.Context, db, coll string, ts bson.Timestamp) {
	c.lock.Lock()
	defer c.lock.Unlock()

	databaseEntry, ok := c.Databases[db]
	if !ok {
		log.Ctx(ctx).Warnf("Catalog: set collection ts: database %q is not found", db)

		return
	}

	collectionEntry, ok := databaseEntry.Collections[coll]
	if !ok {
		log.Ctx(ctx).Warnf("Catalog: set collection ts: namespace %q is not found", db+"."+coll)

		return
	}

	collectionEntry.AddedAt = ts
	databaseEntry.Collections[coll] = collectionEntry
	c.Databases[db] = databaseEntry
}

// Finalize finalizes the indexes in the target MongoDB.
func (c *Catalog) Finalize(ctx context.Context) error {
	c.lock.RLock()
	defer c.lock.RUnlock()

	lg := log.Ctx(ctx)

	for db, colls := range c.Databases {
		for coll, collEntry := range colls.Collections {
			for _, index := range collEntry.Indexes {
				if index.IsClustered() {
					lg.Warn("Clustered index with TTL is not supported")

					continue
				}

				// restore properties
				switch { // unique and prepareUnique are mutually exclusive.
				case index.Unique != nil && *index.Unique:
					lg.Info("Convert index to prepareUnique: " + index.Name)

					err := c.doModifyIndexOption(ctx, db, coll, index.Name, "prepareUnique", true)
					if err != nil {
						return errors.Wrap(err, "convert to unique: prepareUnique: "+index.Name)
					}

					lg.Info("Convert prepareUnique index to unique: " + index.Name)

					err = c.doModifyIndexOption(ctx, db, coll, index.Name, "unique", true)
					if err != nil {
						return errors.Wrap(err, "convert to unique: "+index.Name)
					}

				case index.PrepareUnique != nil && *index.PrepareUnique:
					lg.Info("Convert prepareUnique index to unique: " + index.Name)

					err := c.doModifyIndexOption(ctx, db, coll, index.Name, "prepareUnique", true)
					if err != nil {
						return errors.Wrap(err, "convert to prepareUnique: "+index.Name)
					}
				}

				if index.ExpireAfterSeconds != nil {
					lg.Info("Modify index expireAfterSeconds: " + index.Name)

					err := c.doModifyIndexOption(ctx,
						db, coll, index.Name, "expireAfterSeconds", *index.ExpireAfterSeconds)
					if err != nil {
						return errors.Wrap(err, "modify expireAfterSeconds: "+index.Name)
					}
				}

				if index.Hidden != nil {
					lg.Info("Modify index hidden: " + index.Name)

					err := c.doModifyIndexOption(ctx, db, coll, index.Name, "hidden", index.Hidden)
					if err != nil {
						return errors.Wrap(err, "modify hidden: "+index.Name)
					}
				}
			}
		}
	}

	return nil
}

// doModifyIndexOption modifies an index property in the target MongoDB.
func (c *Catalog) doModifyIndexOption(
	ctx context.Context,
	db string,
	coll string,
	index string,
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

// getIndexFromCatalog gets an index spec from the catalog.
func (c *Catalog) getIndexFromCatalog(db, coll, index string) *topo.IndexSpecification {
	dbCat, ok := c.Databases[db]
	if !ok || len(dbCat.Collections) == 0 {
		return nil
	}

	collCat, ok := dbCat.Collections[coll]
	if !ok {
		return nil
	}

	for _, indexSpec := range collCat.Indexes {
		if indexSpec.Name == index {
			return indexSpec
		}
	}

	return nil
}

// addIndexesToCatalog adds indexes to the catalog.
func (c *Catalog) addIndexesToCatalog(
	ctx context.Context,
	db string,
	coll string,
	indexes []*topo.IndexSpecification,
) {
	dbCat, ok := c.Databases[db]
	if !ok {
		log.Ctx(ctx).Errorf(nil, "Catalog: add indexes: database %q not found", db)

		c.Databases[db] = databaseCatalog{
			Collections: map[string]collectionCatalog{
				coll: {
					Indexes: slices.Clone(indexes),
				},
			},
		}

		return
	}

	collCat, ok := dbCat.Collections[coll]
	if !ok {
		log.Ctx(ctx).Errorf(nil, "Catalog: add indexes: namespace %q not found", db+"."+coll)

		dbCat.Collections[coll] = collectionCatalog{
			Indexes: slices.Clone(indexes),
		}
		c.Databases[db] = dbCat

		return
	}

	for _, index := range indexes {
		found := false

		for i, catIndex := range collCat.Indexes {
			if catIndex.Name == index.Name {
				log.Ctx(ctx).
					Warnf("Catalog: add indexes: index %q already exists in %q namespace",
						index.Name, db+"."+coll)

				collCat.Indexes[i] = index
				found = true

				break
			}
		}

		if !found {
			collCat.Indexes = append(collCat.Indexes, index)
		}
	}

	dbCat.Collections[coll] = collCat
	c.Databases[db] = dbCat
}

// removeIndexFromCatalog removes an index from the catalog.
func (c *Catalog) removeIndexFromCatalog(ctx context.Context, db, coll, index string) {
	databaseEntry, ok := c.Databases[db]
	if !ok {
		log.Ctx(ctx).Warnf("Catalog: remove index: database %q is not found", db)

		return
	}

	collectionEntry, ok := databaseEntry.Collections[coll]
	if !ok {
		log.Ctx(ctx).Warnf("Catalog: remove index: namespace %q is not found", db+"."+coll)

		return
	}

	if len(collectionEntry.Indexes) == 0 {
		log.Ctx(ctx).Warnf("Catalog: remove index: no indexes for namespace %q", db+"."+coll)

		return
	}

	indexes := collectionEntry.Indexes

	if len(indexes) == 1 && indexes[0].Name == index {
		collectionEntry.Indexes = nil

		return
	}

	for i := range indexes {
		if indexes[i].Name == index {
			copy(indexes[i:], indexes[i+1:])
			collectionEntry.Indexes = indexes[:len(indexes)-2]

			return
		}
	}

	log.Ctx(ctx).
		Warnf("Catalog: remove index: index %q not found in namespace %q", index, db+"."+coll)
}

// addCollectionToCatalog adds a collection to the catalog.
func (c *Catalog) addCollectionToCatalog(ctx context.Context, db, coll string) {
	dbCat, ok := c.Databases[db]
	if !ok {
		dbCat = databaseCatalog{
			Collections: make(map[string]collectionCatalog),
		}
	}

	if _, ok = dbCat.Collections[coll]; ok {
		log.Ctx(ctx).
			Errorf(nil, "Catalog: add collection: namespace %q already exists", db+"."+coll)

		return
	}

	dbCat.Collections[coll] = collectionCatalog{}
	c.Databases[db] = dbCat
}

// deleteCollectionFromCatalog deletes a collection entry from the catalog.
func (c *Catalog) deleteCollectionFromCatalog(db, coll string) {
	databaseEntry, ok := c.Databases[db]
	if !ok {
		return
	}

	delete(databaseEntry.Collections, coll)
	if len(databaseEntry.Collections) == 0 {
		delete(c.Databases, db)
	}
}

func (c *Catalog) deleteDatabaseFromCatalog(db string) {
	delete(c.Databases, db)
}

func (c *Catalog) renameCollectionInCatalog(
	ctx context.Context,
	db string,
	coll string,
	targetDB string,
	targetColl string,
) {
	databaseEntry, ok := c.Databases[db]
	if !ok {
		log.Ctx(ctx).Errorf(nil, "Catalog: rename collection: database %q is not found", db)

		return
	}

	collectionEntry, ok := databaseEntry.Collections[coll]
	if !ok {
		log.Ctx(ctx).
			Errorf(nil, "Catalog: rename collection: namespace %q is not found", db+"."+coll)

		return
	}

	c.addCollectionToCatalog(ctx, targetDB, targetColl)
	c.Databases[targetDB].Collections[targetColl] = collectionEntry
	c.deleteCollectionFromCatalog(db, coll)
}
