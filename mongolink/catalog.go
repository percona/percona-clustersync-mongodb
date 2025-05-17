package mongolink

import (
	"context"
	"encoding/hex"
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

var ErrTimeseriesUnsupported = errors.New("timeseries is not supported")

// IDIndex is the name of the "_id" index.
const IDIndex = "_id_"

const (
	// SystemPrefix is the prefix for system collections.
	SystemPrefix = "system."
	// TimeseriesPrefix is the prefix for timeseries buckets.
	TimeseriesPrefix = "system.buckets."
)

// UUIDMap is mapping of hex string of a collection UUID to its namespace.
type UUIDMap map[string]Namespace

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

	ChangeStreamPreAndPostImages *struct {
		Enabled bool `bson:"enabled"`
	} `bson:"changeStreamPreAndPostImages,omitempty"`

	Validator        *bson.Raw `bson:"validator,omitempty"`
	ValidationLevel  *string   `bson:"validationLevel,omitempty"`
	ValidationAction *string   `bson:"validationAction,omitempty"`

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
	UUID    *bson.Binary
	Indexes []indexCatalogEntry
}

type indexCatalogEntry struct {
	*topo.IndexSpecification
	Incomplete bool `bson:"incomplete"`
}

func (i indexCatalogEntry) Ready() bool {
	return !i.Incomplete
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

// Checkpoint returns [catalogCheckpoint] as a part of recovery mechanism.
//
// The [Catalog.LockWrite] must be called before the function is called and
// the [Catalog.UnlockWrite] must be called after the return value is no used anymore.
func (c *Catalog) Checkpoint() *catalogCheckpoint { //nolint:revive
	// do not call [sync.RWMutex.RLock] to avoid deadlock through recursive read-locking
	// that may happen during clone or change replication

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

	if opts.ChangeStreamPreAndPostImages != nil {
		cmd = append(cmd, bson.E{"changeStreamPreAndPostImages", opts.ChangeStreamPreAndPostImages})
	}

	if opts.Validator != nil {
		cmd = append(cmd, bson.E{"validator", opts.Validator})
	}
	if opts.ValidationLevel != nil {
		cmd = append(cmd, bson.E{"validationLevel", opts.ValidationLevel})
	}
	if opts.ValidationAction != nil {
		cmd = append(cmd, bson.E{"validationAction", opts.ValidationAction})
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
	log.Ctx(ctx).Debugf("Created collection %s.%s", db, coll)

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

	log.Ctx(ctx).Debugf("Created view %s.%s", db, view)

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
	log.Ctx(ctx).Debugf("Dropped collection %s.%s", db, coll)

	c.deleteCollectionFromCatalog(ctx, db, coll)

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

			lg.Debugf("Dropped collection %s.%s", db, coll)

			return nil
		})
	}

	lg.Debugf("Dropped database %s", db)

	c.deleteDatabaseFromCatalog(ctx, db)

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

	processedIdxs := make(map[string]error, len(idxs))

	// NOTE: [mongo.IndexView.CreateMany] uses [mongo.IndexModel]
	// which does not support `prepareUnique`.
	for _, index := range idxs {
		res := c.target.Database(db).RunCommand(ctx, bson.D{
			{"createIndexes", coll},
			{"indexes", bson.A{index}},
		})

		if err := res.Err(); err != nil {
			processedIdxs[index.Name] = err

			continue
		}

		processedIdxs[index.Name] = nil
	}

	successfulIdxs := make([]indexCatalogEntry, 0, len(processedIdxs))
	successfulIdxNames := make([]string, 0, len(processedIdxs))
	var idxErrors []error

	for _, idx := range indexes {
		if err := processedIdxs[idx.Name]; err != nil {
			idxErrors = append(idxErrors, errors.Wrap(err, "create index: "+idx.Name))

			continue
		}

		successfulIdxs = append(successfulIdxs, indexCatalogEntry{IndexSpecification: idx})
		successfulIdxNames = append(successfulIdxNames, idx.Name)
	}

	lg.Debugf("Created indexes on %s.%s: %s", db, coll, strings.Join(successfulIdxNames, ", "))
	c.addIndexesToCatalog(ctx, db, coll, successfulIdxs)

	if len(idxErrors) > 0 {
		lg.Errorf(errors.Join(idxErrors...),
			"One or more indexes failed to create on %s.%s", db, coll)
	}

	return nil
}

// AddIncompleteIndexes adds indexes in the catalog but do not create them on the target cluster.
// The indexes have set [indexCatalogEntry.Incomplete] flag.
func (c *Catalog) AddIncompleteIndexes(
	ctx context.Context,
	db string,
	coll string,
	indexes []*topo.IndexSpecification,
) {
	c.lock.Lock()
	defer c.lock.Unlock()

	lg := log.Ctx(ctx)

	if len(indexes) == 0 {
		lg.Error(nil, "No incomplete indexes to add")

		return
	}

	indexEntries := make([]indexCatalogEntry, len(indexes))
	for i, index := range indexes {
		indexEntries[i] = indexCatalogEntry{
			IndexSpecification: index,
			Incomplete:         true,
		}

		lg.Tracef("Added incomplete index %q for %s.%s to catalog", index.Name, db, coll)
	}

	c.addIndexesToCatalog(ctx, db, coll, indexEntries)
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

func (c *Catalog) ModifyChangeStreamPreAndPostImages(
	ctx context.Context,
	db string,
	coll string,
	enabled bool,
) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	cmd := bson.D{
		{"collMod", coll},
		{"changeStreamPreAndPostImages", bson.D{{"enabled", enabled}}},
	}
	err := c.target.Database(db).RunCommand(ctx, cmd).Err()

	return err //nolint:wrapcheck
}

// ModifyCappedCollection modifies a capped collection in the target MongoDB.
func (c *Catalog) ModifyValidation(
	ctx context.Context,
	db string,
	coll string,
	validator *bson.Raw,
	validationLevel *string,
	validationAction *string,
) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	cmd := bson.D{{"collMod", coll}}
	if validator != nil {
		cmd = append(cmd, bson.E{"validator", validator})
	}
	if validationLevel != nil {
		cmd = append(cmd, bson.E{"validationLevel", validationLevel})
	}
	if validationAction != nil {
		cmd = append(cmd, bson.E{"validationAction", validationAction})
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

	lg := log.Ctx(ctx)

	opts := bson.D{
		{"renameCollection", db + "." + coll},
		{"to", targetDB + "." + targetColl},
		{"dropTarget", true},
	}

	err := c.target.Database("admin").RunCommand(ctx, opts).Err()
	if err != nil {
		if topo.IsNamespaceNotFound(err) {
			lg.Errorf(err, "")

			return nil
		}

		return errors.Wrap(err, "rename collection")
	}
	lg.Debugf("Renamed collection %s.%s to %s.%s", db, coll, targetDB, targetColl)

	c.renameCollectionInCatalog(ctx, db, coll, targetDB, targetColl)

	return nil
}

// DropIndex drops an index in the target MongoDB.
func (c *Catalog) DropIndex(ctx context.Context, db, coll, index string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	lg := log.Ctx(ctx)

	err := c.target.Database(db).Collection(coll).Indexes().DropOne(ctx, index)
	if err != nil {
		if !topo.IsNamespaceNotFound(err) && !topo.IsIndexNotFound(err) {
			return err //nolint:wrapcheck
		}

		lg.Warn(err.Error())
	}

	lg.Debugf("Dropped index %s.%s.%s", db, coll, index)

	c.removeIndexFromCatalog(ctx, db, coll, index)

	return nil
}

func (c *Catalog) SetCollectionTimestamp(ctx context.Context, db, coll string, ts bson.Timestamp) {
	c.lock.Lock()
	defer c.lock.Unlock()

	lg := log.Ctx(ctx)

	databaseEntry, ok := c.Databases[db]
	if !ok {
		lg.Warnf("set collection ts: database %q is not found", db)

		return
	}

	collectionEntry, ok := databaseEntry.Collections[coll]
	if !ok {
		lg.Warnf("set collection ts: namespace %q is not found", db+"."+coll)

		return
	}

	collectionEntry.AddedAt = ts
	databaseEntry.Collections[coll] = collectionEntry
	c.Databases[db] = databaseEntry
}

func (c *Catalog) SetCollectionUUID(ctx context.Context, db, coll string, uuid *bson.Binary) {
	c.lock.Lock()
	defer c.lock.Unlock()

	databaseEntry, ok := c.Databases[db]
	if !ok {
		log.Ctx(ctx).Warnf("set collection UUID: database %q is not found", db)

		return
	}

	collectionEntry, ok := databaseEntry.Collections[coll]
	if !ok {
		log.Ctx(ctx).Warnf("set collection UUID: namespace %q is not found", db+"."+coll)

		return
	}

	collectionEntry.UUID = uuid
	databaseEntry.Collections[coll] = collectionEntry
	c.Databases[db] = databaseEntry
}

func (c *Catalog) UUIDMap() UUIDMap {
	c.lock.RLock()
	defer c.lock.RUnlock()

	uuidMap := make(UUIDMap)

	for db, dbCat := range c.Databases {
		for coll, collCat := range dbCat.Collections {
			if collCat.UUID != nil {
				uuidMap[hex.EncodeToString(collCat.UUID.Data)] = Namespace{db, coll}
			}
		}
	}

	return uuidMap
}

// Finalize finalizes the indexes in the target MongoDB.
func (c *Catalog) Finalize(ctx context.Context) error {
	c.lock.RLock()
	defer c.lock.RUnlock()

	lg := log.Ctx(ctx)

	var idxErrors []error

	for db, colls := range c.Databases {
		for coll, collEntry := range colls.Collections {
			for _, index := range collEntry.Indexes {
				if !index.Ready() {
					lg.Warnf("Index %s on %s.%s was incomplete during replication, skipping it",
						index.Name, db, coll)

					continue
				}

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
						idxErrors = append(idxErrors,
							errors.Wrap(err, "convert to prepareUnique: "+index.Name))

						continue
					}

					lg.Info("Convert prepareUnique index to unique: " + index.Name)

					err = c.doModifyIndexOption(ctx, db, coll, index.Name, "unique", true)
					if err != nil {
						idxErrors = append(idxErrors,
							errors.Wrap(err, "convert to unique: "+index.Name))

						continue
					}

				case index.PrepareUnique != nil && *index.PrepareUnique:
					lg.Info("Convert prepareUnique index to unique: " + index.Name)

					err := c.doModifyIndexOption(ctx, db, coll, index.Name, "prepareUnique", true)
					if err != nil {
						idxErrors = append(idxErrors,
							errors.Wrap(err, "convert to prepareUnique: "+index.Name))

						continue
					}
				}

				if index.ExpireAfterSeconds != nil {
					lg.Info("Modify index expireAfterSeconds: " + index.Name)

					err := c.doModifyIndexOption(ctx,
						db, coll, index.Name, "expireAfterSeconds", *index.ExpireAfterSeconds)
					if err != nil {
						idxErrors = append(idxErrors,
							errors.Wrap(err, "modify expireAfterSeconds: "+index.Name))

						continue
					}
				}

				if index.Hidden != nil {
					lg.Info("Modify index hidden: " + index.Name)

					err := c.doModifyIndexOption(ctx, db, coll, index.Name, "hidden", index.Hidden)
					if err != nil {
						idxErrors = append(idxErrors,
							errors.Wrap(err, "modify hidden: "+index.Name))

						continue
					}
				}
			}
		}
	}

	if len(idxErrors) > 0 {
		lg.Errorf(errors.Join(idxErrors...), "Finalize indexes")
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
			return indexSpec.IndexSpecification
		}
	}

	return nil
}

// addIndexesToCatalog adds indexes to the catalog.
func (c *Catalog) addIndexesToCatalog(
	ctx context.Context,
	db string,
	coll string,
	indexes []indexCatalogEntry,
) {
	lg := log.Ctx(ctx)

	dbCat, ok := c.Databases[db]
	if !ok {
		lg.Errorf(nil, "add indexes: database %q not found", db)

		c.Databases[db] = databaseCatalog{
			Collections: map[string]collectionCatalog{coll: {Indexes: indexes}},
		}

		return
	}

	collCat, ok := dbCat.Collections[coll]
	if !ok {
		lg.Errorf(nil, "add indexes: namespace %q not found", db+"."+coll)

		dbCat.Collections[coll] = collectionCatalog{Indexes: indexes}
		c.Databases[db] = dbCat

		return
	}

	idxNames := make([]string, 0, len(collCat.Indexes))
	for _, index := range indexes {
		found := false

		for i, catIndex := range collCat.Indexes {
			if catIndex.Name == index.Name {
				lg.Warnf("add indexes: index %q already exists in %q namespace",
					index.Name, db+"."+coll)

				collCat.Indexes[i] = index
				found = true

				break
			}
		}

		if !found {
			collCat.Indexes = append(collCat.Indexes, index)
			idxNames = append(idxNames, index.Name)
		}
	}

	dbCat.Collections[coll] = collCat
	c.Databases[db] = dbCat
	lg.Debugf("Indexes added to catalog on %s.%s: , %s", db, coll, strings.Join(idxNames, ", "))
}

// removeIndexFromCatalog removes an index from the catalog.
func (c *Catalog) removeIndexFromCatalog(ctx context.Context, db, coll, index string) {
	lg := log.Ctx(ctx)

	databaseEntry, ok := c.Databases[db]
	if !ok {
		lg.Warnf("remove index: database %q is not found", db)

		return
	}

	collectionEntry, ok := databaseEntry.Collections[coll]
	if !ok {
		lg.Warnf("remove index: namespace %q is not found", db+"."+coll)

		return
	}

	if len(collectionEntry.Indexes) == 0 {
		lg.Warnf("remove index: no indexes for namespace %q", db+"."+coll)

		return
	}

	indexes := collectionEntry.Indexes

	if len(indexes) == 1 && indexes[0].Name == index {
		collectionEntry.Indexes = nil

		lg.Debugf("Indexes removed from catalog %s.%s", db, coll)

		return
	}

	for i := range indexes {
		if indexes[i].Name == index {
			copy(indexes[i:], indexes[i+1:])
			collectionEntry.Indexes = indexes[:len(indexes)-2]

			lg.Debugf("Indexes removed from catalog %s.%s", db, coll)

			return
		}
	}

	lg.Warnf("remove index: index %q not found in namespace %q", index, db+"."+coll)
}

// addCollectionToCatalog adds a collection to the catalog.
func (c *Catalog) addCollectionToCatalog(ctx context.Context, db, coll string) {
	lg := log.Ctx(ctx)

	dbCat, ok := c.Databases[db]
	if !ok {
		dbCat = databaseCatalog{
			Collections: make(map[string]collectionCatalog),
		}
	}

	if _, ok = dbCat.Collections[coll]; ok {
		lg.Errorf(nil, "add collection: namespace %q already exists", db+"."+coll)

		return
	}

	dbCat.Collections[coll] = collectionCatalog{}
	c.Databases[db] = dbCat
	lg.Debugf("Collection added to catalog %s.%s", db, coll)
}

// deleteCollectionFromCatalog deletes a collection entry from the catalog.
func (c *Catalog) deleteCollectionFromCatalog(ctx context.Context, db, coll string) {
	databaseEntry, ok := c.Databases[db]
	if !ok {
		return
	}

	delete(databaseEntry.Collections, coll)
	if len(databaseEntry.Collections) == 0 {
		delete(c.Databases, db)
	}
	log.Ctx(ctx).Debugf("Collection deleted from catalog %s.%s", db, coll)
}

func (c *Catalog) deleteDatabaseFromCatalog(ctx context.Context, db string) {
	delete(c.Databases, db)
	log.Ctx(ctx).Debugf("Database deleted from catalog %s", db)
}

func (c *Catalog) renameCollectionInCatalog(
	ctx context.Context,
	db string,
	coll string,
	targetDB string,
	targetColl string,
) {
	lg := log.Ctx(ctx)

	databaseEntry, ok := c.Databases[db]
	if !ok {
		lg.Errorf(nil, "rename collection: database %q is not found", db)

		return
	}

	collectionEntry, ok := databaseEntry.Collections[coll]
	if !ok {
		lg.Errorf(nil, "rename collection: namespace %q is not found", db+"."+coll)

		return
	}

	c.addCollectionToCatalog(ctx, targetDB, targetColl)
	c.Databases[targetDB].Collections[targetColl] = collectionEntry
	c.deleteCollectionFromCatalog(ctx, db, coll)
	lg.Debugf("Collection renamed in catalog %s.%s to %s.%s", db, coll, targetDB, targetColl)
}
