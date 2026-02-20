// Package catalog manages the MongoDB catalog for the target cluster.
// It handles collection and index creation, modification, and tracking.
package catalog

import (
	"context"
	"encoding/hex"
	"math"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/log"
	"github.com/percona/percona-clustersync-mongodb/topo"
)

// ErrTimeseriesUnsupported is returned when a timeseries collection is encountered.
var ErrTimeseriesUnsupported = errors.New("timeseries is not supported")

// IDIndex is the name of the "_id" index.
const IDIndex = "_id_"

const (
	// SystemPrefix is the prefix for system collections.
	SystemPrefix = "system."
	// TimeseriesPrefix is the prefix for timeseries buckets.
	TimeseriesPrefix = "system.buckets."
)

// Namespace is the namespace (database and/or collection) affected by the event.
type Namespace struct {
	// Database is the name of the database where the event occurred.
	Database string `bson:"db"`

	// Collection is the name of the collection where the event occurred.
	Collection string `bson:"coll"`

	// Sharded indicates whether the collection is sharded.
	Sharded bool

	// ShardKey is the shard key used for the collection.
	ShardKey bson.D

	// Capped indicates whether the collection is capped.
	Capped bool
}

// String returns the string representation of the namespace.
func (ns Namespace) String() string {
	var rv string

	if ns.Collection != "" {
		rv = ns.Database + "." + ns.Collection
	} else {
		rv = ns.Database
	}

	return rv
}

// ParseNamespace parses a namespace string into a Namespace struct.
func ParseNamespace(ns string) (Namespace, error) {
	parts := strings.SplitN(ns, ".", 2) //nolint:mnd

	if len(parts) != 2 { //nolint:mnd
		return Namespace{}, errors.Errorf("invalid namespace %q", ns)
	}

	return Namespace{
		Database:   parts[0],
		Collection: parts[1],
	}, nil
}

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

// BaseCatalog defines the shared collection-level operations used by clone and repl packages.
type BaseCatalog interface {
	DropCollection(ctx context.Context, db, coll string) error
	CreateCollection(ctx context.Context, db, coll string, opts *CreateCollectionOptions) error
	CreateIndexes(ctx context.Context, db, coll string, indexes []*topo.IndexSpecification) error
	ShardCollection(ctx context.Context, db, coll string, shardKey bson.D, unique bool) error
	SetCollectionUUID(ctx context.Context, db, coll string, uuid *bson.Binary)
}

var _ BaseCatalog = (*Catalog)(nil)

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
	AddedAt  bson.Timestamp
	UUID     *bson.Binary
	Indexes  []indexCatalogEntry
	Sharded  bool
	ShardKey bson.D
	Capped   bool
}

type indexCatalogEntry struct {
	*topo.IndexSpecification

	Incomplete   bool `bson:"incomplete"`
	Failed       bool `bson:"failed"`
	Inconsistent bool `bson:"inconsistent"`
}

func (i indexCatalogEntry) Unsuccessful() bool {
	return i.Failed || i.Incomplete || i.Inconsistent
}

// NewCatalog creates a new Catalog.
func NewCatalog(target *mongo.Client) *Catalog {
	return &Catalog{
		target:    target,
		Databases: make(map[string]databaseCatalog),
	}
}

// Checkpoint is the checkpoint data for the catalog as part of the recovery mechanism.
type Checkpoint struct {
	Catalog map[string]databaseCatalog `bson:"catalog"`
}

// LockWrite locks the catalog for writing.
func (c *Catalog) LockWrite() {
	c.lock.RLock()
}

// UnlockWrite unlocks the catalog for writing.
func (c *Catalog) UnlockWrite() {
	c.lock.RUnlock()
}

// Checkpoint returns [Checkpoint] as a part of recovery mechanism.
//
// The [Catalog.LockWrite] must be called before the function is called and
// the [Catalog.UnlockWrite] must be called after the return value is no used anymore.
func (c *Catalog) Checkpoint() *Checkpoint {
	// do not call [sync.RWMutex.RLock] to avoid deadlock through recursive read-locking
	// that may happen during clone or change replication
	if len(c.Databases) == 0 {
		return nil
	}

	return &Checkpoint{Catalog: c.Databases}
}

// Recover restores the catalog from a checkpoint.
func (c *Catalog) Recover(cp *Checkpoint) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if len(c.Databases) != 0 {
		return errors.New("cannot restore")
	}

	c.Databases = cp.Catalog

	return nil
}

// CreateCollection creates a collection in the target MongoDB.
func (c *Catalog) CreateCollection(
	ctx context.Context,
	db string,
	coll string,
	opts *CreateCollectionOptions,
) error {
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

	err := runWithRetry(ctx, func(ctx context.Context) error {
		err := c.target.Database(db).RunCommand(ctx, cmd).Err()

		return errors.Wrapf(err, "create collection %s.%s", db, coll)
	})
	if err != nil && !topo.IsNamespaceExists(err) {
		return err
	}

	log.Ctx(ctx).Debugf("Created collection %s.%s", db, coll)

	c.lock.Lock()
	c.addCollectionToCatalog(ctx, db, coll, opts.Capped != nil && *opts.Capped)
	c.lock.Unlock()

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

	err := runWithRetry(ctx, func(ctx context.Context) error {
		err := c.target.Database(db).RunCommand(ctx, cmd).Err()

		return errors.Wrapf(err, "create view %s.%s", db, view)
	})
	if err != nil && !topo.IsNamespaceExists(err) {
		return err
	}

	log.Ctx(ctx).Debugf("Created view %s.%s", db, view)

	return nil
}

// DropCollection drops a collection in the target MongoDB.
func (c *Catalog) DropCollection(ctx context.Context, db, coll string) error {
	err := runWithRetry(ctx, func(ctx context.Context) error {
		err := c.target.Database(db).Collection(coll).Drop(ctx)

		return errors.Wrapf(err, "drop collection %s.%s", db, coll)
	})
	if err != nil {
		return err
	}

	log.Ctx(ctx).Debugf("Dropped collection %s.%s", db, coll)

	c.lock.Lock()
	c.deleteCollectionFromCatalog(ctx, db, coll)
	c.lock.Unlock()

	return nil
}

// DropDatabase drops a database in the target MongoDB.
func (c *Catalog) DropDatabase(ctx context.Context, db string) error {
	lg := log.Ctx(ctx)

	colls, err := topo.ListCollectionNames(ctx, c.target, db)
	if err != nil {
		return errors.Wrap(err, "list collection names")
	}

	eg, grpCtx := errgroup.WithContext(ctx)

	for _, coll := range colls {
		eg.Go(func() error {
			err := runWithRetry(grpCtx, func(ctx context.Context) error {
				err := c.target.Database(db).Collection(coll).Drop(ctx)

				return errors.Wrapf(err, "drop namespace %s.%s", db, coll)
			})
			if err != nil {
				return err
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

		if index.Collation == nil {
			lg.Info("Create index with missing collation, setting simple collation: " + index.Name)

			d := bson.D{{"locale", "simple"}}

			collation, err := bson.Marshal(d)
			if err != nil {
				return errors.Wrapf(err, "marshal simple collation for index %s.%s.%s",
					db, coll, index.Name)
			}

			idxCopy := *index
			idxCopy.Collation = collation
			index = &idxCopy
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
		err := runWithRetry(ctx, func(ctx context.Context) error {
			err := c.target.Database(db).RunCommand(ctx, bson.D{
				{"createIndexes", coll},
				{"indexes", bson.A{index}},
			}).Err()

			return errors.Wrapf(err, "create index %s.%s.%s", db, coll, index.Name)
		})
		if err != nil {
			processedIdxs[index.Name] = err

			continue
		}

		processedIdxs[index.Name] = nil
	}

	successfulIdxs := make([]indexCatalogEntry, 0, len(processedIdxs))
	successfulIdxNames := make([]string, 0, len(processedIdxs))

	failedIdxs := make([]*topo.IndexSpecification, 0, len(processedIdxs))

	var idxErrors []error

	for _, idx := range indexes {
		err := processedIdxs[idx.Name]
		if err != nil {
			failedIdxs = append(failedIdxs, idx)
			idxErrors = append(idxErrors, errors.Wrap(err, "create index: "+idx.Name))

			continue
		}

		successfulIdxs = append(successfulIdxs, indexCatalogEntry{IndexSpecification: idx})
		successfulIdxNames = append(successfulIdxNames, idx.Name)
	}

	lg.Debugf("Created indexes on %s.%s: %s", db, coll, strings.Join(successfulIdxNames, ", "))

	c.lock.Lock()
	c.addIndexesToCatalog(ctx, db, coll, successfulIdxs)
	c.lock.Unlock()

	if len(idxErrors) > 0 {
		c.AddFailedIndexes(ctx, db, coll, failedIdxs)

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

		lg.Debugf("Added incomplete index %q for %s.%s to catalog", index.Name, db, coll)
	}

	c.lock.Lock()
	c.addIndexesToCatalog(ctx, db, coll, indexEntries)
	c.lock.Unlock()
}

// AddFailedIndexes adds indexes in the catalog that failed to create on the target cluster.
// The indexes have set [indexCatalogEntry.Failed] flag.
func (c *Catalog) AddFailedIndexes(
	ctx context.Context,
	db string,
	coll string,
	indexes []*topo.IndexSpecification,
) {
	lg := log.Ctx(ctx)

	if len(indexes) == 0 {
		lg.Error(nil, "No failed indexes to add")

		return
	}

	indexEntries := make([]indexCatalogEntry, len(indexes))
	for i, index := range indexes {
		indexEntries[i] = indexCatalogEntry{
			IndexSpecification: index,
			Failed:             true,
		}

		lg.Tracef("Added failed index %q for %s.%s to catalog", index.Name, db, coll)
	}

	c.lock.Lock()
	c.addIndexesToCatalog(ctx, db, coll, indexEntries)
	c.lock.Unlock()
}

// AddInconsistentIndexes adds indexes in the catalog that are inconsistent across shards on the source cluster.
// The indexes have set [indexCatalogEntry.Inconsistent] flag.
func (c *Catalog) AddInconsistentIndexes(
	ctx context.Context,
	db string,
	coll string,
	indexes []*topo.IndexSpecification,
) {
	lg := log.Ctx(ctx)

	if len(indexes) == 0 {
		lg.Error(nil, "No inconsistent indexes to add")

		return
	}

	indexEntries := make([]indexCatalogEntry, len(indexes))
	for i, index := range indexes {
		indexEntries[i] = indexCatalogEntry{
			IndexSpecification: index,
			Inconsistent:       true,
		}

		lg.Debugf("Added inconsistent index %q for %s.%s to catalog", index.Name, db, coll)
	}

	c.lock.Lock()
	c.addIndexesToCatalog(ctx, db, coll, indexEntries)
	c.lock.Unlock()
}

// ModifyCappedCollection modifies a capped collection in the target MongoDB.
func (c *Catalog) ModifyCappedCollection(
	ctx context.Context,
	db string,
	coll string,
	sizeBytes *int64,
	maxDocs *int64,
) error {
	cmd := bson.D{{"collMod", coll}}
	if sizeBytes != nil {
		cmd = append(cmd, bson.E{"cappedSize", sizeBytes})
	}

	if maxDocs != nil {
		cmd = append(cmd, bson.E{"cappedMax", maxDocs})
	}

	return runWithRetry(ctx, func(ctx context.Context) error {
		err := c.target.Database(db).RunCommand(ctx, cmd).Err()

		return errors.Wrapf(err, "modify capped collection %s.%s", db, coll)
	})
}

// ModifyView modifies a view in the target MongoDB.
func (c *Catalog) ModifyView(ctx context.Context, db, view, viewOn string, pipeline any) error {
	cmd := bson.D{
		{"collMod", view},
		{"viewOn", viewOn},
		{"pipeline", pipeline},
	}

	return runWithRetry(ctx, func(ctx context.Context) error {
		err := c.target.Database(db).RunCommand(ctx, cmd).Err()

		return errors.Wrapf(err, "modify view %s.%s", db, view)
	})
}

// ModifyChangeStreamPreAndPostImages modifies the changeStreamPreAndPostImages option for a collection.
func (c *Catalog) ModifyChangeStreamPreAndPostImages(
	ctx context.Context,
	db string,
	coll string,
	enabled bool,
) error {
	cmd := bson.D{
		{"collMod", coll},
		{"changeStreamPreAndPostImages", bson.D{{"enabled", enabled}}},
	}

	return runWithRetry(ctx, func(ctx context.Context) error {
		err := c.target.Database(db).RunCommand(ctx, cmd).Err()

		return errors.Wrapf(err, "modify changeStreamPreAndPostImages %s.%s", db, coll)
	})
}

// ModifyValidation modifies a capped collection in the target MongoDB.
func (c *Catalog) ModifyValidation(
	ctx context.Context,
	db string,
	coll string,
	validator *bson.Raw,
	validationLevel *string,
	validationAction *string,
) error {
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

	return runWithRetry(ctx, func(ctx context.Context) error {
		err := c.target.Database(db).RunCommand(ctx, cmd).Err()

		return errors.Wrapf(err, "modify validation %s.%s", db, coll)
	})
}

// ModifyIndex modifies an index in the target MongoDB.
func (c *Catalog) ModifyIndex(ctx context.Context, db, coll string, mods *ModifyIndexOption) error {
	if mods.ExpireAfterSeconds != nil {
		cmd := bson.D{
			{"collMod", coll},
			{"index", bson.D{
				{"name", mods.Name},
				{"expireAfterSeconds", math.MaxInt32},
			}},
		}

		err := runWithRetry(ctx, func(ctx context.Context) error {
			err := c.target.Database(db).RunCommand(ctx, cmd).Err()

			return errors.Wrapf(err, "modify index %s.%s.%s", db, coll, mods.Name)
		})
		if err != nil {
			return err
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

// Rename renames a collection in the target MongoDB.
func (c *Catalog) Rename(ctx context.Context, db, coll, targetDB, targetColl string) error {
	lg := log.Ctx(ctx)

	opts := bson.D{
		{"renameCollection", db + "." + coll},
		{"to", targetDB + "." + targetColl},
		{"dropTarget", true},
	}

	err := runWithRetry(ctx, func(ctx context.Context) error {
		err := c.target.Database("admin").RunCommand(ctx, opts).Err()

		return errors.Wrapf(err, "rename collection %s.%s to %s.%s", db, coll, targetDB, targetColl)
	})
	if err != nil {
		if topo.IsNamespaceNotFound(err) {
			lg.Errorf(err, "")

			return nil
		}

		return err
	}

	lg.Debugf("Renamed collection %s.%s to %s.%s", db, coll, targetDB, targetColl)

	c.renameCollectionInCatalog(ctx, db, coll, targetDB, targetColl)

	return nil
}

// DropIndex drops an index in the target MongoDB.
func (c *Catalog) DropIndex(ctx context.Context, db, coll, index string) error {
	lg := log.Ctx(ctx)

	err := runWithRetry(ctx, func(ctx context.Context) error {
		err := c.target.Database(db).Collection(coll).Indexes().DropOne(ctx, index)

		return errors.Wrapf(err, "drop index %s.%s.%s", db, coll, index)
	})
	if err != nil {
		if !topo.IsNamespaceNotFound(err) && !topo.IsIndexNotFound(err) {
			return err
		}

		lg.Warn(err.Error())
	}

	lg.Debugf("Dropped index %s.%s.%s", db, coll, index)

	c.removeIndexFromCatalog(ctx, db, coll, index)

	return nil
}

// SetCollectionTimestamp sets the timestamp for a collection in the catalog.
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

// SetCollectionUUID sets the UUID for a collection in the catalog.
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

// UUIDMap returns a map of collection UUIDs to their namespaces.
func (c *Catalog) UUIDMap() UUIDMap {
	c.lock.RLock()
	defer c.lock.RUnlock()

	uuidMap := make(UUIDMap)

	for db, dbCat := range c.Databases {
		for coll, collCat := range dbCat.Collections {
			if collCat.UUID != nil {
				uuidMap[hex.EncodeToString(collCat.UUID.Data)] = Namespace{
					Database:   db,
					Collection: coll,
					Sharded:    collCat.Sharded,
					ShardKey:   collCat.ShardKey,
					Capped:     collCat.Capped,
				}
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

	foundUnsuccessfulIdx := false

	for db, colls := range c.Databases {
		for coll, collEntry := range colls.Collections {
			nsLg := lg.With(log.NS(db, coll))

			for _, index := range collEntry.Indexes {
				if index.Unsuccessful() {
					foundUnsuccessfulIdx = true

					continue
				}

				if index.IsClustered() {
					nsLg.Warn("Clustered index with TTL is not supported")

					continue
				}

				// restore properties
				switch { // unique and prepareUnique are mutually exclusive.
				case index.Unique != nil && *index.Unique:
					nsLg.Info("Convert index to prepareUnique: " + index.Name)

					err := c.doModifyIndexOption(ctx, db, coll, index.Name, "prepareUnique", true)
					if err != nil {
						idxErrors = append(idxErrors,
							errors.Wrap(err, "convert to prepareUnique: "+index.Name))

						continue
					}

					nsLg.Info("Convert prepareUnique index to unique: " + index.Name)

					err = c.doModifyIndexOption(ctx, db, coll, index.Name, "unique", true)
					if err != nil {
						idxErrors = append(idxErrors,
							errors.Wrap(err, "convert to unique: "+index.Name))

						continue
					}

				case index.PrepareUnique != nil && *index.PrepareUnique:
					nsLg.Info("Convert prepareUnique index to unique: " + index.Name)

					err := c.doModifyIndexOption(ctx, db, coll, index.Name, "prepareUnique", true)
					if err != nil {
						idxErrors = append(idxErrors,
							errors.Wrap(err, "convert to prepareUnique: "+index.Name))

						continue
					}
				}

				if index.ExpireAfterSeconds != nil {
					nsLg.Info("Modify index expireAfterSeconds: " + index.Name)

					err := c.doModifyIndexOption(ctx,
						db, coll, index.Name, "expireAfterSeconds", *index.ExpireAfterSeconds)
					if err != nil {
						idxErrors = append(idxErrors,
							errors.Wrap(err, "modify expireAfterSeconds: "+index.Name))

						continue
					}
				}

				if index.Hidden != nil {
					nsLg.Info("Modify index hidden: " + index.Name)

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

	if foundUnsuccessfulIdx {
		c.finalizeUnsuccessfulIndexes(ctx)
	}

	if len(idxErrors) > 0 {
		lg.Errorf(errors.Join(idxErrors...), "Finalize indexes")
	}

	return nil
}

// finalizeUnsuccessfulIndexes finalizes indexes that were unsuccessful
// during replication, failed, incomplete, or inconsistent.
func (c *Catalog) finalizeUnsuccessfulIndexes(ctx context.Context) {
	lg := log.Ctx(ctx)
	lg.Info("Finalizing unsuccessful indexes")

	for db, colls := range c.Databases {
		for coll, collEntry := range colls.Collections {
			for _, index := range collEntry.Indexes {
				if !index.Unsuccessful() {
					continue // skip successful indexes
				}

				if index.Inconsistent {
					lg.Warnf("Index %s on %s.%s was inconsistent across shards on source, skipping",
						index.Name, db, coll)

					continue // don't try to recreate inconsistent indexes
				}

				if index.Incomplete {
					lg.Infof("Index %s on %s.%s was incomplete during replication, trying to create it",
						index.Name, db, coll)
				}

				if index.Failed {
					lg.Infof("Index %s on %s.%s failed to create during replication, trying to recreate it",
						index.Name, db, coll)
				}

				err := runWithRetry(ctx, func(ctx context.Context) error {
					err := c.target.Database(db).RunCommand(ctx, bson.D{
						{"createIndexes", coll},
						{"indexes", bson.A{index.IndexSpecification}},
					}).Err()

					return errors.Wrapf(err, "recreate index %s.%s.%s", db, coll, index.Name)
				})
				if err != nil {
					lg.Warnf("Failed to recreate unsuccessful index %s on %s.%s: %v",
						index.Name, db, coll, err)

					continue
				}

				lg.Infof("Recreated index %s on %s.%s", index.Name, db, coll)

				c.addIndexesToCatalog(ctx, db, coll, []indexCatalogEntry{{IndexSpecification: index.IndexSpecification}})
			}
		}
	}
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
	return runWithRetry(ctx, func(ctx context.Context) error {
		err := c.target.Database(db).RunCommand(ctx, bson.D{
			{"collMod", coll},
			{"index", bson.D{
				{"name", index},
				{propName, value},
			}},
		}).Err()

		return errors.Wrapf(err, "modify index %s.%s.%s: %s", db, coll, index, propName)
	})
}

// getIndexFromCatalog gets an index spec from the catalog.
func (c *Catalog) getIndexFromCatalog(db, coll, index string) *topo.IndexSpecification {
	c.lock.RLock()
	defer c.lock.RUnlock()

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
	c.lock.Lock()
	defer c.lock.Unlock()

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
func (c *Catalog) addCollectionToCatalog(ctx context.Context, db, coll string, capped bool) {
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

	dbCat.Collections[coll] = collectionCatalog{Capped: capped}
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
	c.lock.Lock()
	defer c.lock.Unlock()

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
	c.lock.Lock()
	defer c.lock.Unlock()

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

	c.addCollectionToCatalog(ctx, targetDB, targetColl, collectionEntry.Capped)
	c.Databases[targetDB].Collections[targetColl] = collectionEntry
	c.deleteCollectionFromCatalog(ctx, db, coll)

	lg.Debugf("Collection renamed in catalog %s.%s to %s.%s", db, coll, targetDB, targetColl)
}

// ShardCollection shards a collection in the target MongoDB.
func (c *Catalog) ShardCollection(
	ctx context.Context,
	db string,
	coll string,
	shardKey bson.D,
	unique bool,
) error {
	cmd := bson.D{
		{Key: "shardCollection", Value: db + "." + coll},
		{Key: "key", Value: shardKey},
		{"collation", bson.D{{"locale", "simple"}}},
	}

	if unique {
		cmd = append(cmd,
			bson.E{Key: "unique", Value: true},
			bson.E{Key: "enforceUniquenessCheck", Value: false},
		)
	}

	err := runWithRetry(ctx, func(ctx context.Context) error {
		err := c.target.Database("admin").RunCommand(ctx, cmd).Err()

		return errors.Wrap(err, "shard collection")
	})
	if err != nil {
		return err
	}

	log.Ctx(ctx).Debugf("Sharded collection %s.%s", db, coll)

	c.lock.Lock()
	databaseEntry := c.Databases[db]
	collectionEntry := databaseEntry.Collections[coll]
	collectionEntry.Sharded = true
	collectionEntry.ShardKey = shardKey
	databaseEntry.Collections[coll] = collectionEntry
	c.Databases[db] = databaseEntry
	c.lock.Unlock()

	return nil
}

func runWithRetry(
	ctx context.Context,
	fn func(context.Context) error,
) error {
	return topo.RunWithRetry(ctx, fn, topo.DefaultRetryInterval, topo.DefaultMaxRetries) //nolint:wrapcheck
}
