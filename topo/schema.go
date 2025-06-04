package topo

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/percona/percona-link-mongodb/errors"
)

type CollectionSpecification = mongo.CollectionSpecification

type CollectionType string

const (
	TypeCollection = "collection"
	TypeTimeseries = "timeseries"
	TypeView       = "view"
)

// IndexSpecification contains all index options.
//
// NOTE: [mongo.IndexView.CreateMany] and [mongo.IndexView.CreateOne] use [mongo.IndexModel]
// which does not support `prepareUnique`.
// GeoHaystack indexes cannot be created in version 5.0 and above (`bucketSize` field).
type IndexSpecification struct {
	Name               string   `bson:"name"`                         // Index name
	Namespace          string   `bson:"ns"`                           // Namespace
	KeysDocument       bson.Raw `bson:"key"`                          // Keys document
	Version            int32    `bson:"v"`                            // Version
	Sparse             *bool    `bson:"sparse,omitempty"`             // Sparse index
	Hidden             *bool    `bson:"hidden,omitempty"`             // Hidden index
	Unique             *bool    `bson:"unique,omitempty"`             // Unique index
	PrepareUnique      *bool    `bson:"prepareUnique,omitempty"`      // Prepare unique index
	Clustered          *bool    `bson:"clustered,omitempty"`          // Clustered index
	ExpireAfterSeconds *int64   `bson:"expireAfterSeconds,omitempty"` // Expire after seconds

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

// IsClustered returns true if the index is clustered.
func (s *IndexSpecification) IsClustered() bool {
	return s.Clustered != nil && *s.Clustered
}

func ListDatabaseNames(ctx context.Context, m *mongo.Client) ([]string, error) {
	//nolint:wrapcheck
	return m.ListDatabaseNames(ctx,
		bson.D{{"name", bson.D{{"$nin", bson.A{"admin", "config", "local"}}}}})
}

// ListCollectionNames returns a list of non-system collection names in the specified database.
func ListCollectionNames(ctx context.Context, m *mongo.Client, dbName string) ([]string, error) {
	//nolint:wrapcheck
	return m.Database(dbName).ListCollectionNames(ctx,
		bson.D{{"name", bson.D{{"$not", bson.D{{"$regex", "^system\\."}}}}}})
}

var ErrNotFound = errors.New("not found")

// ListCollectionSpecs retrieves the specifications of collections.
func ListCollectionSpecs(
	ctx context.Context,
	m *mongo.Client,
	dbName string,
) ([]CollectionSpecification, error) {
	//nolint:wrapcheck
	return m.Database(dbName).ListCollectionSpecifications(ctx,
		bson.D{{"name", bson.D{{"$not", bson.D{{"$regex", "^system\\."}}}}}})
}

// GetCollectionSpec retrieves the specification of a collection.
func GetCollectionSpec(
	ctx context.Context,
	m *mongo.Client,
	dbName string,
	collName string,
) (*CollectionSpecification, error) {
	colls, err := m.Database(dbName).ListCollectionSpecifications(ctx, bson.D{{"name", collName}})
	if err != nil {
		if IsNamespaceNotFound(err) {
			err = ErrNotFound
		}

		return nil, err //nolint:wrapcheck
	}

	if len(colls) == 0 {
		return nil, ErrNotFound
	}

	coll := colls[0] // copy to release the slice memory

	return &coll, nil
}

// GetCollectionNameByUUID retrieves the collection name by its UUID.
func GetCollectionNameByUUID(
	ctx context.Context,
	m *mongo.Client,
	dbName string,
	uuid bson.Binary,
) (string, error) {
	specs, err := m.Database(dbName).ListCollectionSpecifications(ctx, bson.D{{"info.uuid", uuid}})
	if err != nil {
		return "", errors.Wrap(err, "listCollections")
	}

	if len(specs) == 0 {
		return "", ErrNotFound
	}

	return specs[0].Name, nil
}

// ListIndexes retrieves the specifications of indexes for a collection.
func ListIndexes(
	ctx context.Context,
	m *mongo.Client,
	db string,
	coll string,
) ([]*IndexSpecification, error) {
	cur, err := m.Database(db).Collection(coll).Indexes().List(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "list indexes")
	}

	var indexes []*IndexSpecification

	err = cur.All(ctx, &indexes)
	if err != nil {
		return nil, errors.Wrap(err, "list indexes")
	}

	return indexes, nil
}

func ListInProgressIndexBuilds(
	ctx context.Context,
	m *mongo.Client,
	db string,
	coll string,
) ([]string, error) {
	opts := options.Database().
		SetReadPreference(readpref.Primary()).
		SetReadConcern(readconcern.Local())
	cur, err := m.Database("admin", opts).Aggregate(ctx, mongo.Pipeline{
		{{"$currentOp", bson.D{{"allUsers", true}}}},
		{{"$match", bson.D{
			{"op", "command"},
			{"command.createIndexes", coll},
			{"command.$db", db},
		}}},
		{{"$unwind", "$command.indexes"}},
		{{"$replaceRoot", bson.D{{"newRoot", "$command.indexes"}}}},
		{{"$project", bson.D{{"name", 1}}}},
	})
	if err != nil {
		return nil, errors.Wrap(err, "$currentOp")
	}

	var indexBuilds []struct {
		Name string `bson:"name"`
	}

	err = cur.All(ctx, &indexBuilds)
	if err != nil {
		return nil, errors.Wrap(err, "cursor: all")
	}

	if len(indexBuilds) == 0 {
		return []string(nil), nil
	}

	names := make([]string, len(indexBuilds))
	for i, index := range indexBuilds {
		names[i] = index.Name
	}

	return names, nil
}
