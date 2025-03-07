package topo

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona-lab/percona-mongolink/errors"
)

type CollectionSpecification mongo.CollectionSpecification

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

// GetCollectionSpec retrieves the specification of a collection.
func GetCollectionSpec(
	ctx context.Context,
	m *mongo.Client,
	dbName string,
	collName string,
) (*CollectionSpecification, error) {
	colls, err := m.Database(dbName).ListCollectionSpecifications(ctx, bson.D{{"name", collName}})
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	if len(colls) == 0 {
		return nil, ErrNotFound
	}

	coll := CollectionSpecification(colls[0]) // copy to release the slice memory

	return &coll, err //nolint:wrapcheck
}

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

	return indexes, errors.Wrap(err, "decode indexes")
}
