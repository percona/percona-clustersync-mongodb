package topo

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"golang.org/x/sync/errgroup"

	"github.com/percona-lab/percona-mongolink/errors"
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

	Rest map[string]any `bson:",inline"`
}

// IsClustered returns true if the index is clustered.
func (s *IndexSpecification) IsClustered() bool {
	return s.Clustered != nil && *s.Clustered
}

type catalogEntry struct {
	Database   string `bson:"db"`
	Collection string `bson:"name"`
	Type       string `bson:"type"`
	Metadata   struct {
		Options struct {
			UUID *bson.Binary `bson:"uuid"`

			Rest map[string]any `bson:",inline"`
		} `bson:"options"`
		Indexes []struct {
			Spec IndexSpecification `bson:"spec"`

			Rest map[string]any `bson:",inline"`
		} `bson:"indexes"`

		Rest map[string]any `bson:",inline"`
	} `bson:"md"`

	Rest map[string]any `bson:",inline"`
}

type NSSpec struct {
	Database string
	CollectionSpecification
	Indexes []IndexSpecification
}

// ListCatalog is an internal aggregation pipeline operator that may be used to inspect the contents
// of the durable catalog on a running server. (Since v7.0)
//
// https://github.com/mongodb/mongo/blob/v8.0/src/mongo/db/catalog/README.md#listcatalog-aggregation-pipeline-operator
func ListCatalog(ctx context.Context, mc *mongo.Client) ([]NSSpec, error) {
	cur, err := mc.Database("admin").Aggregate(ctx, mongo.Pipeline{
		bson.D{{"$listCatalog", bson.D{}}},
		// bson.D{{"$project", bson.D{{"db", 1}, {"name", 1}, {"type", 1}, {"md.options", 1}}}},
		bson.D{{"$match", bson.D{
			{"db", bson.D{{"$nin", bson.A{"admin", "config", "local"}}}},
			{"name", bson.D{{"$not", bson.D{{"$regex", "^system\\."}}}}},
		}}},
	})
	if err != nil {
		return nil, errors.Wrap(err, "$listCatalog")
	}

	var res []catalogEntry
	err = cur.All(ctx, &res)
	if err != nil {
		return nil, errors.Wrap(err, "all")
	}

	{
		s, err := bson.MarshalExtJSON(map[string][]catalogEntry{"a": res}, true, true)
		if err != nil {
			log.Println(err)
		} else {
			fmt.Println(string(s))
		}
	}

	rv := make([]NSSpec, len(res))

	for i, ent := range res {
		rv[i] = NSSpec{
			Database: ent.Database,
			CollectionSpecification: mongo.CollectionSpecification{
				Name:     ent.Collection,
				Type:     ent.Type,
				UUID:     ent.Metadata.Options.UUID,
				ReadOnly: false,
				Options:  nil,
			},
		}
	}

	return rv, nil
}

func ListCatalogSlow(ctx context.Context, mc *mongo.Client) ([]NSSpec, error) {
	databases, err := ListDatabaseNames(ctx, mc)
	if err != nil {
		return nil, errors.Wrap(err, "list databases")
	}

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.SetLimit(runtime.NumCPU() * 2)

	mu := sync.Mutex{}
	var rv []NSSpec

	for _, db := range databases {
		grp.Go(func() error {
			specs, err := ListCollectionSpecs(grpCtx, mc, db)
			if err != nil {
				return errors.Wrapf(err, "list collections for %q", db)
			}

			for i := range specs {
				mu.Lock()
				rv = append(rv, NSSpec{
					Database:                db,
					CollectionSpecification: specs[i],
				})
				mu.Unlock()
			}

			return nil
		})
	}

	err = grp.Wait()
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	return rv, nil
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
