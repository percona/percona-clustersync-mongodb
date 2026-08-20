//go:build integration

package catalog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-clustersync-mongodb/mdb"
)

// seedFailedIndex appends a failed unsuccessful-index entry to the catalog.
// It mirrors seedIndex but marks the entry Failed so that
// finalizeUnsuccessfulIndexes takes the unconditional-recreate path (which
// skips the source recheck and only touches the target).
func seedFailedIndex(cat *Catalog, db, coll string, spec *mdb.IndexSpecification) {
	seedUnsuccessfulIndex(cat, db, coll, indexCatalogEntry{IndexSpecification: spec, Failed: true})
}

func seedUnsuccessfulIndex(cat *Catalog, db, coll string, entry indexCatalogEntry) {
	dbCat, ok := cat.Databases[db]
	if !ok {
		dbCat = databaseCatalog{Collections: make(map[string]collectionCatalog)}
		cat.Databases[db] = dbCat
	}

	collCat := dbCat.Collections[coll]
	collCat.Indexes = append(collCat.Indexes, entry)
	dbCat.Collections[coll] = collCat
}

func listedIndexByName(
	t *testing.T,
	ctx context.Context,
	client *mongo.Client,
	db, coll, name string,
) *mdb.IndexSpecification {
	t.Helper()

	indexes, err := mdb.ListIndexes(ctx, client, db, coll)
	require.NoError(t, err)

	return findIndexByName(indexes, name)
}

func catalogIndexByName(t *testing.T, cat *Catalog, db, coll, name string) indexCatalogEntry {
	t.Helper()

	for _, index := range cat.Databases[db].Collections[coll].Indexes {
		if index.Name == name {
			return index
		}
	}

	t.Fatalf("index %q missing from catalog", name)

	return indexCatalogEntry{}
}

func assertPartialFilter(t *testing.T, spec *mdb.IndexSpecification, field string) {
	t.Helper()
	require.NotNil(t, spec)

	raw, err := bson.Marshal(spec.PartialFilterExpression)
	require.NoError(t, err)
	value := bson.Raw(raw).Lookup(field)
	require.Equal(t, bson.TypeBoolean, value.Type)
	require.True(t, value.Boolean())
}
