package catalog //nolint:testpackage // accesses unexported indexCatalogEntry to seed test data

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/mdb"
)

// makeCatalogWithIndexes builds an empty Catalog and seeds it with index entries
// so collectUnsuccessfulIndexes can be tested without a live MongoDB.
func makeCatalogWithIndexes(t *testing.T, entries map[string]map[string][]indexCatalogEntry) *Catalog {
	t.Helper()

	c := &Catalog{
		Databases: map[string]databaseCatalog{},
	}

	for db, colls := range entries {
		dbCat := databaseCatalog{Collections: map[string]collectionCatalog{}}

		for coll, idxs := range colls {
			dbCat.Collections[coll] = collectionCatalog{
				Indexes: idxs,
			}
		}

		c.Databases[db] = dbCat
	}

	return c
}

func mustMarshalKeys(t *testing.T, d bson.D) bson.Raw {
	t.Helper()

	raw, err := bson.Marshal(d)
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}

	return raw
}

func newIndexEntry(name string, keys bson.Raw, failed, incomplete, inconsistent bool) indexCatalogEntry {
	return indexCatalogEntry{
		IndexSpecification: &mdb.IndexSpecification{
			Name:         name,
			KeysDocument: keys,
		},
		Failed:       failed,
		Incomplete:   incomplete,
		Inconsistent: inconsistent,
	}
}

func TestCatalog_collectUnsuccessfulIndexes_Empty(t *testing.T) {
	t.Parallel()

	c := makeCatalogWithIndexes(t, nil)
	got := c.collectUnsuccessfulIndexes()

	assert.Empty(t, got, "empty catalog must yield no unsuccessful indexes")
}

func TestCatalog_collectUnsuccessfulIndexes_OnlySuccessful(t *testing.T) {
	t.Parallel()

	keys := mustMarshalKeys(t, bson.D{{"x", 1}})

	c := makeCatalogWithIndexes(t, map[string]map[string][]indexCatalogEntry{
		"db": {
			"coll": []indexCatalogEntry{
				newIndexEntry("ok_idx", keys, false, false, false),
			},
		},
	})

	got := c.collectUnsuccessfulIndexes()
	assert.Empty(t, got, "successful indexes must not appear")
}

func TestCatalog_collectUnsuccessfulIndexes_AllTypes(t *testing.T) {
	t.Parallel()

	keysFailed := mustMarshalKeys(t, bson.D{{"email", 1}})
	keysIncomplete := mustMarshalKeys(t, bson.D{{"name", 1}})
	keysInconsistent := mustMarshalKeys(t, bson.D{{"sku", 1}})

	c := makeCatalogWithIndexes(t, map[string]map[string][]indexCatalogEntry{
		"mydb": {
			"users": []indexCatalogEntry{
				newIndexEntry("good_idx", nil, false, false, false),
				newIndexEntry("email_unique_idx", keysFailed, true, false, false),
			},
			"orders": []indexCatalogEntry{
				newIndexEntry("name_idx", keysIncomplete, false, true, false),
			},
			"products": []indexCatalogEntry{
				newIndexEntry("sku_idx", keysInconsistent, false, false, true),
			},
		},
	})

	got := c.collectUnsuccessfulIndexes()
	assert.Len(t, got, 3)

	// Build a map keyed by index name for assertion regardless of map iteration order.
	byName := map[string]UnsuccessfulIndex{}
	for _, idx := range got {
		byName[idx.Name] = idx
	}

	assert.Equal(t, UnsuccessfulIndex{
		Namespace: "mydb.users",
		Name:      "email_unique_idx",
		Keys:      keysFailed,
		Type:      IndexFailed,
	}, byName["email_unique_idx"])

	assert.Equal(t, UnsuccessfulIndex{
		Namespace: "mydb.orders",
		Name:      "name_idx",
		Keys:      keysIncomplete,
		Type:      IndexIncomplete,
	}, byName["name_idx"])

	assert.Equal(t, UnsuccessfulIndex{
		Namespace: "mydb.products",
		Name:      "sku_idx",
		Keys:      keysInconsistent,
		Type:      IndexInconsistent,
	}, byName["sku_idx"])
}

// Per the agreed model, an index has at most one of Failed/Incomplete/Inconsistent.
// If multiple flags ever get set due to a bug, collectUnsuccessfulIndexes prefers
// Failed, then Incomplete, then Inconsistent (fail-loud over silently dropping).
func TestCatalog_collectUnsuccessfulIndexes_TypePriority(t *testing.T) {
	t.Parallel()

	keys := mustMarshalKeys(t, bson.D{{"x", 1}})

	c := makeCatalogWithIndexes(t, map[string]map[string][]indexCatalogEntry{
		"db": {
			"coll": []indexCatalogEntry{
				// All three flags set; should be reported once with Failed.
				newIndexEntry("triple_flag", keys, true, true, true),
			},
		},
	})

	got := c.collectUnsuccessfulIndexes()
	assert.Len(t, got, 1)
	assert.Equal(t, IndexFailed, got[0].Type)
}
