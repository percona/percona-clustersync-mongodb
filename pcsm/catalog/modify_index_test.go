//go:build integration

package catalog

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/mdb"
)

func mustMarshal(t *testing.T, v any) bson.Raw {
	t.Helper()

	raw, err := bson.Marshal(v)
	require.NoError(t, err)

	return raw
}

func seedIndex(t *testing.T, cat *Catalog, db, coll string, spec *mdb.IndexSpecification) {
	t.Helper()

	dbCat, ok := cat.Databases[db]
	if !ok {
		dbCat = databaseCatalog{Collections: make(map[string]collectionCatalog)}
		cat.Databases[db] = dbCat
	}

	collCat := dbCat.Collections[coll]
	collCat.Indexes = append(collCat.Indexes, indexCatalogEntry{IndexSpecification: spec})
	dbCat.Collections[coll] = collCat
}

func TestDropAndRecreateIndex(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := connectToMongoDB(t)
	defer func() { _ = client.Disconnect(ctx) }()

	db := testDB + "_drop_recreate"
	coll := "test_ttl"
	indexName := "created_at_ttl"

	defer func() { _ = client.Database(db).Drop(ctx) }()

	ttlSeconds := int64(3600)
	spec := &mdb.IndexSpecification{
		Name:               indexName,
		KeysDocument:       mustMarshal(t, bson.D{{"created_at", 1}}),
		Version:            2,
		ExpireAfterSeconds: &ttlSeconds,
	}

	_, err := client.Database(db).Collection(coll).InsertOne(ctx, bson.D{{"created_at", time.Now()}})
	require.NoError(t, err)

	err = client.Database(db).RunCommand(ctx, bson.D{
		{"createIndexes", coll},
		{"indexes", bson.A{spec}},
	}).Err()
	require.NoError(t, err)

	cat := NewCatalog(client, mdb.ServerVersion{})
	seedIndex(t, cat, db, coll, spec)

	t.Run("drops and recreates index from catalog spec", func(t *testing.T) {
		err := cat.dropAndRecreateIndex(ctx, db, coll, indexName)
		require.NoError(t, err)

		cursor, err := client.Database(db).Collection(coll).Indexes().List(ctx)
		require.NoError(t, err)

		var indexes []bson.M
		require.NoError(t, cursor.All(ctx, &indexes))

		found := false
		for _, idx := range indexes {
			if idx["name"] == indexName {
				found = true
				assert.EqualValues(t, ttlSeconds, idx["expireAfterSeconds"])
			}
		}

		assert.True(t, found, "index %q should exist after drop-and-recreate", indexName)
	})

	t.Run("returns error when spec not in catalog", func(t *testing.T) {
		err := cat.dropAndRecreateIndex(ctx, db, coll, "nonexistent_index")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "spec is nil")
	})
}

func TestDoModifyIndexOption(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := connectToMongoDB(t)
	defer func() { _ = client.Disconnect(ctx) }()

	db := testDB + "_modify_idx"
	coll := "test_modify"
	indexName := "modified_at_ttl"

	defer func() { _ = client.Database(db).Drop(ctx) }()

	ttlSeconds := int64(7200)
	spec := &mdb.IndexSpecification{
		Name:               indexName,
		KeysDocument:       mustMarshal(t, bson.D{{"modified_at", 1}}),
		Version:            2,
		ExpireAfterSeconds: &ttlSeconds,
	}

	_, err := client.Database(db).Collection(coll).InsertOne(ctx, bson.D{{"modified_at", time.Now()}})
	require.NoError(t, err)

	err = client.Database(db).RunCommand(ctx, bson.D{
		{"createIndexes", coll},
		{"indexes", bson.A{spec}},
	}).Err()
	require.NoError(t, err)

	cat := NewCatalog(client, mdb.ServerVersion{})
	seedIndex(t, cat, db, coll, spec)

	t.Run("modifies expireAfterSeconds via collMod", func(t *testing.T) {
		newTTL := int64(1800)
		err := cat.doModifyIndexOption(ctx, db, coll, indexName, "expireAfterSeconds", newTTL)
		require.NoError(t, err)

		cursor, err := client.Database(db).Collection(coll).Indexes().List(ctx)
		require.NoError(t, err)

		var indexes []bson.M
		require.NoError(t, cursor.All(ctx, &indexes))

		for _, idx := range indexes {
			if idx["name"] == indexName {
				assert.EqualValues(t, newTTL, idx["expireAfterSeconds"])
			}
		}
	})

	t.Run("modifies hidden via collMod", func(t *testing.T) {
		err := cat.doModifyIndexOption(ctx, db, coll, indexName, "hidden", true)
		require.NoError(t, err)

		cursor, err := client.Database(db).Collection(coll).Indexes().List(ctx)
		require.NoError(t, err)

		var indexes []bson.M
		require.NoError(t, cursor.All(ctx, &indexes))

		for _, idx := range indexes {
			if idx["name"] == indexName {
				assert.Equal(t, true, idx["hidden"])
			}
		}
	})

	t.Run("converts non-unique to unique via prepareUnique", func(t *testing.T) {
		unique := true
		uniqueSpec := &mdb.IndexSpecification{
			Name:         indexName,
			KeysDocument: mustMarshal(t, bson.D{{"modified_at", 1}}),
			Version:      2,
			Unique:       &unique,
		}
		seedIndex(t, cat, db, coll, uniqueSpec)

		// Two-step conversion matching production flow (catalog.go:934-953):
		// MongoDB requires prepareUnique=true before unique=true.
		err := cat.doModifyIndexOption(ctx, db, coll, indexName, "prepareUnique", true)
		require.NoError(t, err)

		err = cat.doModifyIndexOption(ctx, db, coll, indexName, "unique", true)
		require.NoError(t, err)

		cursor, err := client.Database(db).Collection(coll).Indexes().List(ctx)
		require.NoError(t, err)

		var indexes []bson.M
		require.NoError(t, cursor.All(ctx, &indexes))

		found := false
		for _, idx := range indexes {
			if idx["name"] == indexName {
				found = true
				assert.Equal(t, true, idx["unique"])
			}
		}
		assert.True(t, found, "index %q should exist after unique modification", indexName)
	})
}
