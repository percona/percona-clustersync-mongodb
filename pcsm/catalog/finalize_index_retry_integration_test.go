//go:build integration

package catalog

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/mdb"
)

func TestFinalizeUnsuccessfulIndexes_ReportsAttemptedSourceKeysOnRecreateFailure(t *testing.T) {
	ctx := t.Context()
	source := connectToMongoDB(t)
	defer func() { _ = source.Disconnect(ctx) }()
	target := connectToTargetMongoDB(t)
	defer func() { _ = target.Disconnect(ctx) }()

	db := testDB + "_finalize_recreate_error"
	coll := "users"
	indexName := "current_source_keys"
	defer func() { _ = source.Database(db).Drop(ctx) }()
	defer func() { _ = target.Database(db).Drop(ctx) }()

	_, err := source.Database(db).Collection(coll).InsertOne(ctx, bson.D{{"current", 1}})
	require.NoError(t, err)
	_, err = source.Database(db).Collection(coll).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{"current", 1}},
		Options: options.Index().SetName(indexName),
	})
	require.NoError(t, err)
	_, err = target.Database(db).Collection(coll).InsertOne(ctx, bson.D{{"blocker", 1}})
	require.NoError(t, err)
	_, err = target.Database(db).Collection(coll).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{"blocker", 1}},
		Options: options.Index().SetName(indexName),
	})
	require.NoError(t, err)

	cat := NewCatalog(source, target, mdb.ServerVersion{})
	seedUnsuccessfulIndex(cat, db, coll, indexCatalogEntry{
		IndexSpecification: &mdb.IndexSpecification{
			Name:         indexName,
			KeysDocument: mustMarshal(t, bson.D{{"legacy", 1}}),
			Version:      2,
		},
		Incomplete: true,
	})

	report := cat.finalizeUnsuccessfulIndexes(ctx)

	require.Len(t, report, 1)
	require.Equal(t, IndexIncomplete, report[0].Type)
	require.Equal(t, mustMarshal(t, bson.D{{"current", 1}}), report[0].Keys)
}

func TestFinalizeUnsuccessfulIndexes_FailedIndexRetriesStoredSpecWithoutSourceProbe(t *testing.T) {
	ctx := t.Context()
	source := connectToMongoDB(t)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = source.Disconnect(cleanupCtx)
	})
	target := connectToTargetMongoDB(t)
	defer func() { _ = target.Disconnect(ctx) }()

	db := testDB + "_finalize_failed_no_probe"
	coll := "users"
	indexName := "failed_stored_spec"
	defer func() { _ = target.Database(db).Drop(ctx) }()

	_, err := target.Database(db).Collection(coll).InsertOne(ctx, bson.D{{"email", "a@example.com"}})
	require.NoError(t, err)

	stored := &mdb.IndexSpecification{
		Name:                    indexName,
		KeysDocument:            mustMarshal(t, bson.D{{"email", 1}}),
		Version:                 2,
		PartialFilterExpression: bson.D{{"stored", true}},
	}
	cat := NewCatalog(source, target, mdb.ServerVersion{})
	seedFailedIndex(cat, db, coll, stored)

	require.NoError(t, source.Database("admin").RunCommand(ctx, bson.D{
		{"configureFailPoint", "failCommand"},
		{"mode", "alwaysOn"},
		{"data", bson.D{{"failCommands", bson.A{"aggregate"}}, {"errorCode", 2}}},
	}).Err())
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = source.Database("admin").RunCommand(cleanupCtx, bson.D{
			{"configureFailPoint", "failCommand"},
			{"mode", "off"},
		}).Err()
	})

	report := cat.finalizeUnsuccessfulIndexes(ctx)

	require.Empty(t, report)
	targetIndex := listedIndexByName(t, ctx, target, db, coll, indexName)
	assertPartialFilter(t, targetIndex, "stored")
	require.False(t, catalogIndexByName(t, cat, db, coll, indexName).Unsuccessful())
}
