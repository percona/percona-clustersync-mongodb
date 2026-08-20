//go:build integration

package catalog

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

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

func TestFinalizeUnsuccessfulIndexes_RecreatesCurrentSourceSpec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		originalType IndexUnsuccessfulType
	}{
		{name: "incomplete", originalType: IndexIncomplete},
		{name: "inconsistent", originalType: IndexInconsistent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			source := connectToMongoDB(t)
			defer func() { _ = source.Disconnect(ctx) }()
			target := connectToTargetMongoDB(t)
			defer func() { _ = target.Disconnect(ctx) }()

			db := testDB + "_finalize_live_" + tt.name
			coll := "users"
			indexName := "email_active"
			defer func() { _ = source.Database(db).Drop(ctx) }()
			defer func() { _ = target.Database(db).Drop(ctx) }()

			_, err := source.Database(db).Collection(coll).InsertOne(ctx, bson.D{{"email", "a@example.com"}, {"active", true}})
			require.NoError(t, err)
			_, err = source.Database(db).Collection(coll).Indexes().CreateOne(ctx, mongo.IndexModel{
				Keys: bson.D{{"email", 1}},
				Options: options.Index().
					SetName(indexName).
					SetPartialFilterExpression(bson.D{{"active", true}}),
			})
			require.NoError(t, err)
			_, err = target.Database(db).Collection(coll).InsertOne(ctx, bson.D{{"seed", true}})
			require.NoError(t, err)

			stored := &mdb.IndexSpecification{
				Name:                    indexName,
				KeysDocument:            mustMarshal(t, bson.D{{"email", 1}}),
				Version:                 2,
				PartialFilterExpression: bson.D{{"legacy", true}},
			}
			entry := indexCatalogEntry{IndexSpecification: stored}
			switch tt.originalType {
			case IndexIncomplete:
				entry.Incomplete = true
			case IndexInconsistent:
				entry.Inconsistent = true
			}
			cat := NewCatalog(source, target, mdb.ServerVersion{})
			seedUnsuccessfulIndex(cat, db, coll, entry)

			require.Nil(t, listedIndexByName(t, ctx, target, db, coll, indexName))

			report := cat.finalizeUnsuccessfulIndexes(ctx)

			require.Empty(t, report)
			assertPartialFilter(t, listedIndexByName(t, ctx, target, db, coll, indexName), "active")
			catalogEntry := catalogIndexByName(t, cat, db, coll, indexName)
			require.False(t, catalogEntry.Unsuccessful())
			assertPartialFilter(t, catalogEntry.IndexSpecification, "active")
		})
	}
}

func TestFinalizeUnsuccessfulIndexes_ReportsMissingSourceIndex(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	source := connectToMongoDB(t)
	defer func() { _ = source.Disconnect(ctx) }()
	target := connectToTargetMongoDB(t)
	defer func() { _ = target.Disconnect(ctx) }()

	db := testDB + "_finalize_missing"
	coll := "users"
	indexName := "missing_email"
	defer func() { _ = source.Database(db).Drop(ctx) }()
	defer func() { _ = target.Database(db).Drop(ctx) }()

	_, err := source.Database(db).Collection(coll).InsertOne(ctx, bson.D{{"seed", true}})
	require.NoError(t, err)
	_, err = target.Database(db).Collection(coll).InsertOne(ctx, bson.D{{"seed", true}})
	require.NoError(t, err)

	cat := NewCatalog(source, target, mdb.ServerVersion{})
	seedUnsuccessfulIndex(cat, db, coll, indexCatalogEntry{
		IndexSpecification: &mdb.IndexSpecification{
			Name:         indexName,
			KeysDocument: mustMarshal(t, bson.D{{"email", 1}}),
			Version:      2,
		},
		Inconsistent: true,
	})

	report := cat.finalizeUnsuccessfulIndexes(ctx)

	require.Equal(t, []UnsuccessfulIndex{{
		Namespace: db + "." + coll,
		Name:      indexName,
		Keys:      mustMarshal(t, bson.D{{"email", 1}}),
		Type:      IndexInconsistent,
		Reason:    finalizeReasonNoLongerPresent,
	}}, report)
	require.Nil(t, listedIndexByName(t, ctx, target, db, coll, indexName))
}

func TestFinalizeUnsuccessfulIndexes_ReportsDroppedSourceCollection(t *testing.T) {
	tests := []struct {
		name           string
		dbSuffix       string
		failIndexStats bool
	}{
		{name: "natural drop", dbSuffix: "natural"},
		{name: "index stats namespace not found", dbSuffix: "index_stats", failIndexStats: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			source := connectToMongoDB(t)
			t.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				_ = source.Disconnect(cleanupCtx)
			})
			target := connectToTargetMongoDB(t)
			defer func() { _ = target.Disconnect(ctx) }()

			db := testDB + "_finalize_dropped_source_" + tt.dbSuffix
			coll := "users"
			indexName := "dropped_email"
			defer func() { _ = source.Database(db).Drop(ctx) }()
			defer func() { _ = target.Database(db).Drop(ctx) }()

			_, err := source.Database(db).Collection(coll).InsertOne(ctx, bson.D{{"seed", true}})
			require.NoError(t, err)
			_, err = source.Database(db).Collection(coll).Indexes().CreateOne(ctx, mongo.IndexModel{
				Keys:    bson.D{{"email", 1}},
				Options: options.Index().SetName(indexName),
			})
			require.NoError(t, err)
			_, err = target.Database(db).Collection(coll).InsertOne(ctx, bson.D{{"seed", true}})
			require.NoError(t, err)

			keys := mustMarshal(t, bson.D{{"email", 1}})
			entry := indexCatalogEntry{
				IndexSpecification: &mdb.IndexSpecification{
					Name:         indexName,
					KeysDocument: keys,
					Version:      2,
				},
				Inconsistent: true,
			}
			cat := NewCatalog(source, target, mdb.ServerVersion{})
			seedUnsuccessfulIndex(cat, db, coll, entry)

			require.NoError(t, source.Database(db).Collection(coll).Drop(ctx))
			if tt.failIndexStats {
				require.NoError(t, source.Database("admin").RunCommand(ctx, bson.D{
					{"configureFailPoint", "failCommand"},
					{"mode", bson.D{{"skip", 1}}},
					{"data", bson.D{{"failCommands", bson.A{"aggregate"}}, {"errorCode", 26}}},
				}).Err())
				t.Cleanup(func() {
					cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()

					_ = source.Database("admin").RunCommand(cleanupCtx, bson.D{
						{"configureFailPoint", "failCommand"},
						{"mode", "off"},
					}).Err()
				})
			}

			report := cat.finalizeUnsuccessfulIndexes(ctx)

			require.Equal(t, []UnsuccessfulIndex{{
				Namespace: db + "." + coll,
				Name:      indexName,
				Keys:      keys,
				Type:      IndexInconsistent,
				Reason:    finalizeReasonNoLongerPresent,
			}}, report)
			require.Equal(t, entry, catalogIndexByName(t, cat, db, coll, indexName))

			targetDocuments, err := target.Database(db).Collection(coll).CountDocuments(ctx, bson.D{})
			require.NoError(t, err)
			require.EqualValues(t, 1, targetDocuments)

			targetIndexes, err := mdb.ListIndexes(ctx, target, db, coll)
			require.NoError(t, err)
			require.Len(t, targetIndexes, 1)
			require.Equal(t, "_id_", targetIndexes[0].Name)
			require.Nil(t, listedIndexByName(t, ctx, target, db, coll, indexName))
		})
	}
}

func TestFinalizeUnsuccessfulIndexes_ContinuesAfterSourceProbeFailure(t *testing.T) {
	ctx := t.Context()
	source := connectToMongoDB(t)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = source.Disconnect(cleanupCtx)
	})
	target := connectToTargetMongoDB(t)
	defer func() { _ = target.Disconnect(ctx) }()

	db := testDB + "_finalize_probe_error"
	coll := "users"
	defer func() { _ = source.Database(db).Drop(ctx) }()
	defer func() { _ = target.Database(db).Drop(ctx) }()

	_, err := source.Database(db).Collection(coll).InsertOne(ctx, bson.D{{"seed", true}})
	require.NoError(t, err)
	_, err = target.Database(db).Collection(coll).InsertOne(ctx, bson.D{{"seed", true}})
	require.NoError(t, err)

	cat := NewCatalog(source, target, mdb.ServerVersion{})
	probeIndex := &mdb.IndexSpecification{
		Name:         "source_probe_error",
		KeysDocument: mustMarshal(t, bson.D{{"probe", 1}}),
		Version:      2,
	}
	seedUnsuccessfulIndex(cat, db, coll, indexCatalogEntry{
		IndexSpecification: probeIndex,
		Incomplete:         true,
	})
	failedIndex := &mdb.IndexSpecification{
		Name:         "failed_retry",
		KeysDocument: mustMarshal(t, bson.D{{"retry", 1}}),
		Version:      2,
	}
	seedFailedIndex(cat, db, coll, failedIndex)

	require.NoError(t, source.Database("admin").RunCommand(ctx, bson.D{
		{"configureFailPoint", "failCommand"},
		{"mode", bson.D{{"times", 1}}},
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

	require.Len(t, report, 1)
	require.Equal(t, IndexIncomplete, report[0].Type)
	require.Equal(t, probeIndex.Name, report[0].Name)
	require.Contains(t, report[0].Reason, "list source in-progress index builds")
	require.NotNil(t, listedIndexByName(t, ctx, target, db, coll, failedIndex.Name))
}

// TestFinalizeUnsuccessfulIndexes_CheckpointRace exercises the data race
// between the recovery checkpoint path and finalize's catalog write.
//
// PCSM.Checkpoint holds the catalog read lock (Catalog.LockWrite) while it
// marshals the checkpoint, which traverses the live catalog map. Concurrently,
// finalizeUnsuccessfulIndexes writes recreated indexes back into that same map
// via addIndexesToCatalog. Before commit 413a5a9 that write was unsynchronized,
// so the two paths raced. The fix wraps the write in c.lock.Lock(); this test
// reproduces the interleaving and must be run under `go test -race`:
//
//	go test -race -tags integration \
//	    -run TestFinalizeUnsuccessfulIndexes_CheckpointRace ./pcsm/catalog
//
// It fails (race detected) against the pre-fix code and passes clean afterwards.
func TestFinalizeUnsuccessfulIndexes_CheckpointRace(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := connectToMongoDB(t)
	defer func() { _ = client.Disconnect(ctx) }()

	cat := NewCatalog(client, client, mdb.ServerVersion{})

	db := testDB + "_finalize_race"
	coll := "race_coll"
	defer func() { _ = client.Database(db).Drop(ctx) }()

	// Materialize the target collection so createIndexes has a namespace.
	_, err := client.Database(db).Collection(coll).InsertOne(ctx, bson.D{{"seed", time.Now()}})
	require.NoError(t, err)

	// Seed failed entries for indexes that do not yet exist on the collection,
	// so finalize actually creates each one on the target and then writes it
	// back into the catalog map (the write that races the checkpoint reader).
	const numIndexes = 30
	for i := range numIndexes {
		spec := &mdb.IndexSpecification{
			Name:         fmt.Sprintf("idx_%d", i),
			KeysDocument: mustMarshal(t, bson.D{{fmt.Sprintf("f%d", i), 1}}),
			Version:      2,
		}
		seedFailedIndex(cat, db, coll, spec)
	}

	// Reader goroutines mirror PCSM.Checkpoint: hold the read lock while
	// marshalling the checkpoint, which walks the live catalog map.
	stop := make(chan struct{})
	var wg sync.WaitGroup

	const numReaders = 4
	for range numReaders {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-stop:
					return
				default:
				}

				cat.LockWrite()
				cp := cat.Checkpoint()
				_, _ = bson.Marshal(cp)
				cat.UnlockWrite()
			}
		}()
	}

	// Writer: recreate the failed indexes on the target. Each successful
	// recreate writes the spec back into the catalog map via addIndexesToCatalog
	// under c.lock.Lock() (commit 413a5a9), serializing against the readers.
	report := cat.finalizeUnsuccessfulIndexes(ctx)

	close(stop)
	wg.Wait()

	// Every seeded index recreates cleanly, so finalize reports nothing.
	require.Empty(t, report, "expected no unsuccessful-index reports")

	// Confirm each seeded index now exists on the target collection.
	cursor, err := client.Database(db).Collection(coll).Indexes().List(ctx)
	require.NoError(t, err)

	var existing []bson.M
	require.NoError(t, cursor.All(ctx, &existing))

	names := make(map[string]bool, len(existing))
	for _, idx := range existing {
		if name, ok := idx["name"].(string); ok {
			names[name] = true
		}
	}

	for i := range numIndexes {
		require.True(t, names[fmt.Sprintf("idx_%d", i)], "index idx_%d missing on target", i)
	}
}

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
