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
