//go:build integration

package catalog

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/mdb"
)

// seedFailedIndex appends a failed unsuccessful-index entry to the catalog.
// It mirrors seedIndex but marks the entry Failed so that
// finalizeUnsuccessfulIndexes takes the unconditional-recreate path (which
// skips the source recheck and only touches the target).
func seedFailedIndex(cat *Catalog, db, coll string, spec *mdb.IndexSpecification) {
	dbCat, ok := cat.Databases[db]
	if !ok {
		dbCat = databaseCatalog{Collections: make(map[string]collectionCatalog)}
		cat.Databases[db] = dbCat
	}

	collCat := dbCat.Collections[coll]
	collCat.Indexes = append(collCat.Indexes, indexCatalogEntry{IndexSpecification: spec, Failed: true})
	dbCat.Collections[coll] = collCat
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
