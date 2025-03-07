// go test -run="^$" -bench=. -benchmem -benchtime=30s ./...
package perf_test

import (
	"context"
	"math/rand"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona-lab/percona-mongolink/config"
	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/topo"
)

var seed = time.Now().Unix() //nolint:gochecknoglobals

// BenchmarkInsertOne benchmarks the performance of inserting a single document into MongoDB.
func BenchmarkInsertOne(b *testing.B) {
	mongodbURI := os.Getenv("TEST_TARGET_URI")
	if mongodbURI == "" {
		b.Fatal("no MongoDB URI provided")
	}

	client, err := topo.Connect(b.Context(), mongodbURI)
	if err != nil {
		b.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(b.Context()) //nolint:errcheck

	collection := client.Database("db_0").Collection("coll_0")
	collection.Drop(b.Context()) //nolint:errcheck

	payload := make([]byte, config.KiB)
	rnd := rand.New(rand.NewSource(seed)) //nolint:gosec
	rnd.Read(payload)

	id := bson.NewObjectID()
	doc := map[string]any{
		"_id":  id,
		"data": payload,
	}

	b.ResetTimer()

	for b.Loop() {
		_, err := collection.InsertOne(b.Context(), doc)
		if err != nil && !mongo.IsDuplicateKeyError(err) {
			b.Fatalf("Failed to insert document: %v", err)
		}
	}
}

// BenchmarkReplaceOne benchmarks the performance of replacing a single document in MongoDB.
func BenchmarkReplaceOne(b *testing.B) {
	mongodbURI := os.Getenv("TEST_TARGET_URI")
	if mongodbURI == "" {
		b.Fatal("no MongoDB URI provided")
	}

	client, err := topo.Connect(b.Context(), mongodbURI)
	if err != nil {
		b.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(b.Context()) //nolint:errcheck

	collection := client.Database("db_0").Collection("coll_0")
	collection.Drop(b.Context()) //nolint:errcheck

	payload := make([]byte, config.KiB)
	rnd := rand.New(rand.NewSource(seed)) //nolint:gosec
	rnd.Read(payload)

	id := bson.NewObjectID()
	key := bson.D{{"_id", id}}
	doc := map[string]any{
		"_id":  id,
		"data": payload,
	}

	b.ResetTimer()

	for b.Loop() {
		_, err := collection.ReplaceOne(b.Context(), key, doc, options.Replace().SetUpsert(true))
		if err != nil && !mongo.IsDuplicateKeyError(err) {
			b.Fatalf("Failed to insert document: %v", err)
		}
	}
}

// Benchmark_NoIndex benchmarks the performance of copying documents without any index.
func Benchmark_NoIndex(b *testing.B) {
	performIndexTest(b, performIndexTestOptions{NoIndex: true})
}

// Benchmark_UniquePostBuild benchmarks the performance of copying documents with a unique index
// created post-build.
func Benchmark_UniquePostBuild(b *testing.B) {
	performIndexTest(b, performIndexTestOptions{PostBuild: true})
}

// Benchmark_UniquePrepared benchmarks the performance of copying documents with a prepared unique
// index.
func Benchmark_UniquePrepared(b *testing.B) {
	performIndexTest(b, performIndexTestOptions{Prepared: true})
}

// Benchmark_UniquePreBuild benchmarks the performance of copying documents with a unique index
// created pre-build.
func Benchmark_UniquePreBuild(b *testing.B) {
	performIndexTest(b, performIndexTestOptions{PreBuild: true})
}

type performIndexTestOptions struct {
	NoIndex   bool
	PreBuild  bool
	Prepared  bool
	PostBuild bool
}

// performIndexTest performs the index test based on the provided options.
func performIndexTest(b *testing.B, opts performIndexTestOptions) {
	b.Helper()

	sourceURI := os.Getenv("TEST_SOURCE_URI")
	if sourceURI == "" {
		b.Fatal("no Source MongoDB URI provided")
	}

	targetURI := os.Getenv("TEST_TARGET_URI")
	if targetURI == "" {
		b.Fatal("no Target MongoDB URI provided")
	}

	ctx := b.Context()

	source, err := topo.Connect(ctx, sourceURI)
	if err != nil {
		b.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer source.Disconnect(ctx) //nolint:errcheck

	target, err := topo.Connect(ctx, targetURI)
	if err != nil {
		b.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer target.Disconnect(ctx) //nolint:errcheck

	b.ResetTimer()

	for b.Loop() {
		b.StopTimer()

		collection := target.Database("db_0").Collection("coll_0")

		err = collection.Drop(ctx)
		if err != nil {
			b.Fatalf("Failed to drop collection: %v", err)
		}

		b.StartTimer()

		if !opts.NoIndex {
			res := target.Database("db_0").RunCommand(ctx, bson.D{
				{"createIndexes", "coll_0"},
				{"indexes", bson.A{
					bson.D{
						{"key", bson.D{{"n", 1}}},
						{"name", "n_1"},
						{"prepareUnique", opts.Prepared},
						{"unique", opts.PreBuild},
					},
				}},
			})
			if err := res.Err(); err != nil {
				b.Fatalf("Failed to create index: %v", err)
			}
		}

		totalSize, err := copyDocuments(b, source, target, "db_0", "coll_0")
		if err != nil {
			b.Fatalf("Failed to copy documents: %v", err)
		}

		switch {
		case opts.NoIndex:

		case opts.PostBuild:
			res := target.Database("db_0").RunCommand(ctx, bson.D{
				{"collMod", "coll_0"},
				{"index", bson.D{{"name", "n_1"}, {"prepareUnique", true}}},
			})
			if err := res.Err(); err != nil {
				b.Fatalf("Failed to prepareUnique: %v", err)
			}

			fallthrough

		case opts.Prepared:
			res := target.Database("db_0").RunCommand(ctx, bson.D{
				{"collMod", "coll_0"},
				{"index", bson.D{{"name", "n_1"}, {"unique", true}}},
			})
			if err := res.Err(); err != nil {
				b.Fatalf("Failed to prepareUnique: %v", err)
			}
		}

		b.SetBytes(totalSize)
	}
}

// copyDocuments copies documents from the source to the target MongoDB collection.
func copyDocuments(b *testing.B, source, target *mongo.Client, db, coll string) (int64, error) {
	b.Helper()

	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()

	collStats, err := topo.GetCollStats(ctx, source, db, coll)
	if err != nil {
		return 0, errors.Wrap(err, "collStats")
	}

	if collStats.Count == 0 {
		// b.Log("empty collection")
		return 0, nil
	}

	averageDocumentSize := int(collStats.AvgObjSize)
	maxWriteSize := config.CloneMaxWriteSizePerCollection
	maxWriteCount := maxWriteSize / averageDocumentSize
	maxReadCount := maxWriteCount

	totalCopiedBytes := int64(0)
	documentC := make(chan bson.Raw, maxWriteCount)
	doneSig := make(chan error)

	go func() {
		defer close(doneSig)

		// b.Logf("avg_doc_size=%d max_read_count=%d max_write_size=%d max_write_count=%d",
		// 	averageDocumentSize, maxReadCount, maxWriteSize, maxWriteCount)

		targetCollection := target.Database(db).Collection(coll)
		insertOptions := options.InsertMany().SetOrdered(false)

		batch := make([]any, 0, maxReadCount)
		batchSize := 0

		for doc := range documentC {
			docSize := len(doc)
			if len(batch)+1 > cap(batch) || batchSize+docSize > maxWriteSize {
				_, err := targetCollection.InsertMany(ctx, batch, insertOptions)
				if err != nil {
					doneSig <- err

					return
				}

				// b.Logf("inserted_count=%d inserted_size=%d", len(batch), batchSize)

				totalCopiedBytes += int64(batchSize)

				batch = append(batch[:0], doc)
				batchSize = docSize

				continue
			}

			batch = append(batch, doc)
			batchSize += docSize
		}

		if len(batch) != 0 {
			_, err := targetCollection.InsertMany(ctx, batch, insertOptions)
			if err != nil {
				doneSig <- err

				return
			}

			// b.Logf("inserted_count=%d inserted_size=%d", len(batch), batchSize)

			totalCopiedBytes += int64(batchSize)
		}
	}()

	cur, err := source.Database(db).Collection(coll).
		Find(ctx, bson.D{}, options.Find().SetBatchSize(int32(maxReadCount))) //nolint:gosec
	if err != nil {
		return totalCopiedBytes, errors.Wrap(err, "open cursor")
	}
	defer cur.Close(ctx)

	for cur.Next(ctx) {
		documentC <- cur.Current
	}

	close(documentC)

	if err = cur.Err(); err != nil {
		return totalCopiedBytes, errors.Wrap(err, "cursor")
	}

	err = <-doneSig

	return totalCopiedBytes, errors.Wrap(err, "insert documents")
}
