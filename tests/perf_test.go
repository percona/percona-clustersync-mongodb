package tests_test

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

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

	payload := make([]byte, 1024*1024)
	rnd := rand.New(rand.NewSource(seed)) //nolint:gosec
	rnd.Read(payload)

	doc := map[string]any{
		"_id":  bson.NewObjectID(),
		"data": payload,
	}

	b.ResetTimer()

	for range b.N {
		_, err := collection.InsertOne(b.Context(), doc)
		if err != nil {
			if mongo.IsDuplicateKeyError(err) {
				continue
			}

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

	payload := make([]byte, 1024*1024)
	rnd := rand.New(rand.NewSource(seed)) //nolint:gosec
	rnd.Read(payload)

	id := bson.NewObjectID()
	doc := map[string]any{
		"_id":  id,
		"data": payload,
	}

	b.ResetTimer()

	for range b.N {
		_, err := collection.ReplaceOne(b.Context(), bson.D{{"_id", id}}, doc,
			options.Replace().SetUpsert(true))
		if err != nil && !mongo.IsDuplicateKeyError(err) {
			b.Fatalf("Failed to insert document: %v", err)
		}
	}
}
