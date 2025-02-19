package tests

import (
	"context"
	"math/rand"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona-lab/percona-mongolink/topo"
)

var seed int64 = time.Now().Unix()

func BenchmarkInsertOne(b *testing.B) {
	mongodbURI := os.Getenv("TEST_TARGET_URI")
	if mongodbURI == "" {
		b.Fatal("no MongoDB URI provided")
	}

	client, err := topo.Connect(context.Background(), mongodbURI)
	if err != nil {
		b.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(context.Background()) //nolint:errcheck

	collection := client.Database("db_0").Collection("coll_0")
	collection.Drop(context.Background()) //nolint:errcheck

	payload := make([]byte, 1024*1024)
	rnd := rand.New(rand.NewSource(seed)) //nolint:gosec
	rnd.Read(payload)

	doc := map[string]any{
		"_id":  bson.NewObjectID(),
		"data": payload,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := collection.InsertOne(context.Background(), doc)
		if err != nil {
			if mongo.IsDuplicateKeyError(err) {
				continue
			}
			b.Fatalf("Failed to insert document: %v", err)
		}
	}
}

func BenchmarkReplaceOne(b *testing.B) {
	mongodbURI := os.Getenv("TEST_TARGET_URI")
	if mongodbURI == "" {
		b.Fatal("no MongoDB URI provided")
	}

	client, err := topo.Connect(context.Background(), mongodbURI)
	if err != nil {
		b.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(context.Background()) //nolint:errcheck

	collection := client.Database("db_0").Collection("coll_0")
	collection.Drop(context.Background()) //nolint:errcheck

	payload := make([]byte, 1024*1024)
	rnd := rand.New(rand.NewSource(seed)) //nolint:gosec
	rnd.Read(payload)

	id := bson.NewObjectID()
	doc := map[string]any{
		"_id":  id,
		"data": payload,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := collection.ReplaceOne(context.Background(), bson.D{{"_id", id}}, doc,
			options.Replace().SetUpsert(true))
		if err != nil && !mongo.IsDuplicateKeyError(err) {
			b.Fatalf("Failed to insert document: %v", err)
		}
	}
}
