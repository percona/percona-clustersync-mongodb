package tests

import (
	"context"
	"os"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var theDocument = bson.M{
	"_id": 0,
	"val": "3456tgfbvwet6tyrhfbvaetyydhfvsjhzsteydcghjdghtsryu",
}

func BenchmarkInsertOne(b *testing.B) {
	mongodbURI := os.Getenv("TEST_TARGET_URI")
	if mongodbURI == "" {
		b.Fatal("no MongoDB URI provided")
	}

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongodbURI))
	if err != nil {
		b.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(context.Background()) //nolint:errcheck

	collection := client.Database("db_0").Collection("coll_0")
	collection.Drop(context.Background()) //nolint:errcheck

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := collection.InsertOne(context.Background(), theDocument)
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

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongodbURI))
	if err != nil {
		b.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(context.Background()) //nolint:errcheck

	collection := client.Database("db_0").Collection("coll_0")
	collection.Drop(context.Background()) //nolint:errcheck

	key := bson.M{"_id": theDocument["_id"]}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := collection.ReplaceOne(context.Background(),
			key,
			theDocument,
			options.Replace().SetUpsert(true))
		if err != nil && !mongo.IsDuplicateKeyError(err) {
			b.Fatalf("Failed to insert document: %v", err)
		}
	}
}
