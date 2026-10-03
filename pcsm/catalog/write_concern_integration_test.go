//go:build integration

package catalog

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/percona/percona-clustersync-mongodb/mdb"
)

func TestCatalogRawDDLWriteConcern(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	commands := make(chan bson.Raw, 20)
	client, err := mongo.Connect(options.Client().ApplyURI(targetMongoURI).
		SetWriteConcern(writeconcern.W1()).
		SetMonitor(&event.CommandMonitor{
			Started: func(_ context.Context, e *event.CommandStartedEvent) {
				switch e.CommandName {
				case "create", "createIndexes", "collMod", "renameCollection", "dropIndexes":
					commands <- append(bson.Raw(nil), e.Command...)
				}
			},
		}))
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Disconnect(ctx)) }()
	db := "pcsm311_catalog_" + bson.NewObjectID().Hex()
	defer func() { require.NoError(t, client.Database(db).Drop(ctx)) }()
	cat := NewCatalog(client, client, mdb.ServerVersion{})
	require.NoError(t, cat.CreateCollection(ctx, db, "data", &CreateCollectionOptions{}))
	require.NoError(t, cat.CreateCollection(ctx, db, "view",
		&CreateCollectionOptions{ViewOn: "data", Pipeline: bson.A{}}))
	keys, err := bson.Marshal(bson.D{{"value", 1}})
	require.NoError(t, err)
	require.NoError(t, cat.CreateIndexes(ctx, db, "data",
		[]*mdb.IndexSpecification{{Name: "value_1", KeysDocument: keys, Version: 2}}))
	validationLevel := "moderate"
	require.NoError(t, cat.ModifyValidation(ctx, db, "data", nil, &validationLevel, nil))
	require.NoError(t, cat.ModifyView(ctx, db, "view", "data", bson.A{}))
	require.NoError(t, cat.doModifyIndexOption(ctx, db, "data", "value_1", "hidden", true))
	require.NoError(t, cat.dropAndRecreateIndex(ctx, db, "data", "value_1"))
	finalizeKeys, err := bson.Marshal(bson.D{{"finalized", 1}})
	require.NoError(t, err)
	seedFailedIndex(cat, db, "data",
		&mdb.IndexSpecification{Name: "finalize_1", KeysDocument: finalizeKeys, Version: 2})
	require.Empty(t, cat.finalizeUnsuccessfulIndexes(ctx))
	require.NoError(t, cat.Rename(ctx, db, "data", db, "renamed"))

	count := 0
	for {
		select {
		case command := <-commands:
			count++
			w, ok := command.Lookup("writeConcern", "w").StringValueOK()
			require.True(t, ok, "catalog DDL must explicitly include string writeConcern.w")
			assert.Equal(t, "majority", w)
		default:
			require.GreaterOrEqual(t, count, 10)

			return
		}
	}
}
