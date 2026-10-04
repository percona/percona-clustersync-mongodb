//go:build integration

package clone_test

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
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/pcsm/clone"
	"github.com/percona/percona-clustersync-mongodb/sel"
	"github.com/percona/percona-clustersync-mongodb/util"
)

//nolint:paralleltest // Shares the package integration deployments.
func TestCloneTargetWriteConcernCommands(t *testing.T) {
	for _, tt := range []struct {
		name string
		wc   *writeconcern.WriteConcern
		want any
	}{
		{"default majority", nil, "majority"},
		{"explicit one", writeconcern.W1(), int32(1)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, util.CtxWithTimeout(t.Context(), 30*time.Second, func(ctx context.Context) error {
				source := connect(t, sourceURI)
				defer func() { require.NoError(t, source.Disconnect(ctx)) }()
				commands := make(chan bson.Raw, 10)
				target, err := mongo.Connect(options.Client().ApplyURI(targetURI).
					SetWriteConcern(writeconcern.W1()).
					SetMonitor(&event.CommandMonitor{
						Started: func(_ context.Context, e *event.CommandStartedEvent) {
							if e.CommandName == "insert" {
								commands <- append(bson.Raw(nil), e.Command...)
							}
						},
					}))
				require.NoError(t, err)
				defer func() { require.NoError(t, target.Disconnect(ctx)) }()
				db := "pcsm311_clone_" + bson.NewObjectID().Hex()
				defer func() { require.NoError(t, source.Database(db).Drop(ctx)) }()
				defer func() { require.NoError(t, target.Database(db).Drop(ctx)) }()
				_, err = source.Database(db).Collection("data").InsertMany(ctx,
					[]any{bson.D{{"_id", 1}}, bson.D{{"_id", 2}}})
				require.NoError(t, err)
				version, err := mdb.Version(ctx, source)
				require.NoError(t, err)
				cat := catalog.NewCatalog(source, target, version)
				cln := clone.NewClone(source, target, cat, sel.MakeFilter([]string{db + ".*"}, nil),
					&clone.Options{WriteConcern: tt.wc, ReadWorkers: 1, InsertWorkers: 1}, false)
				require.NoError(t, cln.Start(ctx))
				select {
				case <-cln.Done():
				case <-ctx.Done():
					require.FailNow(t, "clone did not complete", "%v", ctx.Err())
				}
				require.NoError(t, cln.Status().Err)
				count, err := target.Database(db).Collection("data").CountDocuments(ctx, bson.D{})
				require.NoError(t, err)
				require.Equal(t, int64(2), count)
				select {
				case command := <-commands:
					var concern struct {
						W any `bson:"w"`
					}
					require.NoError(t, bson.Unmarshal(command.Lookup("writeConcern").Document(), &concern))
					assert.Equal(t, tt.want, concern.W)
				default:
					require.FailNow(t, "clone emitted no insert command")
				}

				return nil
			}))
		})
	}
}
