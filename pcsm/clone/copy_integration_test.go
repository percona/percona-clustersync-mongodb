//go:build integration

package clone_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/pcsm/clone"
)

// TestCopyCountsProcessedDocuments checks the document count a copy reports
// when a batch is retried or finds documents already on the target. A batch
// that succeeds counts all its documents once, and a failed one counts none.
//
//nolint:paralleltest // Drives a server-global failpoint on the shared target.
func TestCopyCountsProcessedDocuments(t *testing.T) {
	const (
		numDocs = 10
		appName = "pcsm422"
	)

	for _, tt := range []struct {
		name string
		// seeded is how many of the batch's documents are on the target first.
		seeded int
		// wceCode is the write concern error the first insert returns; 0 for none.
		wceCode      int32
		wantErr      bool
		wantCount    int
		wantAttempts int32
	}{
		{"complete insert, then a transient write concern error", 0, 189, false, numDocs, 2},
		{"partial insert, then a transient write concern error", numDocs / 2, 189, false, numDocs, 2},
		{"documents already on the target", numDocs / 2, 0, false, numDocs, 1},
		{"non-transient write concern error", 0, 64, true, 0, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()

			source := connect(t, sourceURI)
			defer func() { require.NoError(t, source.Disconnect(ctx)) }()
			admin := connect(t, targetURI)
			defer func() { require.NoError(t, admin.Disconnect(ctx)) }()

			// The copy's target client counts insert attempts. The failpoint
			// matches its app name only, so seeding is not affected.
			var attempts atomic.Int32
			target, err := mongo.Connect(options.Client().ApplyURI(targetURI).
				SetAppName(appName).
				SetMonitor(&event.CommandMonitor{
					Started: func(_ context.Context, e *event.CommandStartedEvent) {
						if e.CommandName == "insert" {
							attempts.Add(1)
						}
					},
				}))
			require.NoError(t, err)
			defer func() { require.NoError(t, target.Disconnect(ctx)) }()

			ns := catalog.Namespace{Database: "pcsm422_" + bson.NewObjectID().Hex(), Collection: "data"}
			defer func() { require.NoError(t, source.Database(ns.Database).Drop(ctx)) }()
			defer func() { require.NoError(t, admin.Database(ns.Database).Drop(ctx)) }()

			docs := make([]any, numDocs)
			for i := range docs {
				docs[i] = bson.D{{"_id", i}}
			}
			_, err = source.Database(ns.Database).Collection(ns.Collection).InsertMany(ctx, docs)
			require.NoError(t, err)
			if tt.seeded > 0 {
				_, err = admin.Database(ns.Database).Collection(ns.Collection).InsertMany(ctx, docs[:tt.seeded])
				require.NoError(t, err)
			}

			if tt.wceCode != 0 {
				require.NoError(t, admin.Database("admin").RunCommand(ctx, bson.D{
					{"configureFailPoint", "failCommand"},
					{"mode", bson.D{{"times", 1}}},
					{"data", bson.D{
						{"failCommands", bson.A{"insert"}},
						{"appName", appName},
						{"writeConcernError", bson.D{{"code", tt.wceCode}, {"errmsg", "injected"}}},
						{"errorLabels", bson.A{}},
					}},
				}).Err())
				defer func() {
					require.NoError(t, admin.Database("admin").RunCommand(context.WithoutCancel(ctx), bson.D{
						{"configureFailPoint", "failCommand"}, {"mode", "off"},
					}).Err())
				}()
			}

			spec, err := mdb.GetCollectionSpec(ctx, source, ns.Database, ns.Collection)
			require.NoError(t, err)

			cm := clone.NewCopyManager(ctx, source, target,
				clone.CopyManagerOptions{NumReadWorkers: 1, NumInsertWorkers: 1})
			defer cm.Close()

			var count int
			var copyErr error
			for update := range cm.Start(ctx, ns, spec) {
				count += update.Count
				if update.Err != nil {
					copyErr = update.Err
				}
			}

			require.Equal(t, tt.wantAttempts, attempts.Load(), "insert attempts")
			require.Equal(t, tt.wantCount, count, "reported documents")
			if tt.wantErr {
				require.Error(t, copyErr)

				return
			}
			require.NoError(t, copyErr)

			onTarget, err := admin.Database(ns.Database).Collection(ns.Collection).CountDocuments(ctx, bson.D{})
			require.NoError(t, err)
			require.Equal(t, int64(numDocs), onTarget)
		})
	}
}
