//go:build integration

package clone_test

import (
	"context"
	"strings"
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
	"github.com/percona/percona-clustersync-mongodb/sel"
	"github.com/percona/percona-clustersync-mongodb/util"
)

func connectWithMonitor(t *testing.T, uri string, mon *event.CommandMonitor) *mongo.Client {
	t.Helper()

	client, err := mongo.Connect(options.Client().
		ApplyURI(uri).
		SetServerSelectionTimeout(5 * time.Second).
		SetRetryWrites(false).
		SetMonitor(mon))
	require.NoError(t, err)
	require.NoError(t, util.CtxWithTimeout(t.Context(), 5*time.Second, func(ctx context.Context) error {
		require.NoError(t, client.Ping(ctx, nil))

		return nil
	}))

	return client
}

func awaitClone(ctx context.Context, t *testing.T, done <-chan struct{}, msg string) {
	t.Helper()

	select {
	case <-done:
	case <-ctx.Done():
		require.FailNow(t, msg, ctx.Err().Error())
	}
}

func seedPadded(ctx context.Context, t *testing.T, coll *mongo.Collection, n, pad int) {
	t.Helper()

	docs := make([]any, 0, n)
	for i := range n {
		docs = append(docs, bson.D{{"_id", i}, {"pad", strings.Repeat("x", pad)}})
	}

	_, err := coll.InsertMany(ctx, docs)
	require.NoError(t, err)
}

func countDocs(ctx context.Context, t *testing.T, coll *mongo.Collection) int64 {
	t.Helper()

	n, err := coll.CountDocuments(ctx, bson.D{})
	require.NoError(t, err)

	return n
}

// sourceRenameHook renames db.from to db.to on the source while the second
// $collStats on from is about to reach the server: the first sizes the clone
// inventory, the second builds the copy segmenter. The collection is then gone
// under its old name after the clone created it on the target and before any
// document was read, which is where a rename at clone start lands.
type sourceRenameHook struct {
	db, from, to string
	renamer      *mongo.Client

	seen    atomic.Int32
	renamed chan error
}

func (h *sourceRenameHook) monitor() *event.CommandMonitor {
	return &event.CommandMonitor{
		Started: func(opCtx context.Context, cmd *event.CommandStartedEvent) {
			if cmd.DatabaseName != h.db || cmd.CommandName != "aggregate" {
				return
			}

			name, _ := cmd.Command.Lookup("aggregate").StringValueOK()
			if name != h.from || !strings.Contains(cmd.Command.Lookup("pipeline").String(), "$collStats") {
				return
			}

			if h.seen.Add(1) != 2 { //nolint:mnd // the segmenter's $collStats follows the inventory's
				return
			}

			h.renamed <- h.renamer.Database("admin").RunCommand(opCtx, bson.D{
				{"renameCollection", h.db + "." + h.from},
				{"to", h.db + "." + h.to},
			}).Err()
		},
	}
}

// TestClone_RenameBeforeCopyFollowsNewName pins that a collection renamed
// after the clone created it on the target, but before its copy began, is the
// collection's own outcome: the clone follows the rename by UUID, drops the
// copy under the old name, copies the collection under its new name and goes
// on with the other collections instead of failing the attempt.
//
//nolint:paralleltest // drives the shared containers and renames with a command monitor
func TestClone_RenameBeforeCopyFollowsNewName(t *testing.T) {
	require.NoError(t, util.CtxWithTimeout(t.Context(), 2*time.Minute, func(ctx context.Context) error {
		dbName := "pcsm388_" + bson.NewObjectID().Hex()

		renamer := connect(t, sourceURI)
		defer func() { _ = renamer.Disconnect(ctx) }()
		defer func() { _ = renamer.Database(dbName).Drop(ctx) }()

		hook := &sourceRenameHook{
			db: dbName, from: "src", to: "renamed",
			renamer: renamer,
			renamed: make(chan error, 1),
		}
		source := connectWithMonitor(t, sourceURI, hook.monitor())
		defer func() { _ = source.Disconnect(ctx) }()

		target := connect(t, targetURI)
		defer func() { _ = target.Disconnect(ctx) }()
		defer func() { _ = target.Database(dbName).Drop(ctx) }()

		// Given: src is larger than other, so Parallelism 1 copies it first.
		srcDB := renamer.Database(dbName)
		seedPadded(ctx, t, srcDB.Collection("src"), 200, 1024)
		seedPadded(ctx, t, srcDB.Collection("other"), 50, 256)

		sourceVer, err := mdb.Version(ctx, source)
		require.NoError(t, err)
		c := clone.NewClone(source, target, catalog.NewCatalog(source, target, sourceVer),
			sel.MakeFilter([]string{dbName + ".*"}, nil), &clone.Options{Parallelism: 1}, false)

		// When: src is renamed between its creation on the target and its copy.
		require.NoError(t, c.Start(ctx))
		awaitClone(ctx, t, c.Done(), "clone did not finish")

		select {
		case err := <-hook.renamed:
			require.NoError(t, err, "rename on the source")
		default:
			require.FailNow(t, "the segmenter's $collStats on src was never issued")
		}

		// Then: the clone finished and holds both collections, src under its new
		// name only.
		status := c.Status()
		require.NoError(t, status.Err)
		require.True(t, status.IsFinished())

		tgtDB := target.Database(dbName)
		names, err := tgtDB.ListCollectionNames(ctx, bson.D{})
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"renamed", "other"}, names)
		require.Equal(t, int64(200), countDocs(ctx, t, tgtDB.Collection("renamed")))
		require.Equal(t, int64(50), countDocs(ctx, t, tgtDB.Collection("other")))

		return nil
	}))
}
