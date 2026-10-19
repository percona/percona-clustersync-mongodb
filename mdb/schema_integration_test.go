//go:build integration

package mdb //nolint:testpackage // shares the TestMain fixture in connect_integration_test.go

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/util"
)

const (
	listTestTimeout    = 30 * time.Second
	listCleanupTimeout = 5 * time.Second
)

// filterRecorder captures the filter document of every listCollections and
// listDatabases command the client sends.
type filterRecorder struct {
	mu      sync.Mutex
	filters map[string][]bson.Raw
}

func newFilterRecorder() *filterRecorder {
	return &filterRecorder{filters: map[string][]bson.Raw{}}
}

func (r *filterRecorder) monitor() *event.CommandMonitor {
	return &event.CommandMonitor{
		Started: func(_ context.Context, evt *event.CommandStartedEvent) {
			if evt.CommandName != "listCollections" && evt.CommandName != "listDatabases" {
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()

			filter, err := evt.Command.LookupErr("filter")
			if err != nil {
				r.filters[evt.CommandName] = append(r.filters[evt.CommandName], nil)

				return
			}

			r.filters[evt.CommandName] = append(r.filters[evt.CommandName], filter.Document())
		},
	}
}

func (r *filterRecorder) requireNoServerSideFilter(t *testing.T, command string, minCalls int) {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()

	// At least, not exactly: a transient error retried by RunWithRetryVal adds
	// Started events while the helper still succeeds.
	require.GreaterOrEqual(t, len(r.filters[command]), minCalls, "%s calls", command)

	for i, filter := range r.filters[command] {
		require.NotNil(t, filter, "%s #%d must carry a filter field", command, i+1)

		elems, err := filter.Elements()
		require.NoError(t, err)
		assert.Empty(t, elems, "%s #%d must not filter server-side", command, i+1)
	}
}

func connectWithRecorder(t *testing.T) (*mongo.Client, *filterRecorder) {
	t.Helper()

	recorder := newFilterRecorder()

	client, err := mongo.Connect(options.Client().ApplyURI(integrationURI).SetMonitor(recorder.monitor()))
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = util.CtxWithTimeout(context.Background(), listCleanupTimeout, client.Disconnect)
	})

	return client, recorder
}

func dropOnCleanup(t *testing.T, db *mongo.Database) {
	t.Helper()

	t.Cleanup(func() {
		_ = util.CtxWithTimeout(context.Background(), listCleanupTimeout, db.Drop)
	})
}

// TestListCollections_ExcludeSystemCollectionsClientSide verifies PCSM-399:
// system collections are excluded after the server replies, so the
// listCollections command carries no filter. The Atlas proxy tiers reject any
// regex under $not, which a plain mongod accepts, so the wire-level assertion
// is what keeps that shape from coming back.
func TestListCollections_ExcludeSystemCollectionsClientSide(t *testing.T) {
	t.Parallel()

	client, recorder := connectWithRecorder(t)

	db := client.Database("pcsm399_list_collections")
	dropOnCleanup(t, db)

	require.NoError(t, util.CtxWithTimeout(t.Context(), listTestTimeout, func(ctx context.Context) error {
		require.NoError(t, db.Drop(ctx))
		require.NoError(t, db.CreateCollection(ctx, "alpha"))
		// A view materializes <db>.system.views, giving the helpers something to exclude.
		require.NoError(t, db.CreateView(ctx, "alpha_view", "alpha", mongo.Pipeline{}))

		names, err := ListCollectionNames(ctx, client, db.Name())
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"alpha", "alpha_view"}, names)

		specs, err := ListCollectionSpecs(ctx, client, db.Name())
		require.NoError(t, err)

		specNames := make([]string, 0, len(specs))
		for _, spec := range specs {
			specNames = append(specNames, spec.Name)
		}

		assert.ElementsMatch(t, []string{"alpha", "alpha_view"}, specNames)

		return nil
	}))

	recorder.requireNoServerSideFilter(t, "listCollections", 2)
}

// TestListDatabaseNames_ExcludeInternalClientSide verifies PCSM-399 for the
// database listing: admin, config, and local are dropped after the server
// replies, so listDatabases carries no filter. The Atlas proxy tiers accept
// only a string or regex for name and reject the $nin form.
func TestListDatabaseNames_ExcludeInternalClientSide(t *testing.T) {
	t.Parallel()

	client, recorder := connectWithRecorder(t)

	db := client.Database("pcsm399_list_databases")
	dropOnCleanup(t, db)

	require.NoError(t, util.CtxWithTimeout(t.Context(), listTestTimeout, func(ctx context.Context) error {
		require.NoError(t, db.CreateCollection(ctx, "alpha"))

		names, err := ListDatabaseNames(ctx, client)
		require.NoError(t, err)

		assert.Contains(t, names, db.Name())
		assert.NotContains(t, names, "admin")
		assert.NotContains(t, names, "config")
		assert.NotContains(t, names, "local")

		return nil
	}))

	recorder.requireNoServerSideFilter(t, "listDatabases", 1)
}
