//go:build integration

package main //nolint:testpackage // Drive a takeover through the real server controller.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/ha"
	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm"
)

func TestLifecycleCheckpointSurvivesTakeover(t *testing.T) {
	t.Parallel()

	sourceURI, source := checkpointFaultMongo(t)
	targetURI, target := checkpointFaultMongo(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	const db = "pcsm439"
	chosen := source.Database(db).Collection("chosen")
	decoy := source.Database(db).Collection("excluded")
	targetDecoy := target.Database(db).Collection("excluded")
	_, err := chosen.InsertOne(ctx, bson.D{{"_id", "cloned"}})
	require.NoError(t, err)
	_, err = decoy.InsertOne(ctx, bson.D{{"_id", "source-only"}})
	require.NoError(t, err)
	_, err = targetDecoy.InsertOne(ctx, bson.D{{"_id", "must-survive"}})
	require.NoError(t, err)

	sourceVer, err := mdb.Version(ctx, source)
	require.NoError(t, err)
	cfg := &config.Config{RecoveryCheckpointInterval: time.Hour}
	saved := make(chan struct{})
	released := make(chan struct{})
	release := sync.OnceFunc(func() { close(released) })
	defer release()
	var first atomic.Int64
	gated := connectLifecycleTestClient(t, targetURI, &event.CommandMonitor{
		Started: func(_ context.Context, cmd *event.CommandStartedEvent) {
			data := cmd.Command.Lookup("update", "$set", "data")
			if cmd.CommandName == "findAndModify" && !data.IsZero() {
				first.CompareAndSwap(0, cmd.RequestID)
			}
		},
		Succeeded: func(_ context.Context, cmd *event.CommandSucceededEvent) {
			if cmd.CommandName == "findAndModify" && cmd.RequestID == first.Load() {
				close(saved)
				select {
				case <-released:
				case <-ctx.Done():
				}
			}
		},
	})
	old := &server{
		cfg: cfg, sourceCluster: source, targetCluster: gated, sourceVersion: sourceVer, membership: &ha.Membership{},
	}
	defer closeTenure(ctx, old)
	old.observeOwnership(ctx, ha.Ownership{Held: true, Term: 1})
	before := awaitTenure(t, ctx, old)

	start := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		const body = `{"includeNamespaces":["pcsm439.*"],"excludeNamespaces":["pcsm439.excluded"],"targetWriteConcern":"1"}`
		rec := httptest.NewRecorder()
		old.HandleStart(rec, httptest.NewRequestWithContext(ctx, http.MethodPost, "/start", strings.NewReader(body)))
		start <- rec
	}()
	awaitCheckpointSignal(t, ctx, saved)

	// The DB has acknowledged the pre-launch snapshot, but the old writer
	// has not returned from the driver and cannot have launched the clone.
	initial := readCheckpoint(t, ctx, target)
	require.Equal(t, pcsm.StateRunning, initial.Data.Lookup("state").StringValue())
	require.Equal(t, db+".*", initial.Data.Lookup("nsInclude", "0").StringValue())
	require.Equal(t, db+".excluded", initial.Data.Lookup("nsExclude", "0").StringValue())
	require.Equal(t, "1", initial.Data.Lookup("targetWriteConcern").StringValue())
	require.True(t, initial.Data.Lookup("clone").IsZero())
	require.Equal(t, pcsm.State(pcsm.StateIdle), before.pipeline.Status(ctx).State)

	old.observeOwnership(ctx, ha.Ownership{})
	release()
	select {
	case response := <-start:
		var body startResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
		require.False(t, body.Ok)
		require.Contains(t, body.Err, "saved but not launched here")
	case <-ctx.Done():
		require.FailNow(t, "old start did not return after checkpoint acknowledged")
	}

	active := &server{
		cfg: cfg, sourceCluster: source, targetCluster: target, sourceVersion: sourceVer, membership: &ha.Membership{},
	}
	defer closeTenure(ctx, active)
	active.observeOwnership(ctx, ha.Ownership{Held: true, Term: 2})
	current := awaitTenure(t, ctx, active)
	checkTakeoverAndFinalize(t, ctx, active, current, chosen, target.Database(db), targetDecoy)

	active.observeOwnership(ctx, ha.Ownership{})
	var reopened atomic.Bool
	freshSource := connectLifecycleTestClient(t, sourceURI, &event.CommandMonitor{
		Started: func(_ context.Context, cmd *event.CommandStartedEvent) {
			if cmd.CommandName == "appendOplogNote" ||
				(cmd.CommandName == "aggregate" && strings.Contains(cmd.Command.String(), "$changeStream")) {
				reopened.Store(true)
			}
		},
	})
	promoted := &server{
		cfg: cfg, sourceCluster: freshSource, targetCluster: target, sourceVersion: sourceVer, membership: &ha.Membership{},
	}
	defer closeTenure(ctx, promoted)
	promoted.observeOwnership(ctx, ha.Ownership{Held: true, Term: 3})
	status := awaitTenure(t, ctx, promoted).pipeline.Status(ctx)
	require.Equal(t, pcsm.State(pcsm.StateFinalized), status.State)
	require.NotNil(t, status.FinalizeStatus)
	require.True(t, status.FinalizeStatus.Completed)
	require.False(t, reopened.Load())
}

func checkTakeoverAndFinalize(
	t *testing.T, ctx context.Context, active *server, current *tenure,
	chosen *mongo.Collection, target *mongo.Database, targetDecoy *mongo.Collection,
) {
	t.Helper()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		status := current.pipeline.Status(ctx)
		require.NoError(c, status.Error)
		require.True(c, status.Clone.IsFinished())
	}, 25*time.Second, 50*time.Millisecond)
	count, err := target.Collection("chosen").CountDocuments(ctx, bson.D{{"_id", "cloned"}})
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
	count, err = targetDecoy.CountDocuments(ctx, bson.D{{"_id", "must-survive"}})
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
	count, err = targetDecoy.CountDocuments(ctx, bson.D{{"_id", "source-only"}})
	require.NoError(t, err)
	require.Zero(t, count)

	_, err = chosen.InsertOne(ctx, bson.D{{"_id", "catch-up"}})
	require.NoError(t, err)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		status := current.pipeline.Status(ctx)
		require.NoError(c, status.Error)
		require.True(c, status.InitialSyncCompleted)
	}, 25*time.Second, 50*time.Millisecond)

	rec := httptest.NewRecorder()
	active.HandleFinalize(rec, httptest.NewRequestWithContext(ctx, http.MethodPost, "/finalize", nil))
	var accepted finalizeResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &accepted))
	require.True(t, accepted.Ok, accepted.Err)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		require.Equal(c, pcsm.State(pcsm.StateFinalized), current.pipeline.Status(ctx).State)
	}, 5*time.Second, 50*time.Millisecond)
	require.Equal(t, pcsm.StateFinalized, readCheckpoint(t, ctx, active.targetCluster).Data.Lookup("state").StringValue())
}

func connectLifecycleTestClient(t *testing.T, uri string, monitor *event.CommandMonitor) *mongo.Client {
	t.Helper()

	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetMonitor(monitor))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Disconnect(context.WithoutCancel(t.Context()))) })

	return client
}
