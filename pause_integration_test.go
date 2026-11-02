//go:build integration

package main //nolint:testpackage // Exercise the real HTTP handlers and pipeline against MongoDB.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/ha"
	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm"
	"github.com/percona/percona-clustersync-mongodb/util"
)

func TestPauseHTTPReportsDrainingUntilTargetWriteCompletes(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	sourceURI, targetURI, err := startDemotedMongo(ctx)
	require.NoError(t, err)

	connect := func(uri string, monitor *event.CommandMonitor) *mongo.Client {
		client, connectErr := mongo.Connect(options.Client().
			ApplyURI(uri).
			SetServerSelectionTimeout(10 * time.Second).
			SetRetryWrites(false).
			SetMonitor(monitor))
		require.NoError(t, connectErr)
		t.Cleanup(func() {
			require.NoError(t, util.CtxWithTimeout(context.Background(), 5*time.Second, client.Disconnect))
		})

		return client
	}

	// Observe the change-stream request, rather than guessing when clone hands
	// over to replication. The stream opens at the clone's inclusive start.
	watching := make(chan struct{})
	markWatching := sync.OnceFunc(func() { close(watching) })
	source := connect(sourceURI, &event.CommandMonitor{
		Started: func(_ context.Context, command *event.CommandStartedEvent) {
			if command.CommandName == "aggregate" && bytes.Contains(command.Command, []byte("$changeStream")) {
				markWatching()
			}
		},
	})
	gate := newMarkerWriteGate("pause-drain-marker")
	target := connect(targetURI, gate.monitor())
	sourceColl := source.Database(demotedTestDB).Collection(demotedTestColl)
	_, err = sourceColl.InsertOne(ctx, bson.D{{"_id", "cloned"}})
	require.NoError(t, err)
	sourceVer, err := mdb.Version(ctx, source)
	require.NoError(t, err)

	pipeline := pcsm.New(ctx, source, target, sourceVer, false, false)
	t.Cleanup(func() {
		cancel()
		gate.releaseAll()
		require.NoError(t, util.CtxWithTimeout(context.Background(), 5*time.Second, func(stopCtx context.Context) error {
			return errors.Wrap(pipeline.Stop(stopCtx), "stop paused pipeline")
		}))
	})
	paused := make(chan struct{})
	markPaused := sync.OnceFunc(func() { close(paused) })
	pipeline.SetOnStateChanged(func(state pcsm.State) {
		if state == pcsm.StatePaused {
			markPaused()
		}
	})

	membership := &ha.Membership{}
	membership.SetRole(ha.RoleActive, 1)
	activeTenure := &tenure{
		term: 1, ctx: ctx, cancel: cancel, pipeline: pipeline,
		ready: make(chan struct{}), done: make(chan struct{}),
	}
	close(activeTenure.ready)
	close(activeTenure.done)
	s := &server{
		cfg:           &config.Config{},
		sourceCluster: source,
		targetCluster: target,
		membership:    membership,
		tenure:        activeTenure,
	}
	httpServer := httptest.NewServer(s.Handler())
	t.Cleanup(httpServer.Close)
	httpClient := httpServer.Client()
	httpClient.Timeout = 10 * time.Second
	request := func(method, path string) statusResponse {
		req, requestErr := http.NewRequestWithContext(ctx, method, httpServer.URL+path, http.NoBody)
		require.NoError(t, requestErr)
		response, requestErr := httpClient.Do(req)
		require.NoError(t, requestErr)
		defer response.Body.Close()
		require.Equal(t, http.StatusOK, response.StatusCode)
		var body statusResponse
		require.NoError(t, json.NewDecoder(response.Body).Decode(&body))

		return body
	}
	await := func(signal <-chan struct{}) {
		select {
		case <-signal:
		case <-ctx.Done():
			require.FailNow(t, "pipeline event did not arrive", "%v", ctx.Err())
		}
	}

	// Given: a replication write is held before reaching the target.
	idleFinalize := request(http.MethodPost, "/finalize")
	require.False(t, idleFinalize.Ok)
	require.Contains(t, idleFinalize.Err, "clone is not completed")
	require.True(t, request(http.MethodPost, "/start").Ok)
	await(watching)
	gate.enable()
	_, err = sourceColl.InsertOne(ctx, bson.D{{"_id", "pause-drain-marker"}})
	require.NoError(t, err)
	await(gate.armed)

	// When: pause is accepted while that write cannot finish.
	require.True(t, request(http.MethodPost, "/pause").Ok)
	status := request(http.MethodGet, "/status")
	require.True(t, status.Ok, "%s", status.Err)
	require.Equal(t, pcsm.State(pcsm.StatePausing), status.State)
	require.True(t, request(http.MethodPost, "/pause").Ok, "a pending pause is idempotent")

	// The HTTP deadline bounds an early resume, not the accepted pause.
	// This deliberately tests the deadline itself; no sleep orders the run.
	resume := request(http.MethodPost, "/resume")
	require.False(t, resume.Ok)
	require.Contains(t, resume.Err, context.DeadlineExceeded.Error())
	require.Equal(t, pcsm.State(pcsm.StatePausing), request(http.MethodGet, "/status").State)

	// Then: only releasing the real target write permits paused and resume.
	gate.releaseAll()
	await(paused)
	status = request(http.MethodGet, "/status")
	require.True(t, status.Ok, "%s", status.Err)
	require.Equal(t, pcsm.State(pcsm.StatePaused), status.State)
	count, err := target.Database(demotedTestDB).Collection(demotedTestColl).
		CountDocuments(ctx, bson.D{{"_id", "pause-drain-marker"}})
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
	require.True(t, request(http.MethodPost, "/resume").Ok)
	require.Equal(t, pcsm.State(pcsm.StateRunning), request(http.MethodGet, "/status").State)
}
