package main //nolint:testpackage // Exercise the HTTP handler and its tenure writer together.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/xoptions"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/ha"
	"github.com/percona/percona-clustersync-mongodb/pcsm"
)

func TestStartWaitsForCheckpoint(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		hold  time.Duration
		fence bool
	}{
		{name: "acknowledged"},
		{name: "HTTP deadline", hold: ServerResponseTimeout},
		{name: "fenced", fence: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) { checkStartCheckpoint(t, tc.hold, tc.fence) })
		})
	}
}

func checkStartCheckpoint(t *testing.T, hold time.Duration, fence bool) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	issued := make(chan bson.Raw, 1)
	release := make(chan struct{})
	cloneCalled := make(chan struct{}, 1)
	target := gatedStartTarget(t, ctx, issued, release, fence)
	source := mockedTarget(t, &event.CommandMonitor{Started: func(ctx context.Context, e *event.CommandStartedEvent) {
		if e.CommandName == "appendOplogNote" {
			cloneCalled <- struct{}{}
			<-ctx.Done()
		}
	}}, bson.D{{"ok", 0}, {"errmsg", "end test clone"}})
	t.Cleanup(func() { require.NoError(t, source.Disconnect(t.Context())) })

	s := &server{
		cfg:           &config.Config{RecoveryCheckpointInterval: time.Hour},
		sourceCluster: source,
		targetCluster: target,
		membership:    &ha.Membership{},
	}
	s.observeOwnership(ctx, ha.Ownership{Held: true, Term: 2})
	synctest.Wait()
	tn := currentTenure(s)
	require.NotNil(t, tn)
	require.True(t, tn.isReady())

	// The mock deployment's response queue is not safe for concurrent
	// operations. Membership reads use a separate mock connection.
	emptyMembers := bson.D{{"ok", 1}, {"cursor", bson.D{
		{"id", int64(0)}, {"ns", config.PCSMDatabase + "." + config.MembersCollection}, {"firstBatch", bson.A{}},
	}}}
	members := mockedTarget(t, nil, emptyMembers, emptyMembers)
	t.Cleanup(func() { require.NoError(t, members.Disconnect(t.Context())) })
	api := &server{cfg: s.cfg, tenure: tn, targetCluster: members, membership: s.membership}

	done := make(chan startResponse, 1)
	go func() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/start",
			strings.NewReader(`{"includeNamespaces":["chosen"],"targetWriteConcern":"1"}`))
		api.HandleStart(rec, req)
		var response startResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		if err != nil {
			t.Error(err)
		}
		done <- response
	}()
	synctest.Wait()
	require.Len(t, issued, 1, "initial checkpoint should be in flight")
	require.Empty(t, done, "start answered before checkpoint acknowledgment")
	require.Empty(t, cloneCalled, "cloning launched before checkpoint acknowledgment")
	require.Equal(t, pcsm.State(pcsm.StateIdle), tn.pipeline.Status(ctx).State)
	snapshot := <-issued
	require.Equal(t, "chosen", snapshot.Lookup("nsInclude", "0").StringValue())
	require.Equal(t, "1", snapshot.Lookup("targetWriteConcern").StringValue())
	require.True(t, snapshot.Lookup("clone").IsZero(), "first checkpoint must not contain clone progress")

	if hold != 0 {
		time.Sleep(hold) // Advance the virtual clock past the HTTP wait.
		synctest.Wait()
		require.Len(t, done, 1)
		response := <-done
		require.False(t, response.Ok)
		require.Contains(t, response.Err, "could not be confirmed")

		retry := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/start",
			strings.NewReader(`{"includeNamespaces":["different"]}`))
		api.HandleStart(retry, req)
		var rejected startResponse
		require.NoError(t, json.Unmarshal(retry.Body.Bytes(), &rejected))
		require.False(t, rejected.Ok)
		require.Contains(t, rejected.Err, "waiting for its checkpoint")
	}

	close(release)
	synctest.Wait()
	if fence {
		require.Len(t, done, 1)
		response := <-done
		require.False(t, response.Ok)
		require.Contains(t, response.Err, "fenced")
		require.Empty(t, cloneCalled)
		require.Nil(t, currentTenure(s))

		return
	}

	if hold == 0 {
		require.Len(t, done, 1)
		require.True(t, (<-done).Ok)
	}
	require.Len(t, cloneCalled, 1, "acknowledged checkpoint did not launch the clone")
	require.Same(t, tn, currentTenure(s), "HTTP wait must not end the tenure")
	s.observeOwnership(ctx, ha.Ownership{})
}

func gatedStartTarget(
	t *testing.T, ctx context.Context, issued chan<- bson.Raw, release <-chan struct{}, fence bool,
) *mongo.Client {
	t.Helper()

	claim := claimed(bson.D{{"_id", recoveryID}, {"term", int64(2)}})
	deployment := drivertest.NewMockDeployment()
	held := false
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		if e.CommandName != "findAndModify" {
			return
		}
		data, ok := e.Command.Lookup("update", "$set", "data").DocumentOK()
		if ok && !held {
			held = true
			issued <- data
			select {
			case <-release:
			case <-ctx.Done():
			}
			if fence {
				deployment.AddResponses(noDocument())

				return
			}
		}
		deployment.AddResponses(claim)
	}}
	opts := options.Client().SetMonitor(monitor)
	require.NoError(t, xoptions.SetInternalClientOptions(opts, "deployment", deployment))
	target, err := mongo.Connect(opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, target.Disconnect(context.WithoutCancel(ctx))) })

	return target
}
