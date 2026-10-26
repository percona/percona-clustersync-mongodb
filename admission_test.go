package main //nolint:testpackage // Exercises request admission against the server's tenure directly.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/xoptions"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/ha"
	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm"
)

func emptyCursor(ns string) bson.D {
	return bson.D{{"ok", 1}, {"cursor", bson.D{
		{"id", int64(0)}, {"ns", ns}, {"firstBatch", bson.A{}},
	}}}
}

// mockTarget answers every command with an empty cursor, enough for the
// not_active envelope's members read and for a missing recovery record.
func mockTarget(t *testing.T, ns string, n int) *mongo.Client {
	t.Helper()

	responses := make([]bson.D, n)
	for i := range responses {
		responses[i] = emptyCursor(ns)
	}

	opts := options.Client()
	require.NoError(t, xoptions.SetInternalClientOptions(opts, "deployment", drivertest.NewMockDeployment(responses...)))
	target, err := mongo.Connect(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = target.Disconnect(context.Background()) })

	return target
}

func TestRestoreReportsMissingCheckpoint(t *testing.T) {
	t.Parallel()

	target := mockTarget(t, config.PCSMDatabase+"."+config.RecoveryCollection, 1)
	pipeline := pcsm.New(t.Context(), nil, target, mdb.ServerVersion{}, false, false)

	err := Restore(t.Context(), target, pipeline, 1)

	require.ErrorIs(t, err, errNoCheckpoint)
	assert.Equal(t, pcsm.State(pcsm.StateIdle), pipeline.Status(t.Context()).State)
}

// TestRestoreRefusesCheckpointOfNewerTerm pins that a tenure never restores a
// checkpoint a newer term owns: it is told the owning term instead, before
// any pipeline state changes.
func TestRestoreRefusesCheckpointOfNewerTerm(t *testing.T) {
	t.Parallel()

	data, err := bson.Marshal(bson.D{{"state", pcsm.StateFailed}})
	require.NoError(t, err)
	record := bson.D{{"_id", recoveryID}, {"term", int64(5)}, {"data", bson.Raw(data)}}
	opts := options.Client()
	require.NoError(t, xoptions.SetInternalClientOptions(opts, "deployment", drivertest.NewMockDeployment(
		bson.D{{"ok", 1}, {"cursor", bson.D{
			{"id", int64(0)},
			{"ns", config.PCSMDatabase + "." + config.RecoveryCollection},
			{"firstBatch", bson.A{record}},
		}}},
	)))
	target, err := mongo.Connect(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = target.Disconnect(context.Background()) })
	pipeline := pcsm.New(t.Context(), nil, target, mdb.ServerVersion{}, false, false)

	err = Restore(t.Context(), target, pipeline, 1)

	require.ErrorIs(t, err, errCheckpointFenced)
	var fenced fencedError
	require.ErrorAs(t, err, &fenced)
	assert.Equal(t, int64(5), fenced.StoredTerm)
	assert.Equal(t, pcsm.State(pcsm.StateIdle), pipeline.Status(t.Context()).State,
		"a refused restore must not recover the pipeline")
}

func newTestTenure(t *testing.T, target *mongo.Client) *tenure {
	t.Helper()

	tctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	return &tenure{
		term: 1, ctx: tctx, cancel: cancel,
		pipeline: pcsm.New(tctx, nil, target, mdb.ServerVersion{}, false, false),
		ready:    make(chan struct{}), done: make(chan struct{}),
	}
}

// TestHandlersRefuseWithoutOpenTenure pins admission: an ACTIVE role is not
// enough to launch work; the request must be admitted under a tenure that is
// open for requests.
func TestHandlersRefuseWithoutOpenTenure(t *testing.T) {
	t.Parallel()

	target := mockTarget(t, config.PCSMDatabase+"."+config.MembersCollection, 4)
	membership := &ha.Membership{}
	membership.SetRole(ha.RoleActive, 1)
	s := &server{
		cfg:           &config.Config{},
		targetCluster: target,
		membership:    membership,
	}

	post := func() *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/start", strings.NewReader("{}"))
		s.HandleStart(rec, req)

		return rec
	}

	// Given: the role is ACTIVE but no tenure is open.
	rec := post()
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "not_active")

	// And: a tenure that ended before its restore finished is refused.
	failed := newTestTenure(t, target)
	failed.cancel()
	close(failed.done)
	s.tenure = failed

	rec = post()
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "not_active")

	// And: a tenure that ended after it opened is refused the same way.
	ended := newTestTenure(t, target)
	close(ended.ready)
	ended.cancel()
	s.tenure = ended

	rec = post()
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "not_active")

	for _, tn := range []*tenure{failed, ended} {
		assert.Equal(t, pcsm.State(pcsm.StateIdle), tn.pipeline.Status(t.Context()).State,
			"a refused request must not start the pipeline")
	}
}

// TestAdmitWaitsForRestore pins that a request reaching a tenure still
// restoring its pipeline is admitted once the restore is done, not bounced.
func TestAdmitWaitsForRestore(t *testing.T) {
	t.Parallel()

	target := mockTarget(t, config.PCSMDatabase+"."+config.MembersCollection, 1)
	tn := newTestTenure(t, target)
	s := &server{targetCluster: target, membership: &ha.Membership{}, tenure: tn}

	type outcome struct {
		ok       bool
		pipeline *pcsm.PCSM
	}
	admitted := make(chan outcome, 1)
	rec := httptest.NewRecorder()
	go func() {
		_, pipeline, ok := s.admit(t.Context(), rec)
		admitted <- outcome{ok: ok, pipeline: pipeline}
	}()

	// When: the restore finishes.
	close(tn.ready)

	// Then: the request is admitted to the tenure's pipeline, with no 409.
	select {
	case got := <-admitted:
		assert.True(t, got.ok)
		assert.Same(t, tn.pipeline, got.pipeline)
	case <-t.Context().Done():
		require.FailNow(t, "admit did not return after the restore finished")
	}

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, rec.Body.String())
}

// TestAdmitRefusesWhenRequestExpiresDuringRestore pins the bound on that wait:
// a request whose context ends while the restore is still in progress is
// refused with 409.
func TestAdmitRefusesWhenRequestExpiresDuringRestore(t *testing.T) {
	t.Parallel()

	target := mockTarget(t, config.PCSMDatabase+"."+config.MembersCollection, 1)
	tn := newTestTenure(t, target)
	s := &server{targetCluster: target, membership: &ha.Membership{}, tenure: tn}

	reqCtx, cancelReq := context.WithCancel(t.Context())
	cancelReq()
	rec := httptest.NewRecorder()

	_, pipeline, ok := s.admit(reqCtx, rec)

	assert.False(t, ok)
	assert.Nil(t, pipeline)
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "not_active")
}

func TestRefusedNotActive(t *testing.T) {
	t.Parallel()

	target := mockTarget(t, config.PCSMDatabase+"."+config.MembersCollection, 1)
	s := &server{targetCluster: target, membership: &ha.Membership{}}

	rec := httptest.NewRecorder()
	assert.False(t, s.refusedNotActive(t.Context(), rec, errors.New("other")))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, rec.Body.String())

	rec = httptest.NewRecorder()
	assert.True(t, s.refusedNotActive(t.Context(), rec, errors.Wrap(pcsm.ErrNotActive, "start")))
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "not_active")
}
