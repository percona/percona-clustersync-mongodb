package main //nolint:testpackage // Exercises request admission against the server's epoch directly.

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

	err := Restore(t.Context(), target, pipeline)

	require.ErrorIs(t, err, errNoCheckpoint)
	assert.Equal(t, pcsm.State(pcsm.StateIdle), pipeline.Status(t.Context()).State)
}

// TestHandlersRefuseWithoutOpenEpoch pins admission: an ACTIVE role is not
// enough to launch work; the request must be admitted under an open epoch.
func TestHandlersRefuseWithoutOpenEpoch(t *testing.T) {
	t.Parallel()

	target := mockTarget(t, config.PCSMDatabase+"."+config.MembersCollection, 4)
	membership := &ha.Membership{}
	membership.SetRole(ha.RoleActive, 1)
	s := &server{
		cfg:           &config.Config{},
		targetCluster: target,
		pcsm:          pcsm.New(t.Context(), nil, target, mdb.ServerVersion{}, false, false),
		membership:    membership,
	}

	post := func() *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/start", strings.NewReader("{}"))
		s.HandleStart(rec, req)

		return rec
	}

	// Given: the role is ACTIVE but no epoch is open (promotion not consumed).
	rec := post()
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "not_active")

	// And: an epoch that ended since admission is refused the same way.
	epoch, endEpoch := context.WithCancel(t.Context())
	s.epochCtx, s.epochCancel = epoch, endEpoch
	endEpoch()

	rec = post()
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "not_active")

	assert.Equal(t, pcsm.State(pcsm.StateIdle), s.pcsm.Status(t.Context()).State,
		"a refused request must not start the pipeline")
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
