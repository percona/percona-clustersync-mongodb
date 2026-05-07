package pcsm //nolint:testpackage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/pcsm/clone"
)

func TestCopyFinalizeStatus_Nil(t *testing.T) {
	t.Parallel()

	got := copyFinalizeStatus(nil)
	assert.Nil(t, got)
}

func TestCopyFinalizeStatus_DeepCopiesIndexes(t *testing.T) {
	t.Parallel()

	original := &FinalizeStatus{
		Completed:   true,
		StartedAt:   time.Now(),
		CompletedAt: time.Now(),
		UnsuccessfulIndexes: []catalog.UnsuccessfulIndex{
			{Namespace: "db.coll", Name: "idx", Type: catalog.IndexFailed},
		},
	}

	copied := copyFinalizeStatus(original)
	require.NotNil(t, copied)
	assert.Equal(t, original.Completed, copied.Completed)
	assert.Equal(t, original.StartedAt, copied.StartedAt)
	assert.Equal(t, original.CompletedAt, copied.CompletedAt)
	assert.Equal(t, original.UnsuccessfulIndexes, copied.UnsuccessfulIndexes)

	// Mutating the copy must not affect the original.
	copied.UnsuccessfulIndexes[0].Name = "mutated"
	assert.Equal(t, "idx", original.UnsuccessfulIndexes[0].Name)
}

func TestStatus_FinalizationIncluded(t *testing.T) {
	t.Parallel()

	startedAt := time.Now().Add(-30 * time.Second)
	completedAt := time.Now()

	fs := &FinalizeStatus{
		Completed:   true,
		StartedAt:   startedAt,
		CompletedAt: completedAt,
		UnsuccessfulIndexes: []catalog.UnsuccessfulIndex{
			{
				Namespace: "mydb.users",
				Name:      "email_unique_idx",
				Type:      catalog.IndexFailed,
			},
		},
	}

	// Uses StateFailed because that path skips the source-cluster lookup in
	// Status() which would otherwise need a real MongoDB client.
	p := &PCSM{
		state:          StateFailed,
		err:            errors.New("boom"),
		finalizeStatus: fs,
		clone:          &mockCloner{doneCh: make(chan struct{})},
		repl: &mockReplicator{
			doneCh:     make(chan struct{}),
			startTime:  time.Now(),
			lastOpTime: bson.Timestamp{T: 100},
		},
		onStateChanged: func(State) {},
	}

	got := p.Status(context.Background())

	require.NotNil(t, got.FinalizeStatus)
	assert.True(t, got.FinalizeStatus.Completed)
	assert.Equal(t, startedAt, got.FinalizeStatus.StartedAt)
	assert.Equal(t, completedAt, got.FinalizeStatus.CompletedAt)
	assert.Len(t, got.FinalizeStatus.UnsuccessfulIndexes, 1)
	assert.Equal(t, catalog.IndexFailed, got.FinalizeStatus.UnsuccessfulIndexes[0].Type)

	// And the returned slice must be a copy, so mutating it cannot affect PCSM internals.
	got.FinalizeStatus.UnsuccessfulIndexes[0].Name = "mutated"
	assert.Equal(t, "email_unique_idx", fs.UnsuccessfulIndexes[0].Name)
}

func TestStatus_NoFinalizationBeforeFinalize(t *testing.T) {
	t.Parallel()

	// Uses StateFailed because that path skips the source-cluster lookup in
	// Status() which would otherwise need a real MongoDB client.
	p := &PCSM{
		state:          StateFailed,
		err:            errors.New("boom"),
		finalizeStatus: nil,
		clone:          &mockCloner{doneCh: make(chan struct{}), status: clone.Status{}},
		repl:           &mockReplicator{doneCh: make(chan struct{})},
		onStateChanged: func(State) {},
	}

	got := p.Status(context.Background())
	assert.Nil(t, got.FinalizeStatus, "Finalization must be nil when finalize was never triggered")
}
