package mdb_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/mdb"
)

func TestIsDatabaseDropPending(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"DatabaseDropPending error", mongo.CommandError{Name: "DatabaseDropPending", Code: 357}, true},
		{"other command error", mongo.CommandError{Name: "NamespaceNotFound"}, false},
		{"nil error", nil, false},
		{"non-command error", errors.New("generic"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, mdb.IsDatabaseDropPending(tt.err))
		})
	}
}

func TestIsSplitPointAlreadyBoundary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name: "boundary key message",
			err: mongo.CommandError{
				Message: "new split key { _id: 500 } is a boundary key of existing chunk " +
					"[{ _id: 500 },{ _id: MaxKey })",
			},
			expected: true,
		},
		{"other command error", mongo.CommandError{Name: "NamespaceNotFound", Message: "ns not found"}, false},
		{"nil error", nil, false},
		{"non-command error", errors.New("is a boundary key of existing chunk"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, mdb.IsSplitPointAlreadyChunkBoundary(tt.err))
		})
	}
}

func TestIsTransient_ConflictingOperationInProgress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			"ConflictingOperationInProgress error",
			mongo.CommandError{Name: "ConflictingOperationInProgress", Code: 117},
			true,
		},
		{
			"wrapped ConflictingOperationInProgress",
			errors.Wrap(mongo.CommandError{Name: "ConflictingOperationInProgress", Code: 117}, "move chunk"),
			true,
		},
		{"other command error", mongo.CommandError{Name: "NamespaceNotFound", Code: 26}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, mdb.IsTransient(tt.err))
		})
	}
}

func TestIsTransient_ChunkMigrationFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err       mongo.CommandError
		migration bool
		global    bool
	}{
		{mongo.CommandError{Name: "RetriableRemoteCommandFailure", Code: 91331}, true, true},
		{mongo.CommandError{Name: "LockTimeout", Code: 24}, true, true},
		{mongo.CommandError{Name: "ExceededTimeLimit", Code: 262}, true, true},
		// Interrupted also represents killOp, so it must not retry globally.
		{mongo.CommandError{Name: "Interrupted", Code: 11601}, true, false},
		// CallbackCanceled is internal to migration teardown, not a client-retriable error.
		{mongo.CommandError{Name: "CallbackCanceled", Code: 90}, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.err.Name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.migration, mdb.IsChunkMigrationTransient(tt.err),
				"IsChunkMigrationTransient: code %d", tt.err.Code)
			assert.Equal(t, tt.global, mdb.IsTransient(tt.err),
				"IsTransient: code %d", tt.err.Code)
		})
	}
}

type labeledError struct {
	labels []string
}

func (e labeledError) Error() string { return "labeled error" }

func (e labeledError) HasErrorLabel(label string) bool {
	return slices.Contains(e.labels, label)
}

func TestIsTransient_RetryableWriteLabel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			"unwrapped retryable-write label",
			labeledError{labels: []string{"RetryableWriteError"}},
			true,
		},
		{
			"wrapped retryable-write label",
			errors.Wrap(labeledError{labels: []string{"RetryableWriteError"}}, "insert batch"),
			true,
		},
		{
			"wrapped without retryable-write label",
			errors.Wrap(labeledError{labels: []string{"SomeOtherLabel"}}, "insert batch"),
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, mdb.IsTransient(tt.err))
		})
	}
}
