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

// TestIsTransient_ConflictingOperationInProgress locks in that a shard's
// ConflictingOperationInProgress (117) — returned while another chunk migration
// or DDL is in flight — is treated as transient so chunk splits/moves retry.
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

// labeledError is a minimal mongo.LabeledError implementation used to verify
// that IsTransient detects retryable write labels through error wrapping.
type labeledError struct {
	labels []string
}

func (e labeledError) Error() string { return "labeled error" }

func (e labeledError) HasErrorLabel(label string) bool {
	return slices.Contains(e.labels, label)
}

// TestIsTransient_RetryableWriteLabel locks the fix that detects the
// RetryableWriteError label via errors.As so it survives error wrapping inside
// retry closures (previously a direct type assertion hid the label once
// wrapped).
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
