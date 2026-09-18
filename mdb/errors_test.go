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

// TestIsTransient_ChunkMigrationFailures covers the errors a
// client-issued moveChunk returns during chunk pre-split, taken
// from codes migration path actually raises in server source.
func TestIsTransient_ChunkMigrationFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err mongo.CommandError
		// migration is whether the moveChunk retry (IsChunkMigrationTransient)
		// retries the code at all.
		migration bool
		// global is whether IsTransient (every RunWithRetry caller, including the
		// unbounded replication bulk-write loop) retries the code, as opposed to
		// only the moveChunk retry.
		global bool
	}{
		// _configsvrMoveRange rewrites InterruptedDueToReplStateChange into
		// this code for remote callers, which is every moveChunk issued through
		// mongos, and mongos passes the config server's status through
		// unchanged.
		{mongo.CommandError{Name: "RetriableRemoteCommandFailure", Code: 91331}, true, true},

		// The donor shard failing to take its lock for the migration. The
		// server expects it often enough to keep a counter for it
		// (ShardingStatistics::countDonorMoveChunkLockTimeout).
		{mongo.CommandError{Name: "LockTimeout", Code: 24}, true, true},

		// Raised by MigrationSourceManager and MigrationDestinationManager
		// while the migration is in flight. Interrupted is also what killOp
		// returns, so it stays out of the global set.
		{mongo.CommandError{Name: "ExceededTimeLimit", Code: 262}, true, true},
		{mongo.CommandError{Name: "Interrupted", Code: 11601}, true, false},

		// The destination manager sets CallbackCanceled only on an internal
		// promise during teardown, after its sole waiter has returned. It never
		// reaches the client, so retrying it would be a guess.
		{mongo.CommandError{Name: "CallbackCanceled", Code: 90}, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.err.Name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.migration, mdb.IsChunkMigrationTransient(tt.err),
				"code %d decides whether a pre-split move retries or fails the clone",
				tt.err.Code)
			assert.Equal(t, tt.global, mdb.IsTransient(tt.err),
				"code %d global transient classification", tt.err.Code)
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
