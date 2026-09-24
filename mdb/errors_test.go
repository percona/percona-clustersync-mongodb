package mdb_test

import (
	"crypto/x509"
	"net"
	"slices"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"

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

// TestIsTransient_HandshakeDialError covers the driver's handshake failure
// shape seen when the target hostname stops resolving during a network
// partition (Docker network disconnect): a topology.ConnectionError wrapping
// a *net.OpError / *net.DNSError. The driver does not label it NetworkError,
// so it must be recognized as a net.Error instead.
func TestIsTransient_HandshakeDialError(t *testing.T) {
	t.Parallel()

	dnsErr := &net.DNSError{Err: "no such host", Name: "mongos2", Server: "127.0.0.11:53", IsNotFound: true}
	opErr := &net.OpError{Op: "dial", Net: "tcp", Err: dnsErr}
	connErr := topology.ConnectionError{ConnectionID: "mongos2:27017[-33]", Wrapped: opErr}

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"dns not found under connection error", connErr, true},
		{"wrapped by caller", errors.Wrap(connErr, "drop collection"), true},
		{
			"connection refused, unlabeled shape",
			topology.ConnectionError{Wrapped: &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}},
			true,
		},
		// A handshake that fails above the dial (TLS trust) is not a dial
		// error and must stay terminal.
		{"tls unknown authority", topology.ConnectionError{Wrapped: x509.UnknownAuthorityError{}}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, mdb.IsTransient(tt.err))
		})
	}
}

// TestIsTransient_DDLWriteConcernError covers the DDL shape: for drop, create
// and createIndexes the driver's wrapErrors leaves a writeConcernError as a raw
// driver.WriteCommandError instead of converting it to mongo.WriteException.
func TestIsTransient_DDLWriteConcernError(t *testing.T) {
	t.Parallel()

	wce := func(code int64, name string) driver.WriteCommandError {
		return driver.WriteCommandError{
			WriteConcernError: &driver.WriteConcernError{Name: name, Code: code, Message: name},
		}
	}

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"primary stepped down", wce(189, "PrimarySteppedDown"), true},
		{"wrapped by caller", errors.Wrap(wce(189, "PrimarySteppedDown"), "drop collection a.b"), true},
		{"interrupted due to repl state change", wce(11602, "InterruptedDueToReplStateChange"), true},
		{"not writable primary", wce(10107, "NotWritablePrimary"), true},
		{"write error code", driver.WriteCommandError{WriteErrors: driver.WriteErrors{{Code: 189}}}, true},
		{"unsatisfiable write concern", wce(100, "UnsatisfiableWriteConcern"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, mdb.IsTransient(tt.err))
		})
	}
}
