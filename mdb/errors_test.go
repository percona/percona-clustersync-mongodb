package mdb_test

import (
	"crypto/x509"
	"net"
	"slices"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/mongo"
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
		{"connection refused", topology.ConnectionError{Wrapped: &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}}, true},
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
