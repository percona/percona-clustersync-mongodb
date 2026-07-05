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
