package mdb_test

import (
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

func TestIsTransient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name: "nil error",
		},
		{
			name:     "wrapped primary stepped down command error",
			err:      errors.Wrap(mongo.CommandError{Name: "PrimarySteppedDown", Code: 189}, "drop collection"),
			expected: true,
		},
		{
			name: "wrapped primary stepped down write concern error",
			err: errors.Wrap(mongo.WriteException{
				WriteConcernError: &mongo.WriteConcernError{Name: "PrimarySteppedDown", Code: 189},
			}, "drop collection"),
			expected: true,
		},
		{
			name: "wrapped retryable write label",
			err: errors.Wrap(mongo.CommandError{
				Name:   "SomeTransientLabelOnlyError",
				Labels: []string{"RetryableWriteError"},
			}, "write command"),
			expected: true,
		},
		{
			name: "wrapped non transient command error",
			err:  errors.Wrap(mongo.CommandError{Name: "NamespaceNotFound", Code: 26}, "drop collection"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, mdb.IsTransient(tt.err))
		})
	}
}
