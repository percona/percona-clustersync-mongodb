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
