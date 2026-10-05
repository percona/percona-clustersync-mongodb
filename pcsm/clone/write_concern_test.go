package clone //nolint:testpackage // Verifies resolved internal copy-manager options.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/percona/percona-clustersync-mongodb/sel"
)

func TestCloneAndCopyManagerWriteConcernDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		wc   *writeconcern.WriteConcern
		want any
	}{
		{"default majority", nil, "majority"},
		{"explicit one", writeconcern.W1(), 1},
		{"explicit two", &writeconcern.WriteConcern{W: 2}, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			opts := &Options{WriteConcern: tt.wc}
			cln := NewClone(nil, nil, nil, sel.AllowAllFilter, opts, false)
			assert.Equal(t, tt.want, cln.options.WriteConcern.W)
			copyOpts := CopyManagerOptions{WriteConcern: cln.options.WriteConcern}
			copyOpts.applyDefaults()
			assert.Equal(t, tt.want, copyOpts.WriteConcern.W)
		})
	}
}

func TestCopyManagerDefaultWriteConcern(t *testing.T) {
	t.Parallel()

	opts := CopyManagerOptions{}
	opts.applyDefaults()
	assert.Equal(t, "majority", opts.WriteConcern.W)
}
