package clone //nolint:testpackage // Verifies resolved internal copy-manager options.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/percona/percona-clustersync-mongodb/sel"
)

func TestCloneAndCopyManagerWriteConcernDefaults(t *testing.T) {
	t.Parallel()

	for _, wc := range []*writeconcern.WriteConcern{nil, {W: 1}, {W: 2}} {
		opts := &Options{WriteConcern: wc}
		cln := NewClone(nil, nil, nil, sel.AllowAllFilter, opts, false)
		copyOpts := CopyManagerOptions{WriteConcern: cln.options.WriteConcern}
		copyOpts.applyDefaults()
		assert.Equal(t, cln.options.WriteConcern, copyOpts.WriteConcern)
		if wc == nil {
			assert.Equal(t, "majority", copyOpts.WriteConcern.W)
		} else {
			assert.Equal(t, wc, copyOpts.WriteConcern)
		}
	}

	opts := CopyManagerOptions{}
	opts.applyDefaults()
	assert.Equal(t, "majority", opts.WriteConcern.W)
}
