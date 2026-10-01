package repl //nolint:testpackage // Verifies successive internal bulk-writer factories.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

func TestWorkerSuccessiveBulkWriteConcern(t *testing.T) {
	t.Parallel()

	for _, collectionBulk := range []bool{false, true} {
		for _, wc := range []*writeconcern.WriteConcern{nil, {W: 1}, {W: 2}} {
			opts := &Options{WriteConcern: wc}
			opts.applyDefaults()
			w := newWorker(0, opts, nil, nil, collectionBulk, false, make(chan error, 1))

			for _, bw := range []bulkWriter{w.currentBulkWrite, w.newBulkWriter(), w.newBulkWriter()} {
				if collectionBulk {
					cbw, ok := bw.(*collectionBulkWrite)
					require.True(t, ok)
					assert.Equal(t, opts.WriteConcern, cbw.writeConcern)
				} else {
					cbw, ok := bw.(*clientBulkWrite)
					require.True(t, ok)
					assert.Equal(t, opts.WriteConcern, cbw.writeConcern)
				}
			}
			if wc == nil {
				assert.Equal(t, "majority", opts.WriteConcern.W)
			}
		}
	}
}
