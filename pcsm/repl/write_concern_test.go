package repl //nolint:testpackage // Verifies successive internal bulk-writer factories.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

func TestWorkerSuccessiveBulkWriteConcern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		collectionBulk bool
		wc             *writeconcern.WriteConcern
		want           any
	}{
		{"client default majority", false, nil, "majority"},
		{"client explicit one", false, writeconcern.W1(), 1},
		{"client explicit two", false, &writeconcern.WriteConcern{W: 2}, 2},
		{"collection default majority", true, nil, "majority"},
		{"collection explicit one", true, writeconcern.W1(), 1},
		{"collection explicit two", true, &writeconcern.WriteConcern{W: 2}, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			opts := &Options{WriteConcern: tt.wc}
			opts.applyDefaults()
			w := newWorker(0, opts, nil, nil, tt.collectionBulk, false, make(chan error, 1))

			for _, bw := range []bulkWriter{w.currentBulkWrite, w.newBulkWriter(), w.newBulkWriter()} {
				if tt.collectionBulk {
					cbw, ok := bw.(*collectionBulkWrite)
					require.True(t, ok)
					assert.Equal(t, tt.want, cbw.writeConcern.W)
				} else {
					cbw, ok := bw.(*clientBulkWrite)
					require.True(t, ok)
					assert.Equal(t, tt.want, cbw.writeConcern.W)
				}
			}
		})
	}
}
