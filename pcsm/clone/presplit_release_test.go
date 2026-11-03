//nolint:testpackage // tests the unexported presplit ledger
package clone

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardSizes_ReleaseRemovesRecordedCharge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		sizes []int64
		// moved reconciles every chunk onto the second planned shard before
		// recording, as a placement that did not go as planned does.
		moved    bool
		recorded bool
		release  string
		wantKept bool
	}{
		{
			name:     "a dropped copy does not keep charging its shards",
			sizes:    []int64{1000, 10},
			recorded: true,
			release:  "db.c",
		},
		{
			name:     "release of an unknown namespace is a no-op",
			sizes:    []int64{500},
			release:  "db.unknown",
			wantKept: true,
		},
		{
			name:     "reconciled placement releases what the chunks really hold",
			sizes:    []int64{600, 400},
			moved:    true,
			recorded: true,
			release:  "db.c",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			shards := []string{"s0", "s1"}
			w := newShardSizes()
			assign := w.assignLargestFirst(tt.sizes, shards)

			placed := assign
			if tt.moved {
				require.NotEqual(t, assign[0], assign[1])

				placed = []string{assign[1], assign[1]}
				w.reconcile(tt.sizes, assign, placed)
			}

			if tt.recorded {
				w.record("db.c", tt.sizes, placed)
			}

			w.release(tt.release)

			for i, shard := range assign {
				wantSize, wantCount := int64(0), 0
				if tt.wantKept {
					wantSize, wantCount = tt.sizes[i], 1
				}

				assert.Equal(t, wantSize, w.sizes[shard], "released charge must leave the shard")
				assert.Equal(t, wantCount, w.counts[shard])
			}

			if !tt.wantKept {
				assert.Equal(t, assign, w.assignLargestFirst(tt.sizes, shards),
					"a released collection must place like the first time")
			}
		})
	}
}
