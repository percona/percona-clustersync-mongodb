//nolint:testpackage // tests unexported resume bookkeeping and the presplit ledger
package clone

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/errors"
)

func TestResume_Guards(t *testing.T) {
	t.Parallel()

	closedDone := func() chan struct{} {
		ch := make(chan struct{})
		close(ch)

		return ch
	}

	tests := []struct {
		name    string
		clone   *Clone
		wantErr string
	}{
		{
			name:    "not started",
			clone:   &Clone{doneCh: make(chan struct{})},
			wantErr: "not started",
		},
		{
			name:    "already completed",
			clone:   &Clone{doneCh: closedDone(), startTime: time.Now(), finishTime: time.Now()},
			wantErr: "already completed",
		},
		{
			name:    "still running",
			clone:   &Clone{doneCh: make(chan struct{}), startTime: time.Now()},
			wantErr: "still running",
		},
		{
			name:    "existing error",
			clone:   &Clone{doneCh: closedDone(), startTime: time.Now(), err: errors.New("boom")},
			wantErr: "cannot resume due an existing error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.clone.Resume(context.Background())
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestRemainingNamespaces_SkipsCompletedByTaskKey(t *testing.T) {
	t.Parallel()

	uuidA := &bson.Binary{Subtype: 0x04, Data: []byte("aaaaaaaaaaaaaaaa")}
	uuidB := &bson.Binary{Subtype: 0x04, Data: []byte("bbbbbbbbbbbbbbbb")}

	collA := namespaceInfo{Database: "db", Collection: "a", UUID: uuidA}
	collB := namespaceInfo{Database: "db", Collection: "b", UUID: uuidB}
	view := namespaceInfo{Database: "db", Collection: "v"}

	c := &Clone{
		namespaces: []namespaceInfo{collA, collB, view},
		completed:  map[string]struct{}{taskKey(collA): {}, taskKey(view): {}},
	}

	// A renamed collection keeps its UUID, so its key is stable across names.
	renamedA := collA
	renamedA.Collection = "a2"
	assert.Equal(t, taskKey(collA), taskKey(renamedA))
	assert.NotEqual(t, taskKey(collA), taskKey(collB))

	assert.Equal(t, []namespaceInfo{collB}, c.remainingNamespaces())
}

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
			name:     "redo of a collection does not charge it twice",
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
