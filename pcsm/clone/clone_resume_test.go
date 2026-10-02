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

	t.Run("redo of a collection does not charge it twice", func(t *testing.T) {
		t.Parallel()

		shards := []string{"s0", "s1"}
		w := newShardSizes()
		sizes := []int64{1000, 10}

		first := w.assignLargestFirst(sizes, shards)
		w.record("db.c", sizes, first)

		w.release("db.c")
		assert.Equal(t, int64(0), w.sizes[first[0]], "released charge must leave the shard")
		assert.Equal(t, int64(0), w.sizes[first[1]])
		assert.Equal(t, 0, w.counts[first[0]])
		assert.Equal(t, 0, w.counts[first[1]])

		second := w.assignLargestFirst(sizes, shards)
		assert.Equal(t, first, second, "a released collection must place like the first time")
	})

	t.Run("release of an unknown namespace is a no-op", func(t *testing.T) {
		t.Parallel()

		shards := []string{"s0", "s1"}
		w := newShardSizes()
		assignment := w.assignLargestFirst([]int64{500}, shards)

		w.release("db.unknown")

		assert.Equal(t, int64(500), w.sizes[assignment[0]])
		assert.Equal(t, 1, w.counts[assignment[0]])
	})

	t.Run("reconciled placement releases what the chunks really hold", func(t *testing.T) {
		t.Parallel()

		shards := []string{"s0", "s1"}
		w := newShardSizes()
		sizes := []int64{600, 400}
		assign := w.assignLargestFirst(sizes, shards)
		require.NotEqual(t, assign[0], assign[1])

		actual := []string{assign[1], assign[1]}
		w.reconcile(sizes, assign, actual)
		w.record("db.c", sizes, actual)

		w.release("db.c")

		assert.Equal(t, int64(0), w.sizes[assign[0]])
		assert.Equal(t, int64(0), w.sizes[assign[1]])
		assert.Equal(t, 0, w.counts[assign[0]])
		assert.Equal(t, 0, w.counts[assign[1]])
	})
}
