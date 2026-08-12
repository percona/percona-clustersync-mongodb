//nolint:testpackage // tests unexported isHashedPrefix/presplit dispatch
package clone

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
)

func TestIsHashedPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		shardKey bson.D
		want     bool
	}{
		{"single hashed", bson.D{{Key: "_id", Value: "hashed"}}, true},
		{"compound hashed prefix", bson.D{{Key: "x", Value: "hashed"}, {Key: "region", Value: int32(1)}}, true},
		{"compound non-hashed prefix", bson.D{{Key: "region", Value: int32(1)}, {Key: "x", Value: "hashed"}}, false},
		{"single ranged", bson.D{{Key: "_id", Value: int32(1)}}, false},
		{"empty key", bson.D{}, false},
		{"nil key", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, isHashedPrefix(tt.shardKey))
		})
	}
}

func TestPresplitDispatchNoop(t *testing.T) {
	t.Parallel()

	// Every current branch is a no-op: hashed relies on the balanced native
	// shardCollection layout, and ranged with <=1 chunk has nothing to replay.
	// None of them touch the target client, so a nil client is safe. Ranged
	// multi-chunk placement is covered by E2E.
	tests := []struct {
		name   string
		shInfo *mdb.ShardingInfo
	}{
		{
			name:   "hashed prefix",
			shInfo: &mdb.ShardingInfo{ShardKey: bson.D{{Key: "_id", Value: "hashed"}}},
		},
		{
			name: "ranged single chunk",
			shInfo: &mdb.ShardingInfo{
				ShardKey: bson.D{{Key: "_id", Value: int32(1)}},
				Chunks:   []mdb.ChunkInfo{{Shard: "a"}},
			},
		},
		{
			name:   "ranged no chunks",
			shInfo: &mdb.ShardingInfo{ShardKey: bson.D{{Key: "_id", Value: int32(1)}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ns := catalog.Namespace{Database: "db", Collection: "coll"}

			// These branches never touch the source/target clients, so nil is safe.
			require.NoError(t, presplit(t.Context(), nil, nil, ns, tt.shInfo, newShardSizes()))
		})
	}
}

func TestPairShards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  []string
		tgt  []string
		want map[string]string
	}{
		{
			name: "already sorted",
			src:  []string{"srcA", "srcB"},
			tgt:  []string{"tgtA", "tgtB"},
			want: map[string]string{"srcA": "tgtA", "srcB": "tgtB"},
		},
		{
			name: "unsorted inputs are sorted first",
			src:  []string{"rs1", "rs0", "config"},
			tgt:  []string{"shardC", "shardA", "shardB"},
			want: map[string]string{"config": "shardA", "rs0": "shardB", "rs1": "shardC"},
		},
		{
			name: "single shard",
			src:  []string{"only"},
			tgt:  []string{"dst"},
			want: map[string]string{"only": "dst"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, pairShards(tt.src, tt.tgt))
		})
	}
}

// shardLoads sums the sizes assigned to each shard for a given assignment.
func shardLoads(assignment []string, sizes []int64) map[string]int64 {
	loads := map[string]int64{}
	for i, s := range assignment {
		loads[s] += sizes[i]
	}

	return loads
}

func TestAssignLargestFirst(t *testing.T) {
	t.Parallel()

	t.Run("skewed sizes pack evenly", func(t *testing.T) {
		t.Parallel()

		sizes := []int64{768, 256, 256, 256, 128}
		shards := []string{"s0", "s1", "s2"}

		assignment := newShardSizes().assignLargestFirst(sizes, shards)

		require.Len(t, assignment, len(sizes))
		// The jumbo (768) sits alone; the rest fill the other shards. Spread of
		// per-shard load is bounded by the largest chunk.
		loads := shardLoads(assignment, sizes)
		var minL, maxL int64 = 1 << 62, 0
		for _, s := range shards {
			minL = min(minL, loads[s])
			maxL = max(maxL, loads[s])
		}
		assert.LessOrEqual(t, maxL-minL, int64(768), "spread should not exceed the largest chunk")
	})

	t.Run("assignment preserves chunk order", func(t *testing.T) {
		t.Parallel()

		sizes := []int64{10, 100, 10}
		shards := []string{"a", "b"}

		assignment := newShardSizes().assignLargestFirst(sizes, shards)

		require.Len(t, assignment, 3)
		// Largest chunk (index 1) goes to the first-lightest shard; the two
		// small ones fill the other shard.
		assert.Equal(t, assignment[0], assignment[2])
		assert.NotEqual(t, assignment[1], assignment[0])
	})

	t.Run("cumulative weights spread jumbos across collections", func(t *testing.T) {
		t.Parallel()

		shards := []string{"s0", "s1"}
		w := newShardSizes()

		// Two collections, each with one jumbo. With cumulative weights the
		// second jumbo must not stack on the first jumbo's shard.
		a1 := w.assignLargestFirst([]int64{1000}, shards)
		a2 := w.assignLargestFirst([]int64{900}, shards)

		assert.NotEqual(t, a1[0], a2[0], "second jumbo stacked on the first jumbo's shard")
	})

	t.Run("single shard takes everything", func(t *testing.T) {
		t.Parallel()

		assignment := newShardSizes().assignLargestFirst([]int64{5, 3, 8}, []string{"only"})
		assert.Equal(t, []string{"only", "only", "only"}, assignment)
	})
}
