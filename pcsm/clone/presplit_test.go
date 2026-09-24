//nolint:testpackage // tests unexported hasHashedField/presplit dispatch
package clone

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
)

func TestHasHashedField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		shardKey bson.D
		want     bool
	}{
		{"single hashed", bson.D{{Key: "_id", Value: "hashed"}}, true},
		{"compound hashed prefix", bson.D{{Key: "x", Value: "hashed"}, {Key: "region", Value: int32(1)}}, true},
		{"compound hashed suffix", bson.D{{Key: "region", Value: int32(1)}, {Key: "x", Value: "hashed"}}, true},
		{"single ranged", bson.D{{Key: "_id", Value: int32(1)}}, false},
		{"compound ranged", bson.D{{Key: "region", Value: int32(1)}, {Key: "x", Value: int32(1)}}, false},
		{"empty key", bson.D{}, false},
		{"nil key", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, hasHashedField(tt.shardKey))
		})
	}
}

func TestPresplitDispatchNoop(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		shInfo *mdb.ShardingInfo
	}{
		{
			name:   "hashed prefix",
			shInfo: &mdb.ShardingInfo{ShardKey: bson.D{{Key: "_id", Value: "hashed"}}},
		},
		{
			name: "hashed suffix with multiple chunks",
			shInfo: &mdb.ShardingInfo{
				ShardKey: bson.D{{Key: "region", Value: int32(1)}, {Key: "x", Value: "hashed"}},
				Chunks:   []mdb.ChunkInfo{{Shard: "a"}, {Shard: "b"}},
			},
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

func TestPresplitRangedEvenMissingSourceShard(t *testing.T) {
	t.Parallel()

	ns := catalog.Namespace{Database: "db", Collection: "coll"}

	// A shard removed after the chunk snapshot must fail before any target call;
	// the nil target client ensures no such call is made.
	shInfo := &mdb.ShardingInfo{
		ShardKey: bson.D{{Key: "_id", Value: int32(1)}},
		Chunks: []mdb.ChunkInfo{
			{Shard: "srcA"},
			{Shard: "srcC"},
		},
	}

	err := presplitRangedEven(
		t.Context(), nil, ns, shInfo,
		[]string{"srcA", "srcB"}, []string{"tgtA", "tgtB"},
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "srcC")
	assert.Contains(t, err.Error(), ns.String())
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
		assert.Equal(t, assignment[0], assignment[2])
		assert.NotEqual(t, assignment[1], assignment[0])
	})

	t.Run("cumulative weights spread jumbos across collections", func(t *testing.T) {
		t.Parallel()

		shards := []string{"s0", "s1"}
		w := newShardSizes()

		a1 := w.assignLargestFirst([]int64{1000}, shards)
		a2 := w.assignLargestFirst([]int64{900}, shards)

		assert.NotEqual(t, a1[0], a2[0], "second jumbo stacked on the first jumbo's shard")
	})

	t.Run("failed placement does not bias the next collection", func(t *testing.T) {
		t.Parallel()

		shards := []string{"s0", "s1"}
		w := newShardSizes()
		sizesA := []int64{1000}
		assignA := w.assignLargestFirst(sizesA, shards)

		// nil means the target layout could not be read.
		w.reconcile(sizesA, assignA, nil)

		assignB := w.assignLargestFirst([]int64{10}, shards)
		assert.Equal(t, assignA[0], assignB[0], "released reservation still biased placement")
	})

	t.Run("partial placement charges the shard the chunk is really on", func(t *testing.T) {
		t.Parallel()

		shards := []string{"s0", "s1"}
		w := newShardSizes()
		sizes := []int64{600, 400}
		assign := w.assignLargestFirst(sizes, shards)
		require.NotEqual(t, assign[0], assign[1])

		// The failed move leaves both chunks on the second shard.
		actual := []string{assign[1], assign[1]}
		w.reconcile(sizes, assign, actual)

		assert.Equal(t, int64(0), w.sizes[assign[0]], "unrealized reservation must be released")
		assert.Equal(t, int64(1000), w.sizes[assign[1]], "real owner must carry the chunk")
		assert.Equal(t, 0, w.counts[assign[0]], "unrealized chunk count must be released")
		assert.Equal(t, 2, w.counts[assign[1]], "real owner must count both chunks")
	})

	t.Run("single shard takes everything", func(t *testing.T) {
		t.Parallel()

		assignment := newShardSizes().assignLargestFirst([]int64{5, 3, 8}, []string{"only"})
		assert.Equal(t, []string{"only", "only", "only"}, assignment)
	})
}

// TestAssignLargestFirstZeroSizeChunks covers chunks whose estimate is zero:
// an empty collection, or one whose data sits in a few ranges while the rest
// were emptied by deletes. Such chunks add nothing to the running per-shard
// byte total, so they leave the lightest-shard comparison tied and only the
// chunk count keeps them from all landing on the same shard.
func TestAssignLargestFirstZeroSizeChunks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		collections [][]int64
		shards      []string
		evenCounts  bool
	}{
		{
			name:        "empty collection spreads over every shard",
			collections: [][]int64{{0, 0, 0, 0, 0, 0}},
			shards:      []string{"s0", "s1", "s2"},
			evenCounts:  true,
		},
		{
			name:        "one loaded range does not strand the empty ones",
			collections: [][]int64{{0, 0, 0, 0, 0, 5_000_000}},
			shards:      []string{"s0", "s1", "s2"},
		},
		{
			name:        "empty collections spread across a run",
			collections: [][]int64{{0, 0}, {0, 0}, {0, 0}, {0, 0}},
			shards:      []string{"s0", "s1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := newShardSizes()
			counts := map[string]int{}
			total := 0

			for _, sizes := range tt.collections {
				for _, shard := range w.assignLargestFirst(sizes, tt.shards) {
					counts[shard]++
				}

				total += len(sizes)
			}

			assert.Len(t, counts, len(tt.shards),
				"chunks landed on %d of %d target shards: %v", len(counts), len(tt.shards), counts)

			if !tt.evenCounts {
				return
			}

			minC, maxC := total, 0
			for _, s := range tt.shards {
				minC = min(minC, counts[s])
				maxC = max(maxC, counts[s])
			}

			assert.LessOrEqual(t, maxC-minC, 1,
				"uneven chunk counts for equally sized chunks: %v", counts)
		})
	}
}
