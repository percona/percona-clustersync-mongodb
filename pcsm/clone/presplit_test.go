//nolint:testpackage // tests unexported isHashedPrefix/presplit dispatch
package clone

import (
	"math"
	"math/big"
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

	// Only the ranged skip branches are permanent no-ops (nothing to replay).
	// The hashed and ranged-multi-chunk branches need live cluster access and
	// are covered by E2E tests.
	tests := []struct {
		name   string
		shInfo *mdb.ShardingInfo
	}{
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

			// The skip branches never touch the target client, so nil is safe.
			require.NoError(t, presplit(t.Context(), nil, ns, tt.shInfo))
		})
	}
}

func TestHashChunkCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		nShards   int
		srcChunks int
		want      int
	}{
		{"6 chunks 5 shards", 5, 6, 10},
		{"6 chunks 3 shards", 3, 6, 6},
		{"3 chunks 5 shards", 5, 3, 5},
		{"8 chunks 3 shards", 3, 8, 9},
		{"1 chunk 4 shards", 4, 1, 4},
		{"0 chunks 4 shards", 4, 0, 4},
		{"exact multiple", 4, 8, 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, hashChunkCount(tt.nShards, tt.srcChunks))
		})
	}
}

func TestInteriorHashPoints(t *testing.T) {
	t.Parallel()

	key := bson.D{{Key: "_id", Value: "hashed"}}

	t.Run("full-space chunk split into quarters", func(t *testing.T) {
		t.Parallel()

		minB := bson.D{{Key: "_id", Value: bson.MinKey{}}}
		maxB := bson.D{{Key: "_id", Value: bson.MaxKey{}}}

		points, err := interiorHashPoints(key, minB, maxB, 4)
		require.NoError(t, err)
		require.Len(t, points, 3)

		// MinKey/MaxKey map to int64 min/max, so width = 2^64-1 and
		// step = floor((2^64-1)/4).
		width := new(big.Int).Sub(big.NewInt(math.MaxInt64), big.NewInt(math.MinInt64))
		step := new(big.Int).Div(width, big.NewInt(4)).Int64()
		want := []int64{
			math.MinInt64 + 1*step,
			math.MinInt64 + 2*step,
			math.MinInt64 + 3*step,
		}
		for i, p := range points {
			require.Len(t, p, 1)
			assert.Equal(t, "_id", p[0].Key)
			assert.Equal(t, want[i], p[0].Value)
		}
	})

	t.Run("interior int64-bounded chunk split in half", func(t *testing.T) {
		t.Parallel()

		minB := bson.D{{Key: "_id", Value: int64(0)}}
		maxB := bson.D{{Key: "_id", Value: int64(100)}}

		points, err := interiorHashPoints(key, minB, maxB, 2)
		require.NoError(t, err)
		require.Len(t, points, 1)
		assert.Equal(t, int64(50), points[0][0].Value)
	})

	t.Run("compound hashed prefix pads with MinKey", func(t *testing.T) {
		t.Parallel()

		ckey := bson.D{{Key: "a", Value: "hashed"}, {Key: "b", Value: int32(1)}}
		minB := bson.D{{Key: "a", Value: int64(0)}, {Key: "b", Value: bson.MinKey{}}}
		maxB := bson.D{{Key: "a", Value: int64(100)}, {Key: "b", Value: bson.MinKey{}}}

		points, err := interiorHashPoints(ckey, minB, maxB, 2)
		require.NoError(t, err)
		require.Len(t, points, 1)
		require.Len(t, points[0], 2)

		assert.Equal(t, "a", points[0][0].Key)
		assert.Equal(t, int64(50), points[0][0].Value)
		assert.Equal(t, "b", points[0][1].Key)
		assert.IsType(t, bson.MinKey{}, points[0][1].Value)
	})

	t.Run("pieces <= 1 yields no points", func(t *testing.T) {
		t.Parallel()

		minB := bson.D{{Key: "_id", Value: bson.MinKey{}}}
		maxB := bson.D{{Key: "_id", Value: bson.MaxKey{}}}

		p1, err := interiorHashPoints(key, minB, maxB, 1)
		require.NoError(t, err)
		assert.Nil(t, p1)

		p0, err := interiorHashPoints(key, minB, maxB, 0)
		require.NoError(t, err)
		assert.Nil(t, p0)
	})
}
