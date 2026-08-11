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
	// multi-chunk (mirror/LPT) is not yet implemented and is covered by E2E.
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
			require.NoError(t, presplit(t.Context(), nil, nil, ns, tt.shInfo))
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
