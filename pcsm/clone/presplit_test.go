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

	// Stage 3 skeleton: every dispatch branch is a no-op that returns nil.
	// A zero-value Clone is sufficient because no branch touches its fields yet.
	tests := []struct {
		name   string
		shInfo *mdb.ShardingInfo
	}{
		{
			name:   "hashed prefix",
			shInfo: &mdb.ShardingInfo{ShardKey: bson.D{{Key: "_id", Value: "hashed"}}},
		},
		{
			name: "ranged multiple chunks",
			shInfo: &mdb.ShardingInfo{
				ShardKey: bson.D{{Key: "_id", Value: int32(1)}},
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

			c := &Clone{}
			ns := catalog.Namespace{Database: "db", Collection: "coll"}

			require.NoError(t, c.presplit(t.Context(), ns, tt.shInfo))
		})
	}
}
