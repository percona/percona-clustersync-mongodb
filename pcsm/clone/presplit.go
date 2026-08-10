package clone

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
)

// hashedKeyType is the shard-key field value that marks a hashed key.
const hashedKeyType = "hashed"

// isHashedPrefix reports whether the shard key's leading field is hashed.
func isHashedPrefix(shardKey bson.D) bool {
	if len(shardKey) == 0 {
		return false
	}

	v, ok := shardKey[0].Value.(string)

	return ok && v == hashedKeyType
}

// presplit recreates a balanced chunk layout on the empty target collection so
// that clone writes spread across all shards instead of piling onto one. It runs
// after ShardCollection has succeeded, which implies the target is a mongos.
//
//nolint:unparam // ctx/target are used once the ranged mirror/LPT paths land.
func presplit(ctx context.Context, target *mongo.Client, ns catalog.Namespace, shInfo *mdb.ShardingInfo) error {
	switch {
	case isHashedPrefix(shInfo.ShardKey):
		// Hashed sharding spreads documents uniformly across the hash space, and
		// shardCollection already distributes native chunks evenly across all
		// shards (on every supported version and shard count). The default layout
		// is already balanced, so there is nothing to pre-split.
		// 	For example, with 3 shards:
		// - MongoDB 6.0/7.0: 6 chunks, approximately 2 on each shard.
		// - MongoDB 8.0: 3 chunks, approximately 1 on each shard.
		return nil

	case len(shInfo.Chunks) <= 1:
		// Ranged with no interior boundaries to replay.
		return nil

	default:
		// Ranged with boundaries to replay: mirror (equal shard counts) or
		// dataSize + LPT (unequal). Not yet implemented.
		return nil
	}
}
