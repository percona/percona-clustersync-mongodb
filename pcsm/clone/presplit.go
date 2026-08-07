package clone

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"

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

// presplit recreates a balanced chunk layout on the target collection while it is still empty.
func (c *Clone) presplit(ctx context.Context, ns catalog.Namespace, shInfo *mdb.ShardingInfo) error {
	switch {
	case isHashedPrefix(shInfo.ShardKey):
		// Stage 4: even hash layout (source chunks ignored).
		return nil

	case len(shInfo.Chunks) <= 1:
		// Ranged with no interior boundaries to replay.
		return nil

	default:
		// Stage 5: mirror (equal shard counts).
		// Stage 6: dataSize + LPT (unequal shard counts).
		return nil
	}
}
