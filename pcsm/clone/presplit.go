package clone

import (
	"context"
	"slices"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/log"
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
func presplit(
	ctx context.Context,
	source, target *mongo.Client,
	ns catalog.Namespace,
	shInfo *mdb.ShardingInfo,
) error {
	switch {
	case isHashedPrefix(shInfo.ShardKey):
		// shardCollection already spreads hashed chunks evenly across shards,
		// so the default layout needs no pre-split.
		return nil

	case len(shInfo.Chunks) <= 1:
		// Ranged with no interior boundaries to replay.
		return nil

	default:
		return presplitRanged(ctx, source, target, ns, shInfo)
	}
}

// presplitRanged mirrors the source's ranged chunk layout onto the target:
// replay the source boundaries as splits, then move each chunk to the target
// shard paired with its source owner (both shard lists sorted).
//
// Only equal shard counts are handled; unequal counts (size-weighted placement)
// are skipped for now.
func presplitRanged(
	ctx context.Context,
	source, target *mongo.Client,
	ns catalog.Namespace,
	shInfo *mdb.ShardingInfo,
) error {
	lg := log.Ctx(ctx).With(log.NS(ns.Database, ns.Collection))

	srcShards, err := mdb.ListShards(ctx, source)
	if err != nil {
		return errors.Wrap(err, "list source shards")
	}

	tgtShards, err := mdb.ListShards(ctx, target)
	if err != nil {
		return errors.Wrap(err, "list target shards")
	}

	if len(srcShards) != len(tgtShards) {
		lg.Infof(
			"Skipping pre-split for %s: unequal shard counts (source %d, target %d)",
			ns.String(), len(srcShards), len(tgtShards),
		)

		return nil
	}

	pairing := pairShards(srcShards, tgtShards)
	nsStr := ns.String()

	// Split at every source boundary. A chunk's lower bound is its boundary;
	// the first chunk's is the collection minimum, not a split point.
	for _, chunk := range shInfo.Chunks[1:] {
		err = mdb.SplitChunkAt(ctx, target, nsStr, chunk.Min)
		if err != nil {
			return errors.Wrap(err, "split chunk")
		}
	}

	tgtInfo, err := mdb.GetCollectionShardingInfo(ctx, target, ns.Database, ns.Collection)
	if err != nil {
		return errors.Wrap(err, "get target sharding info")
	}

	if len(tgtInfo.Chunks) != len(shInfo.Chunks) {
		return errors.Errorf(
			"target has %d chunks after split, expected %d",
			len(tgtInfo.Chunks), len(shInfo.Chunks),
		)
	}

	moves := 0
	for i, tgtChunk := range tgtInfo.Chunks {
		dstShard := pairing[shInfo.Chunks[i].Shard]
		if tgtChunk.Shard == dstShard {
			continue
		}

		err = mdb.MoveChunk(ctx, target, nsStr, tgtChunk.Min, tgtChunk.Max, dstShard)
		if err != nil {
			return errors.Wrap(err, "move chunk")
		}

		moves++
	}

	lg.Infof(
		"Pre-split ranged collection %s: mirrored %d chunks across %d shards (%d moves)",
		nsStr, len(shInfo.Chunks), len(tgtShards), moves,
	)

	return nil
}

// pairShards maps each source shard to a target shard by pairing the two
// sorted lists index to index. The caller guarantees equal lengths.
func pairShards(srcShards, tgtShards []string) map[string]string {
	src := slices.Clone(srcShards)
	tgt := slices.Clone(tgtShards)
	slices.Sort(src)
	slices.Sort(tgt)

	pairing := make(map[string]string, len(src))
	for i, s := range src {
		pairing[s] = tgt[i]
	}

	return pairing
}
