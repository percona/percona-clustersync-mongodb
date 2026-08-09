package clone

import (
	"context"
	"math"
	"math/big"

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

// hashChunkCount returns how many chunks to create for a hashed collection:
// max(N, ceil(srcChunks/N)*N). The result is always a multiple of N (so chunks
// divide evenly across shards) and never less than N (so every shard gets one).
func hashChunkCount(nShards, srcChunks int) int {
	if nShards <= 0 {
		return 0
	}

	// ceil(srcChunks/nShards) * nShards
	rounded := ((srcChunks + nShards - 1) / nShards) * nShards

	return max(nShards, rounded)
}

// interiorHashPoints returns pieces-1 evenly spaced split points inside the chunk
// bounded by [minBound, maxBound) along the hashed prefix field. Trailing
// shard-key fields are padded with MinKey.
func interiorHashPoints(shardKey bson.D, minBound, maxBound bson.D, pieces int) ([]bson.D, error) {
	if pieces <= 1 || len(shardKey) == 0 {
		return nil, nil
	}

	lo, err := hashBoundValue(minBound, math.MinInt64)
	if err != nil {
		return nil, err
	}

	hi, err := hashBoundValue(maxBound, math.MaxInt64)
	if err != nil {
		return nil, err
	}

	loBig := big.NewInt(lo)
	width := new(big.Int).Sub(big.NewInt(hi), loBig)
	step := new(big.Int).Div(width, big.NewInt(int64(pieces)))

	leadField := shardKey[0].Key

	points := make([]bson.D, 0, pieces-1)
	for i := 1; i < pieces; i++ {
		offset := new(big.Int).Mul(step, big.NewInt(int64(i)))
		val := new(big.Int).Add(loBig, offset)

		point := bson.D{{Key: leadField, Value: val.Int64()}}
		for _, e := range shardKey[1:] {
			point = append(point, bson.E{Key: e.Key, Value: bson.MinKey{}})
		}

		points = append(points, point)
	}

	return points, nil
}

// hashBoundValue extracts the leading hashed-field value from a chunk boundary
// as an int64. MinKey and MaxKey map to fallback (the space's min or max).
func hashBoundValue(bound bson.D, fallback int64) (int64, error) {
	if len(bound) == 0 {
		return fallback, nil
	}

	switch v := bound[0].Value.(type) {
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case bson.MinKey:
		return math.MinInt64, nil
	case bson.MaxKey:
		return math.MaxInt64, nil
	default:
		return 0, errors.Errorf("unexpected hashed boundary type %T", bound[0].Value)
	}
}

// presplit recreates a balanced chunk layout on the empty target collection so
// that clone writes spread across all shards instead of piling onto one. It runs
// after ShardCollection has succeeded, which implies the target is a mongos.
func presplit(ctx context.Context, target *mongo.Client, ns catalog.Namespace, shInfo *mdb.ShardingInfo) error {
	switch {
	case isHashedPrefix(shInfo.ShardKey):
		return presplitHashed(ctx, target, ns, shInfo)

	case len(shInfo.Chunks) <= 1:
		// Ranged with no interior boundaries to replay.
		return nil

	default:
		// Ranged with boundaries to replay: mirror (equal shard counts) or
		// dataSize + LPT (unequal). Not yet implemented.
		return nil
	}
}

// presplitHashed lays down an even hash-space chunk layout on the target,
// ignoring the source's chunk boundaries. Hashed sharding spreads documents
// uniformly across the hash space, so equal-width chunks hold equal data.
// The number of chunks is max(N, ceil(srcChunks/N)*N).
//
// Examples (N = target shard count, src = source chunk count):
//
//	src=3, N=3 -> max(3, 1*3) = 3 chunks  (1 per shard; equal shard counts)
//	src=6, N=3 -> max(3, 2*3) = 6 chunks  (2 per shard; equal shard counts)
//	src=6, N=5 -> max(5, 2*5) = 10 chunks (2 per shard)
//	src=3, N=5 -> max(5, 1*5) = 5 chunks  (1 per shard)
//	src=8, N=3 -> max(3, 3*3) = 9 chunks  (3 per shard)
//	src=1, N=4 -> max(4, 1*4) = 4 chunks  (1 per shard)
//
// Rounding up to a multiple of N keeps per-shard ownership even; the max(N, ...)
// floor guarantees at least one chunk per shard.
func presplitHashed(
	ctx context.Context,
	target *mongo.Client,
	ns catalog.Namespace,
	srcShInfo *mdb.ShardingInfo,
) error {
	lg := log.Ctx(ctx).With(log.NS(ns.Database, ns.Collection))

	shards, err := mdb.ListShards(ctx, target)
	if err != nil {
		return errors.Wrap(err, "list target shards")
	}

	numShards := len(shards)
	if numShards == 0 {
		return errors.New("no target shards found")
	}

	numChunks := hashChunkCount(numShards, len(srcShInfo.Chunks))

	// config.chunks is keyed by collection UUID, which differs between source and
	// target, so read the target's own UUID to fetch its chunks.
	tgtInfo, err := mdb.GetCollectionShardingInfo(ctx, target, ns.Database, ns.Collection)
	if err != nil {
		return errors.Wrap(err, "get target sharding info")
	}

	chunks, err := mdb.GetChunks(ctx, target, tgtInfo.UUID)
	if err != nil {
		return errors.Wrap(err, "read target chunks")
	}

	perShard := numChunks / numShards

	// shardCollection already places one native chunk per shard, evenly spread
	// across the hash space. Subdividing each in place into perShard pieces keeps
	// that even ownership without moving chunks (moves would trigger cross-shard
	// migration and its orphan-cleanup races).
	splits, err := subdivideChunksInPlace(ctx, target, ns.String(), chunks, srcShInfo.ShardKey, perShard)
	if err != nil {
		return err
	}

	lg.Infof(
		"Pre-split hashed collection %s into ~%d chunks across %d shards (%d splits)",
		ns.String(), numChunks, numShards, splits,
	)

	return nil
}

// subdivideChunksInPlace splits each existing chunk of ns into perShard
// equal-width pieces along the hashed prefix, leaving ownership unchanged.
// Returns the number of split commands issued.
func subdivideChunksInPlace(
	ctx context.Context,
	target *mongo.Client,
	ns string,
	chunks []mdb.ChunkInfo,
	shardKey bson.D,
	perShard int,
) (int, error) {
	if perShard <= 1 {
		return 0, nil
	}

	splits := 0
	for _, chunk := range chunks {
		points, err := interiorHashPoints(shardKey, chunk.Min, chunk.Max, perShard)
		if err != nil {
			return splits, errors.Wrap(err, "compute interior points")
		}

		for _, point := range points {
			err = mdb.SplitChunkAt(ctx, target, ns, point)
			if err != nil {
				return splits, errors.Wrap(err, "split chunk")
			}

			splits++
		}
	}

	return splits, nil
}
