package clone

import (
	"context"
	"slices"
	"sort"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/log"
	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
)

// hashedKeyType is the shard-key field value that marks a hashed key.
const hashedKeyType = "hashed"

// presplitSizeWorkers bounds the parallel chunk dataSize estimates per collection.
const presplitSizeWorkers = 4

// shardSizes tracks the cumulative estimated bytes assigned to each target
// shard across all collections in a run.
type shardSizes struct {
	mu    sync.Mutex
	sizes map[string]int64
}

func newShardSizes() *shardSizes {
	return &shardSizes{sizes: make(map[string]int64)}
}

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
	targetShardSizes *shardSizes,
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
		srcShards, err := mdb.ListShards(ctx, source)
		if err != nil {
			return errors.Wrap(err, "list source shards")
		}

		tgtShards, err := mdb.ListShards(ctx, target)
		if err != nil {
			return errors.Wrap(err, "list target shards")
		}

		if len(srcShards) == len(tgtShards) {
			return presplitRangedEven(ctx, target, ns, shInfo, srcShards, tgtShards)
		}

		return presplitRangedUneven(ctx, source, target, ns, shInfo, tgtShards, targetShardSizes)
	}
}

// presplitRangedEven mirrors the source's ranged layout when source and target
// have the same number of shards: replay the source boundaries and move each
// chunk to the target shard paired with its source owner (both shard lists
// sorted, index to index).
func presplitRangedEven(
	ctx context.Context,
	target *mongo.Client,
	ns catalog.Namespace,
	shInfo *mdb.ShardingInfo,
	srcShards, tgtShards []string,
) error {
	pairing := pairShards(srcShards, tgtShards)

	assignment := make([]string, len(shInfo.Chunks))
	for i, chunk := range shInfo.Chunks {
		assignment[i] = pairing[chunk.Shard]
	}

	moves, err := replayAndPlace(ctx, target, ns, shInfo, assignment)
	if err != nil {
		return err
	}

	log.Ctx(ctx).With(log.NS(ns.Database, ns.Collection)).Infof(
		"Pre-split ranged collection %s: mirrored %d chunks across %d shards (%d moves)",
		ns.String(), len(shInfo.Chunks), len(tgtShards), moves,
	)

	return nil
}

// presplitRangedUneven handles unequal shard counts by packing chunks onto
// target shards by estimated size (largest chunk to the currently lightest
// shard), keeping per-shard data volume even. Cumulative sizes carry across
// collections so heavy chunks from different collections spread out.
func presplitRangedUneven(
	ctx context.Context,
	source, target *mongo.Client,
	ns catalog.Namespace,
	shInfo *mdb.ShardingInfo,
	tgtShards []string,
	targetShardSizes *shardSizes,
) error {
	chunkSizes, err := estimateChunkSizes(ctx, source, ns, shInfo)
	if err != nil {
		return err
	}

	assignment := targetShardSizes.assignLargestFirst(chunkSizes, tgtShards)

	moves, err := replayAndPlace(ctx, target, ns, shInfo, assignment)
	if err != nil {
		return err
	}

	log.Ctx(ctx).With(log.NS(ns.Database, ns.Collection)).Infof(
		"Pre-split ranged collection %s: size-weighted %d chunks across %d shards (%d moves)",
		ns.String(), len(shInfo.Chunks), len(tgtShards), moves,
	)

	return nil
}

// replayAndPlace splits the target at every source boundary, then moves each
// resulting chunk to its assigned shard (assignment is index-aligned with
// shInfo.Chunks). Returns the number of moves performed.
func replayAndPlace(
	ctx context.Context,
	target *mongo.Client,
	ns catalog.Namespace,
	shInfo *mdb.ShardingInfo,
	assignment []string,
) (int, error) {
	nsStr := ns.String()

	// Split at every source boundary. A chunk's lower bound is its boundary;
	// the first chunk's is the collection minimum, not a split point.
	for _, chunk := range shInfo.Chunks[1:] {
		err := mdb.SplitChunkAt(ctx, target, nsStr, chunk.Min)
		if err != nil {
			return 0, errors.Wrap(err, "split chunk")
		}
	}

	tgtInfo, err := mdb.GetCollectionShardingInfo(ctx, target, ns.Database, ns.Collection)
	if err != nil {
		return 0, errors.Wrap(err, "get target sharding info")
	}

	if len(tgtInfo.Chunks) != len(shInfo.Chunks) {
		return 0, errors.Errorf(
			"target has %d chunks after split, expected %d",
			len(tgtInfo.Chunks), len(shInfo.Chunks),
		)
	}

	moves := 0
	for i, tgtChunk := range tgtInfo.Chunks {
		dstShard := assignment[i]
		if tgtChunk.Shard == dstShard {
			continue
		}

		err = mdb.MoveChunk(ctx, target, nsStr, tgtChunk.Min, tgtChunk.Max, dstShard)
		if err != nil {
			return moves, errors.Wrap(err, "move chunk")
		}

		moves++
	}

	return moves, nil
}

// estimateChunkSizes returns the estimated byte size of each source chunk, in
// chunk order, using bounded parallelism.
func estimateChunkSizes(
	ctx context.Context,
	source *mongo.Client,
	ns catalog.Namespace,
	shInfo *mdb.ShardingInfo,
) ([]int64, error) {
	sizes := make([]int64, len(shInfo.Chunks))

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.SetLimit(presplitSizeWorkers)

	for i, chunk := range shInfo.Chunks {
		grp.Go(func() error {
			size, err := mdb.EstimateChunkSize(
				grpCtx, source, ns.String(), shInfo.ShardKey, chunk.Min, chunk.Max,
			)
			if err != nil {
				return errors.Wrap(err, "estimate chunk size")
			}

			sizes[i] = size

			return nil
		})
	}

	err := grp.Wait()
	if err != nil {
		return nil, errors.Wrap(err, "estimate chunk sizes")
	}

	return sizes, nil
}

// assignLargestFirst assigns chunks to target shards by processing the largest
// chunks first and placing each on the currently lightest shard. It returns the
// assignments in the original chunk order. Cumulative sizes are updated under
// the lock so concurrent collections share one running total.
func (s *shardSizes) assignLargestFirst(chunkSizes []int64, tgtShards []string) []string {
	order := make([]int, len(chunkSizes))
	for i := range order {
		order[i] = i
	}

	sort.SliceStable(order, func(a, b int) bool {
		return chunkSizes[order[a]] > chunkSizes[order[b]]
	})

	s.mu.Lock()
	defer s.mu.Unlock()

	assignment := make([]string, len(chunkSizes))
	for _, idx := range order {
		lightest := tgtShards[0]
		for _, shard := range tgtShards[1:] {
			if s.sizes[shard] < s.sizes[lightest] {
				lightest = shard
			}
		}

		assignment[idx] = lightest
		s.sizes[lightest] += chunkSizes[idx]
	}

	return assignment
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
