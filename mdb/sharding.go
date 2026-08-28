package mdb

import (
	"context"
	"slices"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-clustersync-mongodb/errors"
)

// ListShards returns the shard IDs of the sharded cluster, sorted.
// It must be run against a mongos.
func ListShards(ctx context.Context, m *mongo.Client) ([]string, error) {
	type shardEntry struct {
		ID string `bson:"_id"`
	}

	type listShardsResult struct {
		Shards []shardEntry `bson:"shards"`
	}

	res, err := RunWithRetryVal(ctx, func(ctx context.Context) (*listShardsResult, error) {
		out := &listShardsResult{}
		err := m.Database("admin").
			RunCommand(ctx, bson.D{{Key: "listShards", Value: 1}}).
			Decode(out)

		return out, err //nolint:wrapcheck
	}, DefaultRetryInterval, DefaultMaxRetries)
	if err != nil {
		return nil, errors.Wrap(err, "listShards")
	}

	shards := make([]string, 0, len(res.Shards))
	for _, s := range res.Shards {
		shards = append(shards, s.ID)
	}

	slices.Sort(shards)

	return shards, nil
}

// SplitChunkAt splits the chunk of ns containing the point `middle` into two
// chunks at that point. The point must lie strictly inside an existing chunk.
// Metadata-only. Must be run against a mongos.
func SplitChunkAt(ctx context.Context, m *mongo.Client, ns string, middle bson.D) error {
	err := RunWithRetry(ctx, func(ctx context.Context) error {
		cmdErr := m.Database("admin").RunCommand(ctx, bson.D{
			{Key: "split", Value: ns},
			{Key: "middle", Value: middle},
		}).Err()

		// Already a boundary means the split is effectively done (retry after an
		// ambiguous failure, or a re-run after resume): treat as success.
		if cmdErr != nil && IsSplitPointAlreadyChunkBoundary(cmdErr) {
			return nil
		}

		return cmdErr //nolint:wrapcheck
	}, DefaultRetryInterval, DefaultMaxRetries)
	if err != nil {
		return errors.Wrapf(err, "split %s", ns)
	}

	return nil
}

// MoveChunk moves the chunk of ns bounded by [minBound, maxBound) to the named
// target shard. For empty chunks this is a metadata-only operation. Must be run
// against a mongos.
func MoveChunk(
	ctx context.Context,
	m *mongo.Client,
	ns string,
	minBound, maxBound bson.D,
	toShard string,
) error {
	err := RunWithRetry(ctx, func(ctx context.Context) error {
		return m.Database("admin").RunCommand(ctx, bson.D{
			{Key: "moveChunk", Value: ns},
			{Key: "bounds", Value: bson.A{minBound, maxBound}},
			{Key: "to", Value: toShard},
		}).Err() //nolint:wrapcheck
	}, DefaultRetryInterval, DefaultMaxRetries)
	if err != nil {
		return errors.Wrapf(err, "moveChunk %s to %s", ns, toShard)
	}

	return nil
}

// EstimateChunkSize returns the estimated size in bytes of the chunk
// [minBound, maxBound) of ns, using the dataSize command with estimate:true
// (avgObjSize * key count — cheap and index-only). Must be run against a mongos.
func EstimateChunkSize(
	ctx context.Context,
	m *mongo.Client,
	ns string,
	keyPattern, minBound, maxBound bson.D,
) (int64, error) {
	type dataSizeResult struct {
		Size int64 `bson:"size"`
	}

	res, err := RunWithRetryVal(ctx, func(ctx context.Context) (*dataSizeResult, error) {
		out := &dataSizeResult{}
		err := m.Database("admin").RunCommand(ctx, bson.D{
			{Key: "dataSize", Value: ns},
			{Key: "keyPattern", Value: keyPattern},
			{Key: "min", Value: minBound},
			{Key: "max", Value: maxBound},
			{Key: "estimate", Value: true},
		}).Decode(out)

		return out, err //nolint:wrapcheck
	}, DefaultRetryInterval, DefaultMaxRetries)
	if err != nil {
		return 0, errors.Wrapf(err, "dataSize %s", ns)
	}

	return res.Size, nil
}
