package topo

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona-lab/percona-mongolink/errors"
)

// errMissedClusterTime is returned when the cluster time is missing.
var errMissedClusterTime = errors.New("missed clusterTime")

// ClusterTime retrieves the cluster time from the MongoDB client.
func ClusterTime(ctx context.Context, m *mongo.Client) (bson.Timestamp, error) {
	raw, err := m.Database("admin").RunCommand(ctx, bson.D{{"hello", 1}}).Raw()
	if err != nil {
		return bson.Timestamp{}, err //nolint:wrapcheck
	}

	t, i, ok := raw.Lookup("$clusterTime", "clusterTime").TimestampOK()
	if !ok {
		return bson.Timestamp{}, errMissedClusterTime
	}

	return bson.Timestamp{T: t, I: i}, nil
}
