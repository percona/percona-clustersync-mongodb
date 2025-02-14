package topo

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona-lab/percona-mongolink/errors"
)

var errMissedClusterTime = errors.New("missed clusterTime")

func ClusterTime(ctx context.Context, m *mongo.Client) (primitive.Timestamp, error) {
	raw, err := m.Database("admin").RunCommand(ctx, bson.D{{"hello", 1}}).Raw()
	if err != nil {
		return primitive.Timestamp{}, err //nolint:wrapcheck
	}

	t, i, ok := raw.Lookup("$clusterTime", "clusterTime").TimestampOK()
	if !ok {
		return primitive.Timestamp{}, errMissedClusterTime
	}

	return primitive.Timestamp{T: t, I: i}, nil
}
