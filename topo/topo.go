package topo

import (
	"context"
	"time"

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

// Hello represents the result of the db.hello() command.
type Hello struct {
	// IsWritablePrimary indicates if the node is writable primary.
	IsWritablePrimary bool `bson:"isWritablePrimary"`
	// MaxBsonObjectSize is the maximum BSON object size supported by the server.
	MaxBsonObjectSize int32 `bson:"maxBsonObjectSize"`
	// MaxMessageSizeBytes is the maximum message size supported by the server.
	MaxMessageSizeBytes int32 `bson:"maxMessageSizeBytes"`
	// MaxWriteBatchSize is the maximum write batch size supported by the server.
	MaxWriteBatchSize int32 `bson:"maxWriteBatchSize"`
	// LocalTime is the server's local time.
	LocalTime time.Time `bson:"localTime"`
	// LogicalSessionTimeoutMinutes is the logical session timeout in minutes.
	LogicalSessionTimeoutMinutes int32 `bson:"logicalSessionTimeoutMinutes"`
	// ConnectionID is the connection ID of the server.
	ConnectionID int32 `bson:"connectionId"`
	// MinWireVersion is the minimum wire protocol version supported by the server.
	MinWireVersion int32 `bson:"minWireVersion"`
	// MaxWireVersion is the maximum wire protocol version supported by the server.
	MaxWireVersion int32 `bson:"maxWireVersion"`
	// ReadOnly indicates if the server is in read-only mode.
	ReadOnly bool `bson:"readOnly"`
	// SetName is the name of the replica set.
	SetName string `bson:"setName"`
	// SetVersion is the version of the replica set.
	SetVersion int32 `bson:"setVersion"`
	// ElectionID is the election ID of the primary.
	ElectionID bson.ObjectID `bson:"electionId"`
	// Primary is the address of the primary node.
	Primary string `bson:"primary"`
	// Secondary indicates if the node is a secondary.
	Secondary bool `bson:"secondary"`
	// ArbiterOnly indicates if the node is an arbiter.
	ArbiterOnly bool `bson:"arbiterOnly"`
	// Hidden indicates if the node is hidden.
	Hidden bool `bson:"hidden"`
	// Passive indicates if the node is passive.
	Passive bool `bson:"passive"`
	// Tags are the tags associated with the node.
	Tags bson.M `bson:"tags"`
	// Me is the address of the node.
	Me string `bson:"me"`
}

// DBStats represents the result of the dbStats command.
type DBStats struct {
	// DB is the name of the database.
	DB string `bson:"db"`
	// Collections is the number of collections in the database.
	Collections int64 `bson:"collections"`
	// Views is the number of views in the database.
	Views int64 `bson:"views"`
	// Objects is the number of objects in the database.
	Objects int64 `bson:"objects"`
	// AvgObjSize is the average size of objects in the database.
	AvgObjSize float64 `bson:"avgObjSize"`
	// DataSize is the total size of data in the database.
	DataSize int64 `bson:"dataSize"`
	// StorageSize is the total size of storage used by the database.
	StorageSize int64 `bson:"storageSize"`
	// Indexes is the number of indexes in the database.
	Indexes int64 `bson:"indexes"`
	// IndexSize is the total size of indexes in the database.
	IndexSize int64 `bson:"indexSize"`
	// TotalSize is the total size of the database.
	TotalSize int64 `bson:"totalSize"`
}

// CollStats represents the result of the collStats command.
type CollStats struct {
	// Namespace is the full namespace of the collection.
	Namespace string `bson:"ns"`
	// Count is the number of documents in the collection.
	Count int64 `bson:"count"`
	// Size is the total size of the collection.
	Size int64 `bson:"size"`
	// AvgObjSize is the average size of documents in the collection.
	AvgObjSize int64 `bson:"avgObjSize"`
	// StorageSize is the total storage size of the collection.
	StorageSize int64 `bson:"storageSize"`
	// TotalIndexSize is the total size of indexes in the collection.
	TotalIndexSize int64 `bson:"totalIndexSize"`
	// TotalSize is the total size of the collection including indexes.
	TotalSize int64 `bson:"totalSize"`
}

// SayHello runs the db.hello() command and returns the result as a Hello struct.
func SayHello(ctx context.Context, m *mongo.Client) (*Hello, error) {
	var result *Hello

	err := m.Database("admin").RunCommand(ctx, bson.D{{"hello", 1}}).Decode(&result)

	return result, err //nolint:wrapcheck
}

// GetDBStats runs the dbStats command and returns the result as a DBStats struct.
func GetDBStats(ctx context.Context, m *mongo.Client, dbName string) (*DBStats, error) {
	var result *DBStats

	err := m.Database(dbName).RunCommand(ctx, bson.D{{"dbStats", 1}}).Decode(&result)

	return result, err //nolint:wrapcheck
}

// GetCollStats runs the collStats command and returns the result as a CollStats struct.
func GetCollStats(ctx context.Context, m *mongo.Client, dbName, collName string) (*CollStats, error) {
	var result *CollStats

	err := m.Database(dbName).RunCommand(ctx, bson.D{{"collStats", collName}}).Decode(&result)

	return result, err //nolint:wrapcheck
}
