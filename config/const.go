package config

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MongoDB logging configuration constants.
const (
	// MongoLogEnabled indicates if MongoDB logging is enabled.
	MongoLogEnabled = false
	// MongoLogComponent specifies the MongoDB log component.
	MongoLogComponent = options.LogComponentAll
	// MongoLogLevel specifies the MongoDB log level.
	MongoLogLevel = options.LogLevelInfo
)

// MongoDB database and collection names.
const (
	// MongoLinkDatabase is the name of the MongoDB database used by MongoLink.
	MongoLinkDatabase = "percona_mongolink"
	// TickCollection is the name of the collection used for ticks.
	TickCollection = "tick"
)

// Change stream and replication settings.
const (
	// ChangeStreamBatchSize is the batch size for MongoDB change streams.
	ChangeStreamBatchSize = 100

	// ReplTickInteral is the interval for replication ticks.
	ReplTickInteral = time.Second

	// ReplInitialSyncCheckInterval is the interval for initial sync checks.
	ReplInitialSyncCheckInterval = time.Second
)
