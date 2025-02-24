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

// CloneMaxWriteSizePerCollection is the maximum write size per collection during the cloning process.
const CloneMaxWriteSizePerCollection = 100 * MB

// MaxBSONSize is hardcoded maximum BSON document size. 16 mebibytes.
//
//	https://www.mongodb.com/docs/v8.0/reference/limits/#mongodb-limit-BSON-Document-Size
//	https://www.mongodb.com/docs/manual/reference/command/hello/#mongodb-data-hello.maxBsonObjectSize
//
// db.hello().maxBsonObjectSize => 16777216.
const MaxBSONSize = 16 * MiB

// MaxMessageSizeBytes is the maximum permitted size of a BSON wire protocol message.
// The default value is 48000000 bytes.
//
//	https://www.mongodb.com/docs/v8.0/reference/command/hello/#mongodb-data-hello.maxMessageSizeBytes
//
// db.hello().maxMessageSizeBytes => 48000000.
const MaxMessageSizeBytes = 48 * MB

// MaxWriteBatchSize is the maximum number of write operations permitted in a write batch.
// If a batch exceeds this limit, the client driver divides the batch into smaller groups each with
// counts less than or equal to the value of this field.
//
// The value of this limit is 100,000 writes.
//
//	https://www.mongodb.com/docs/manual/reference/command/hello/#mongodb-data-hello.maxWriteBatchSize
//
// db.hello().maxWriteBatchSize => 100000.
const MaxWriteBatchSize = 100_000

// Size constants for data storage.
const (
	KB = 1000
	MB = 1000 * KB
	GB = 1000 * MB
	TB = 1000 * GB

	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
	TiB = 1024 * GiB
)
