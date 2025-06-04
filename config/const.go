package config

import (
	"math"
	"time"

	"github.com/dustin/go-humanize"
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
	// PLMDatabase is the name of the MongoDB database used by PLM.
	PLMDatabase = "percona_link_mongodb"
	// RecoveryCollection is the name of the collection used for recovery data.
	RecoveryCollection = "checkpoints"
	// HeartbeatCollection is the name of the collection used for heartbeats.
	HeartbeatCollection = "heartbeats"
)

// Recovery and heartbeat settings.
const (
	// RecoveryCheckpointingInternal is the interval for recovery checkpointing.
	RecoveryCheckpointingInternal = 15 * time.Second
	// HeartbeatInternal is the interval for heartbeats.
	HeartbeatInternal = 30 * time.Second
	// HeartbeatTimeout is the timeout duration for heartbeats.
	HeartbeatTimeout = 5 * time.Second
	// StaleHeartbeatDuration is the duration after which a heartbeat is considered stale.
	StaleHeartbeatDuration = HeartbeatInternal + HeartbeatTimeout
	// PingTimeout is the timeout duration for initial ping.
	PingTimeout = 10 * time.Second
	// DisconnectTimeout is the timeout duration for connection disconnection and close server.
	DisconnectTimeout = 5 * time.Second
	// CloseCursorTimeout is the timeout duration for closing cursor.
	CloseCursorTimeout = 10 * time.Second
	// OperationTimeout is the timeout duration for MonngoDB client operations like
	// insert, update, delete, etc.
	OperationTimeout = 5 * time.Minute
)

// Change stream and replication settings.
const (
	// ChangeStreamBatchSize is the batch size for MongoDB change streams.
	ChangeStreamBatchSize = 1000
	// ChangeStreamAwaitTime is the maximum amount of time to wait for new change event.
	ChangeStreamAwaitTime = time.Second
	// ReplQueueSize defines the buffer size of the internal channel used to transfer
	// events between the change stream read and the change replication.
	ReplQueueSize = ChangeStreamBatchSize
	// BulkOpsSize is the maximum number of operations in a bulk write.
	BulkOpsSize = ChangeStreamBatchSize
	// BulkOpsInterval is the maximum interval between bulk write operations.
	BulkOpsInterval = ChangeStreamAwaitTime
	// InitialSyncCheckInterval is the interval for checking the initial sync status.
	InitialSyncCheckInterval = 10 * time.Second
	// PrintLagTimeInterval is the interval at which the lag time is printed to the logs.
	PrintLagTimeInterval = InitialSyncCheckInterval
)

// https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/#standard-message-header
const (
	wireProtoMsgReserveSizeBytes = 512
	// MaxWriteBatchSizeBytes defines the max size in bytes for a write batch,
	// accounting for an estimated 512-byte wire protocol message header overhead.
	MaxWriteBatchSizeBytes = MaxMessageSizeBytes - wireProtoMsgReserveSizeBytes
)

const (
	// DefaultCloneNumParallelCollection defines the default number of collections
	// to clone in parallel during initial sync or cloning operations.
	DefaultCloneNumParallelCollection = 2

	// AutoCloneSegmentSize enables auto segment size per collection during cloning.
	AutoCloneSegmentSize = 0
	// MinCloneSegmentSizeBytes is the minimum allowed segment size for collection cloning,
	// set to ensure each segment can hold at least one write batch.
	MinCloneSegmentSizeBytes = 10 * MaxWriteBatchSizeBytes
	// MaxCloneSegmentSizeBytes is the maximum allowed segment size for collection cloning.
	MaxCloneSegmentSizeBytes = 64 * humanize.GiByte

	// DefaultCloneReadBatchSizeBytes defines the default read cursor batch size.
	DefaultCloneReadBatchSizeBytes = MaxWriteBatchSizeBytes
	// MinCloneReadBatchSizeBytes is the minimum allowed read cursor batch size.
	MinCloneReadBatchSizeBytes = MaxBSONSize
	// MaxCloneReadBatchSizeBytes is the maximum allowed read cursor batch size.
	MaxCloneReadBatchSizeBytes = math.MaxInt32

	// MaxInsertBatchSize defines the maximum number of documents that can be inserted in a single
	// batch insert operation.
	MaxInsertBatchSize = 10_000
	// MaxInsertBatchSizeBytes defines the maximum size in bytes of a single insert batch payload.
	MaxInsertBatchSizeBytes = MaxBSONSize
)

// MaxBSONSize is hardcoded maximum BSON document size. 16 mebibytes.
//
//	https://www.mongodb.com/docs/v8.0/reference/limits/#mongodb-limit-BSON-Document-Size
//	https://www.mongodb.com/docs/manual/reference/command/hello/#mongodb-data-hello.maxBsonObjectSize
//
// db.hello().maxBsonObjectSize => 16777216.
const MaxBSONSize = 16 * humanize.MiByte

// MaxMessageSizeBytes is the maximum permitted size of a BSON wire protocol message.
// The default value is 48000000 bytes.
//
//	https://www.mongodb.com/docs/v8.0/reference/command/hello/#mongodb-data-hello.maxMessageSizeBytes
//
// db.hello().maxMessageSizeBytes => 48000000.
const MaxMessageSizeBytes = 48 * humanize.MByte

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
