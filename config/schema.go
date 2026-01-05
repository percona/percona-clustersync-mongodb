package config

import (
	"time"
)

// Config holds all PCSM configuration.
type Config struct {
	Port   int    `mapstructure:"port"`
	Source string `mapstructure:"source"`
	Target string `mapstructure:"target"`

	Log LogConfig `mapstructure:",squash"`

	MongoDB MongoDBConfig `mapstructure:",squash"`

	UseCollectionBulkWrite bool `mapstructure:"use-collection-bulk-write"`

	Clone CloneConfig `mapstructure:",squash"`

	// hidden startup flags
	Start              bool `mapstructure:"start"`
	ResetState         bool `mapstructure:"reset-state"`
	PauseOnInitialSync bool `mapstructure:"pause-on-initial-sync"`
}

// LogConfig holds logging configuration.
type LogConfig struct {
	Level   string `mapstructure:"log-level"`
	JSON    bool   `mapstructure:"log-json"`
	NoColor bool   `mapstructure:"no-color"`
}

// MongoDBConfig holds MongoDB client configuration.
type MongoDBConfig struct {
	OperationTimeout  time.Duration `mapstructure:"mongodb-operation-timeout"`
	TargetCompressors []string      `mapstructure:"dev-target-client-compressors"`
}

// CloneConfig holds clone operation configuration.
// These options can be set via environment variables and overridden by CLI flags or HTTP params.
type CloneConfig struct {
	// NumParallelCollections is the number of collections to clone in parallel.
	// 0 means auto (calculated at runtime).
	NumParallelCollections int `mapstructure:"clone-num-parallel-collections"`
	// NumReadWorkers is the number of read workers during clone.
	// 0 means auto (calculated at runtime).
	NumReadWorkers int `mapstructure:"clone-num-read-workers"`
	// NumInsertWorkers is the number of insert workers during clone.
	// 0 means auto (calculated at runtime).
	NumInsertWorkers int `mapstructure:"clone-num-insert-workers"`
	// SegmentSize is the segment size for clone operations (e.g., "500MB", "1GiB").
	// Empty string means auto (calculated at runtime for each collection).
	SegmentSize string `mapstructure:"clone-segment-size"`
	// ReadBatchSize is the read batch size during clone (e.g., "16MiB", "100MB").
	// Empty string means auto (calculated at runtime for each collection).
	ReadBatchSize string `mapstructure:"clone-read-batch-size"`
}
