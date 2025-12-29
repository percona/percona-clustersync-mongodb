package config

import (
	"math"
	"time"

	"github.com/dustin/go-humanize"
)

// Config holds all PCSM configuration.
type Config struct {
	// Connection
	Port   int    `mapstructure:"port"`
	Source string `mapstructure:"source"`
	Target string `mapstructure:"target"`

	// Logging (squash keeps flat keys)
	Log LogConfig `mapstructure:",squash"`

	// MongoDB client options
	MongoDB MongoDBConfig `mapstructure:",squash"`

	// Clone tuning (CLI/HTTP only)
	Clone CloneConfig `mapstructure:",squash"`

	// Internal options
	UseCollectionBulkWrite bool `mapstructure:"use-collection-bulk-write"`

	// Hidden startup flags
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
	OperationTimeout string `mapstructure:"mongodb-operation-timeout"`
}

// OperationTimeoutDuration returns the parsed timeout or default.
func (m *MongoDBConfig) OperationTimeoutDuration() time.Duration {
	if m.OperationTimeout != "" {
		d, err := time.ParseDuration(m.OperationTimeout)
		if err == nil && d > 0 {
			return d
		}
	}

	return DefaultMongoDBOperationTimeout
}

// CloneConfig holds clone tuning configuration.
type CloneConfig struct {
	NumParallelCollections int    `mapstructure:"clone-num-parallel-collections"`
	NumReadWorkers         int    `mapstructure:"clone-num-read-workers"`
	NumInsertWorkers       int    `mapstructure:"clone-num-insert-workers"`
	SegmentSize            string `mapstructure:"clone-segment-size"`
	ReadBatchSize          string `mapstructure:"clone-read-batch-size"`
}

// SegmentSizeBytes parses and returns the segment size in bytes.
func (c *CloneConfig) SegmentSizeBytes() int64 {
	if c.SegmentSize == "" {
		return AutoCloneSegmentSize
	}

	bytes, err := humanize.ParseBytes(c.SegmentSize)
	if err == nil && bytes > 0 {
		return int64(min(bytes, math.MaxInt64)) //nolint:gosec
	}

	return AutoCloneSegmentSize
}

// ReadBatchSizeBytes parses and returns the read batch size in bytes.
func (c *CloneConfig) ReadBatchSizeBytes() int32 {
	if c.ReadBatchSize == "" {
		return 0
	}

	bytes, err := humanize.ParseBytes(c.ReadBatchSize)
	if err == nil {
		return int32(min(bytes, math.MaxInt32)) //nolint:gosec
	}

	return 0
}
