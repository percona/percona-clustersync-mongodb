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
