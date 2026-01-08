// Package config provides configuration management for PCSM using Viper.
package config

import (
	"context"
	"math"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/log"
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
	NoColor bool   `mapstructure:"log-no-color"`
}

// MongoDBConfig holds MongoDB client configuration.
type MongoDBConfig struct {
	OperationTimeout  time.Duration `mapstructure:"mongodb-operation-timeout"`
	TargetCompressors []string      `mapstructure:"dev-target-client-compressors"`
}

// CloneConfig holds clone operation configuration.
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

// Load initializes Viper and returns a validated Config.
func Load(cmd *cobra.Command) (*Config, error) {
	viper.SetEnvPrefix("PCSM")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	if cmd.PersistentFlags() != nil {
		_ = viper.BindPFlags(cmd.PersistentFlags())
	}

	if cmd.Flags() != nil {
		_ = viper.BindPFlags(cmd.Flags())
	}

	bindEnvVars()

	var cfg Config

	err := viper.Unmarshal(&cfg, viper.DecodeHook(
		mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToSliceHookFunc(","),
		),
	))
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal config")
	}

	cfg.MongoDB.TargetCompressors = filterCompressors(cfg.MongoDB.TargetCompressors)

	if viper.GetBool("no-color") {
		cfg.Log.NoColor = true
	}

	return &cfg, nil
}

// WarnDeprecatedEnvVars logs warnings for any deprecated environment variables that are set.
// Expects the logger to be initialized.
func WarnDeprecatedEnvVars(ctx context.Context) {
	deprecated := map[string]string{
		"PLM_MONGODB_CLI_OPERATION_TIMEOUT": "PCSM_MONGODB_OPERATION_TIMEOUT",
	}

	for old, replacement := range deprecated {
		if _, ok := os.LookupEnv(old); ok {
			log.Ctx(ctx).Warnf(
				"Environment variable %s is deprecated; use %s instead",
				old, replacement,
			)
		}
	}
}

func bindEnvVars() {
	_ = viper.BindEnv("port", "PCSM_PORT")

	_ = viper.BindEnv("source", "PCSM_SOURCE_URI")
	_ = viper.BindEnv("target", "PCSM_TARGET_URI")

	_ = viper.BindEnv("log-level", "PCSM_LOG_LEVEL")
	_ = viper.BindEnv("log-json", "PCSM_LOG_JSON")
	_ = viper.BindEnv("log-no-color", "PCSM_LOG_NO_COLOR", "PCSM_NO_COLOR")

	_ = viper.BindEnv("mongodb-operation-timeout",
		"PCSM_MONGODB_OPERATION_TIMEOUT",
		"PLM_MONGODB_CLI_OPERATION_TIMEOUT", // deprecated
	)

	_ = viper.BindEnv("use-collection-bulk-write", "PCSM_USE_COLLECTION_BULK_WRITE")

	_ = viper.BindEnv("dev-target-client-compressors", "PCSM_DEV_TARGET_CLIENT_COMPRESSORS")

	_ = viper.BindEnv("clone-num-parallel-collections", "PCSM_CLONE_NUM_PARALLEL_COLLECTIONS")
	_ = viper.BindEnv("clone-num-read-workers", "PCSM_CLONE_NUM_READ_WORKERS")
	_ = viper.BindEnv("clone-num-insert-workers", "PCSM_CLONE_NUM_INSERT_WORKERS")
	_ = viper.BindEnv("clone-segment-size", "PCSM_CLONE_SEGMENT_SIZE")
	_ = viper.BindEnv("clone-read-batch-size", "PCSM_CLONE_READ_BATCH_SIZE")
}

//nolint:gochecknoglobals
var allowedCompressors = []string{"zstd", "zlib", "snappy"}

func filterCompressors(compressors []string) []string {
	if len(compressors) == 0 {
		return nil
	}

	filtered := make([]string, 0, len(allowedCompressors))

	for _, c := range compressors {
		c = strings.TrimSpace(c)
		if slices.Contains(allowedCompressors, c) && !slices.Contains(filtered, c) {
			filtered = append(filtered, c)
		}
	}

	return filtered
}

// ParseAndValidateCloneSegmentSize parses a byte size string and validates it.
// It allows 0 (auto) or values within [MinCloneSegmentSizeBytes, MaxCloneSegmentSizeBytes].
func ParseAndValidateCloneSegmentSize(value string) (int64, error) {
	sizeBytes, err := humanize.ParseBytes(value)
	if err != nil {
		return 0, errors.Wrapf(err, "invalid cloneSegmentSize value: %s", value)
	}

	err = ValidateCloneSegmentSize(sizeBytes)
	if err != nil {
		return 0, err
	}

	return int64(min(sizeBytes, math.MaxInt64)), nil //nolint:gosec
}

// ParseAndValidateCloneReadBatchSize parses a byte size string and validates it.
// It allows 0 (auto) or values within [[MinCloneReadBatchSizeBytes], [MaxCloneReadBatchSizeBytes]].
func ParseAndValidateCloneReadBatchSize(value string) (int32, error) {
	sizeBytes, err := humanize.ParseBytes(value)
	if err != nil {
		return 0, errors.Wrapf(err, "invalid cloneReadBatchSize value: %s", value)
	}

	err = ValidateCloneReadBatchSize(sizeBytes)
	if err != nil {
		return 0, err
	}

	return int32(min(sizeBytes, math.MaxInt32)), nil //nolint:gosec
}
