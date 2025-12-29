// Package config provides configuration management for PCSM using Viper.
package config

import (
	"math"
	"os"
	"slices"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/percona/percona-clustersync-mongodb/errors"
)

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

	err := viper.Unmarshal(&cfg)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal config")
	}

	return &cfg, nil
}

// bindEnvVars binds environment variable names to Viper keys.
// Note: Clone tuning options are CLI/HTTP only (no env var support).
func bindEnvVars() {
	// Server connection URIs
	_ = viper.BindEnv("source", "PCSM_SOURCE_URI")
	_ = viper.BindEnv("target", "PCSM_TARGET_URI")

	// MongoDB client timeout
	_ = viper.BindEnv("mongodb-operation-timeout", "PCSM_MONGODB_OPERATION_TIMEOUT")

	// Bulk write option (hidden, internal use)
	_ = viper.BindEnv("use-collection-bulk-write", "PCSM_USE_COLLECTION_BULK_WRITE")
}

// UseTargetClientCompressors returns a list of enabled compressors (from "zstd", "zlib", "snappy")
// for the target MongoDB client connection, as specified by the comma-separated environment
// variable PCSM_DEV_TARGET_CLIENT_COMPRESSORS. If unset or empty, returns nil.
func UseTargetClientCompressors() []string {
	s := strings.TrimSpace(os.Getenv("PCSM_DEV_TARGET_CLIENT_COMPRESSORS"))
	if s == "" {
		return nil
	}

	allowCompressors := []string{"zstd", "zlib", "snappy"}

	rv := make([]string, 0, min(len(s), len(allowCompressors)))
	for a := range strings.SplitSeq(s, ",") {
		a = strings.TrimSpace(a)
		if slices.Contains(allowCompressors, a) && !slices.Contains(rv, a) {
			rv = append(rv, a)
		}
	}

	return rv
}

// ResolveCloneSegmentSize resolves the clone segment size from an optional HTTP value
// or falls back to the CLI config value. Both sources are validated.
func ResolveCloneSegmentSize(cfg *Config, value *string) (int64, error) {
	if value != nil {
		return parseAndValidateCloneSegmentSize(*value)
	}

	// Fall back to CLI config value and validate it
	sizeBytes := cfg.Clone.SegmentSizeBytes()

	err := ValidateCloneSegmentSize(uint64(max(sizeBytes, 0))) //nolint:gosec
	if err != nil {
		return 0, errors.Wrap(err, "config clone-segment-size")
	}

	return sizeBytes, nil
}

// ResolveCloneReadBatchSize resolves the clone read batch size from an optional HTTP value
// or falls back to the CLI config value. Both sources are validated.
func ResolveCloneReadBatchSize(cfg *Config, value *string) (int32, error) {
	if value != nil {
		return parseAndValidateCloneReadBatchSize(*value)
	}

	// Fall back to CLI config value and validate it
	sizeBytes := cfg.Clone.ReadBatchSizeBytes()

	err := ValidateCloneReadBatchSize(uint64(max(sizeBytes, 0))) //nolint:gosec
	if err != nil {
		return 0, errors.Wrap(err, "config clone-read-batch-size")
	}

	return sizeBytes, nil
}

// parseAndValidateCloneSegmentSize parses a byte size string and validates it.
func parseAndValidateCloneSegmentSize(value string) (int64, error) {
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

// parseAndValidateCloneReadBatchSize parses a byte size string and validates it.
func parseAndValidateCloneReadBatchSize(value string) (int32, error) {
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
