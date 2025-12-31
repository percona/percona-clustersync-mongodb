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
	_ = viper.BindEnv("port", "PCSM_PORT")

	_ = viper.BindEnv("source", "PCSM_SOURCE_URI")
	_ = viper.BindEnv("target", "PCSM_TARGET_URI")

	_ = viper.BindEnv("log-level", "PCSM_LOG_LEVEL")
	_ = viper.BindEnv("log-json", "PCSM_LOG_JSON")
	_ = viper.BindEnv("no-color", "PCSM_NO_COLOR")

	_ = viper.BindEnv("mongodb-operation-timeout", "PCSM_MONGODB_OPERATION_TIMEOUT")

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
// It allows 0 (auto) or values within [MinCloneReadBatchSizeBytes, MaxCloneReadBatchSizeBytes].
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
