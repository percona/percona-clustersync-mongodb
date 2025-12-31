// Package config provides configuration management for PCSM using Viper.
package config

import (
	"math"
	"slices"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/go-viper/mapstructure/v2"
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

	err := viper.Unmarshal(
		&cfg,
		viper.DecodeHook(mapstructure.StringToSliceHookFunc(",")),
	)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal config")
	}

	cfg.MongoDB.TargetCompressors = filterCompressors(cfg.MongoDB.TargetCompressors)

	return &cfg, nil
}

func bindEnvVars() {
	_ = viper.BindEnv("port", "PCSM_PORT")

	_ = viper.BindEnv("source", "PCSM_SOURCE_URI")
	_ = viper.BindEnv("target", "PCSM_TARGET_URI")

	_ = viper.BindEnv("log-level", "PCSM_LOG_LEVEL")
	_ = viper.BindEnv("log-json", "PCSM_LOG_JSON")
	_ = viper.BindEnv("no-color", "PCSM_NO_COLOR")

	_ = viper.BindEnv("mongodb-operation-timeout", "PCSM_MONGODB_OPERATION_TIMEOUT")

	_ = viper.BindEnv("use-collection-bulk-write", "PCSM_USE_COLLECTION_BULK_WRITE")

	_ = viper.BindEnv("dev-target-client-compressors", "PCSM_DEV_TARGET_CLIENT_COMPRESSORS")
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
