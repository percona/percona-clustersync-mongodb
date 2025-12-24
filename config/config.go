// Package config provides configuration management for PCSM using Viper.
package config

import (
	"os"
	"slices"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/validate"
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

	err = validate.Struct(&cfg)
	if err != nil {
		return nil, errors.Wrap(err, "validate config")
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
	_ = viper.BindEnv("mongodb-cli-operation-timeout", "PCSM_MONGODB_CLI_OPERATION_TIMEOUT")

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
