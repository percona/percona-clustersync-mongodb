// Package config provides configuration management for PCSM using Viper.
package config

import (
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Init initializes Viper configuration binding with Cobra command flags.
// This should be called in PersistentPreRun to ensure all flags are bound before use.
func Init(cmd *cobra.Command) {
	// Set environment variable prefix
	viper.SetEnvPrefix("PCSM")

	// Replace hyphens with underscores for env var lookup
	// e.g., "log-level" -> "PCSM_LOG_LEVEL"
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	// Automatically read matching env vars
	viper.AutomaticEnv()

	// Bind CLI flags to Viper
	if cmd.PersistentFlags() != nil {
		_ = viper.BindPFlags(cmd.PersistentFlags())
	}
	if cmd.Flags() != nil {
		_ = viper.BindPFlags(cmd.Flags())
	}

	// Bind specific env var names
	bindEnvVars()
}

// bindEnvVars binds environment variable names to Viper keys.
//
// Configuration Reference:
//
//	| Viper Key                      | CLI Flag                          | Env Var                              | Default |
//	|--------------------------------|-----------------------------------|--------------------------------------|---------|
//	| source                         | --source                          | PCSM_SOURCE_URI                      | -       |
//	| target                         | --target                          | PCSM_TARGET_URI                      | -       |
//	| port                           | --port                            | PCSM_PORT                            | 2242    |
//	| log-level                      | --log-level                       | PCSM_LOG_LEVEL                       | info    |
//	| log-json                       | --log-json                        | PCSM_LOG_JSON                        | false   |
//	| no-color                       | --no-color                        | PCSM_NO_COLOR                        | false   |
//	| mongodb-cli-operation-timeout  | --mongodb-cli-operation-timeout   | PCSM_MONGODB_CLI_OPERATION_TIMEOUT   | 5m      |
//	| use-collection-bulk-write      | --use-collection-bulk-write       | PCSM_USE_COLLECTION_BULK_WRITE       | false   |
//	| clone-num-parallel-collections | --clone-num-parallel-collections  | -                                    | 0       |
//	| clone-num-read-workers         | --clone-num-read-workers          | -                                    | 0       |
//	| clone-num-insert-workers       | --clone-num-insert-workers        | -                                    | 0       |
//	| clone-segment-size             | --clone-segment-size              | -                                    | 0       |
//	| clone-read-batch-size          | --clone-read-batch-size           | -                                    | 0       |
//
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

// GetString returns a string configuration value from Viper.
func GetString(key string) string {
	return viper.GetString(key)
}

// GetInt returns an integer configuration value from Viper.
func GetInt(key string) int {
	return viper.GetInt(key)
}

// GetBool returns a boolean configuration value from Viper.
func GetBool(key string) bool {
	return viper.GetBool(key)
}

// IsSet checks if a configuration key has been set.
func IsSet(key string) bool {
	return viper.IsSet(key)
}
