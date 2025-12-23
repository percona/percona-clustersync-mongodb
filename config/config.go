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
// Only global/server options have env var support.
// Clone tuning options are intentionally NOT bound (CLI/HTTP only).
func bindEnvVars() {
	// Server connection URIs
	_ = viper.BindEnv("source", "PCSM_SOURCE_URI")
	_ = viper.BindEnv("target", "PCSM_TARGET_URI")

	// MongoDB client timeout
	_ = viper.BindEnv("mongodb-cli-operation-timeout", "PCSM_MONGODB_CLI_OPERATION_TIMEOUT")

	// Bulk write option (internal, has env var support)
	_ = viper.BindEnv("use-collection-bulk-write", "PCSM_USE_COLLECTION_BULK_WRITE")

	// NOTE: Clone tuning options intentionally NOT bound to env vars.
	// They are CLI/HTTP-only per stakeholder decision (PCSM-219).
	// See: comment-decisions.md - Decision #2
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
