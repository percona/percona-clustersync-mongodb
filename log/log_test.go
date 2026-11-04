package log_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/percona/percona-clustersync-mongodb/log"
)

//nolint:paralleltest // Replaces the process-wide default logger.
func TestNewInheritsDefaultWithoutChangingIt(t *testing.T) {
	var output bytes.Buffer
	base := zerolog.New(&output).With().Str("component", "pcsm").Logger()
	previous := zerolog.DefaultContextLogger
	zerolog.DefaultContextLogger = &base
	t.Cleanup(func() { zerolog.DefaultContextLogger = previous })

	first := log.New("clone")
	second := log.New("repl")
	first.Info("first")
	second.Info("second")
	base.Info().Msg("base")

	decoder := json.NewDecoder(&output)
	for _, scope := range []string{"clone", "repl", ""} {
		var entry map[string]any
		require.NoError(t, decoder.Decode(&entry))
		assert.Equal(t, "pcsm", entry["component"])
		if scope == "" {
			assert.NotContains(t, entry, "s", "creating scoped loggers must not mutate the default")
		} else {
			assert.Equal(t, scope, entry["s"])
		}
	}
}

//nolint:paralleltest // Replaces the process-wide default logger.
func TestNewWithoutDefaultIsDisabled(t *testing.T) {
	previous := zerolog.DefaultContextLogger
	zerolog.DefaultContextLogger = nil
	t.Cleanup(func() { zerolog.DefaultContextLogger = previous })

	logger := log.New("clone")

	assert.Equal(t, zerolog.Disabled, logger.Unwrap().GetLevel())
	logger.Info("discarded")
	assert.Nil(t, zerolog.DefaultContextLogger)
}
