package topo //nolint:testpackage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerVersion(t *testing.T) {
	t.Parallel()

	t.Run("PSMDB", func(t *testing.T) {
		t.Parallel()

		s := "8.0.4-2"
		version, err := parseServerVersion(s)
		require.NoError(t, err)
		assert.Equal(t, 8, version.Major())
		assert.Equal(t, 0, version.Minor())
		assert.Equal(t, 4, version.Patch())
		assert.Equal(t, 2, version.Build())
		assert.True(t, version.IsPSMDB())
		assert.Equal(t, s, version.String())
	})

	t.Run("CE", func(t *testing.T) {
		t.Parallel()

		s := "8.0.4"
		version, err := parseServerVersion(s)
		require.NoError(t, err)
		assert.Equal(t, 8, version.Major())
		assert.Equal(t, 0, version.Minor())
		assert.Equal(t, 4, version.Patch())
		assert.Equal(t, 0, version.Build())
		assert.False(t, version.IsPSMDB())
		assert.Equal(t, s, version.String())
	})

	t.Run("max values", func(t *testing.T) {
		t.Parallel()

		s := "255.255.255-255"
		version, err := parseServerVersion(s)
		require.NoError(t, err)
		assert.Equal(t, 255, version.Major())
		assert.Equal(t, 255, version.Minor())
		assert.Equal(t, 255, version.Patch())
		assert.Equal(t, 255, version.Build())
		assert.Equal(t, s, version.String())
	})

	t.Run("min values", func(t *testing.T) {
		t.Parallel()

		s := "0.0.0-1"
		version, err := parseServerVersion(s)
		require.NoError(t, err)
		assert.Equal(t, s, version.String())
	})

	t.Run("build without patch", func(t *testing.T) {
		t.Parallel()

		assert := assertAs(t, &InvalidVersionError{})
		assert(parseServerVersion("8.0-2"))
		assert(parseServerVersion("8.0"))
	})

	t.Run("empty", func(t *testing.T) {
		t.Parallel()

		assert := assertAs(t, &InvalidVersionError{})
		assert(parseServerVersion(""))
		assert(parseServerVersion("."))
		assert(parseServerVersion(".."))
		assert(parseServerVersion(".1."))
		assert(parseServerVersion("..-"))
	})

	t.Run("invalid minor", func(t *testing.T) {
		t.Parallel()

		assert := assertAs(t, &InvalidVersionError{})
		assert(parseServerVersion("8..0"))
		assert(parseServerVersion("8.a.0"))
	})

	t.Run("invalid patch", func(t *testing.T) {
		t.Parallel()

		assert := assertAs(t, &InvalidVersionError{})
		assert(parseServerVersion("8.0."))
		assert(parseServerVersion("8.0.-2"))
		assert(parseServerVersion("8.0.a-2"))
	})

	t.Run("invalid build", func(t *testing.T) {
		t.Parallel()

		assert := assertAs(t, &InvalidVersionError{})
		assert(parseServerVersion("8.0.0-"))
		assert(parseServerVersion("8.0.0-"))
		assert(parseServerVersion("8.0.0-a"))
	})
}

func assertAs(t *testing.T, expected any) func(any, error) {
	t.Helper()

	return func(_ any, err error) {
		t.Helper()

		require.ErrorAs(t, err, expected)
	}
}
