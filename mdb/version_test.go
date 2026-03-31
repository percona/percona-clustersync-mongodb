package mdb //nolint:testpackage

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

func TestCheckVersionCompat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		source       string
		target       string
		crossVersion bool
		wantErr      bool
	}{
		{
			name:   "same major version",
			source: "8.0.4", target: "8.0.4",
		},
		{
			name:   "same major, different patch",
			source: "8.0.2", target: "8.0.6",
		},
		{
			name:   "upgrade 7.0 to 8.0",
			source: "7.0.0", target: "8.0.0",
			crossVersion: true,
		},
		{
			name:   "upgrade 6.0 to 8.0",
			source: "6.0.3", target: "8.0.4",
			crossVersion: true,
		},
		{
			name:   "downgrade 8.0 to 7.0",
			source: "8.0.4", target: "7.0.0",
			wantErr: true,
		},
		{
			name:   "downgrade 8.0 to 6.0",
			source: "8.0.0", target: "6.0.3",
			wantErr: true,
		},
		{
			name:   "PSMDB upgrade",
			source: "7.0.4-2", target: "8.0.4-2",
			crossVersion: true,
		},
		{
			name:   "PSMDB downgrade",
			source: "8.0.4-2", target: "7.0.4-2",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			source, err := parseServerVersion(tt.source)
			require.NoError(t, err)

			target, err := parseServerVersion(tt.target)
			require.NoError(t, err)

			crossVersion, err := CheckVersionCompat(source, target)

			if tt.wantErr {
				require.ErrorIs(t, err, ErrDowngrade)
				assert.False(t, crossVersion)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.crossVersion, crossVersion)
			}
		})
	}
}

func assertAs(t *testing.T, expected any) func(any, error) {
	t.Helper()

	return func(_ any, err error) {
		t.Helper()

		require.ErrorAs(t, err, expected)
	}
}
