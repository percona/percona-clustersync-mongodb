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
		version, err := parseServerVersion(s, false)
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
		version, err := parseServerVersion(s, false)
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
		version, err := parseServerVersion(s, false)
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
		version, err := parseServerVersion(s, false)
		require.NoError(t, err)
		assert.Equal(t, s, version.String())
	})

	t.Run("build without patch", func(t *testing.T) {
		t.Parallel()

		assert := assertAs(t, &InvalidVersionError{})
		assert(parseServerVersion("8.0-2", false))
		assert(parseServerVersion("8.0", false))
	})

	t.Run("empty", func(t *testing.T) {
		t.Parallel()

		assert := assertAs(t, &InvalidVersionError{})
		assert(parseServerVersion("", false))
		assert(parseServerVersion(".", false))
		assert(parseServerVersion("..", false))
		assert(parseServerVersion(".1.", false))
		assert(parseServerVersion("..-", false))
	})

	t.Run("invalid minor", func(t *testing.T) {
		t.Parallel()

		assert := assertAs(t, &InvalidVersionError{})
		assert(parseServerVersion("8..0", false))
		assert(parseServerVersion("8.a.0", false))
	})

	t.Run("invalid patch", func(t *testing.T) {
		t.Parallel()

		assert := assertAs(t, &InvalidVersionError{})
		assert(parseServerVersion("8.0.", false))
		assert(parseServerVersion("8.0.-2", false))
		assert(parseServerVersion("8.0.a-2", false))
	})

	t.Run("invalid build", func(t *testing.T) {
		t.Parallel()

		assert := assertAs(t, &InvalidVersionError{})
		assert(parseServerVersion("8.0.0-", false))
		assert(parseServerVersion("8.0.0-", false))
		assert(parseServerVersion("8.0.0-a", false))
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

			source, err := parseServerVersion(tt.source, false)
			require.NoError(t, err)

			target, err := parseServerVersion(tt.target, false)
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

func TestServerVersionIgnoreBuild(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		input       string
		want        ServerVersion
		strictError bool
	}{
		{name: "numeric build", input: "8.0.4-2", want: ServerVersion{8, 0, 4}},
		{name: "no build", input: "8.0.4", want: ServerVersion{8, 0, 4}},
		{name: "empty build", input: "8.0.4-", want: ServerVersion{8, 0, 4}, strictError: true},
		{name: "arbitrary build", input: "8.0.4-custom", want: ServerVersion{8, 0, 4}, strictError: true},
		{name: "overflow build", input: "8.0.4-256", want: ServerVersion{8, 0, 4}, strictError: true},
		{name: "negative build", input: "8.0.4--1", want: ServerVersion{8, 0, 4}, strictError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			version, err := parseServerVersion(tt.input, true)
			require.NoError(t, err)
			assert.Equal(t, tt.want, version)
			assert.Zero(t, version.Build())
			assert.False(t, version.IsPSMDB())
			assert.Equal(t, tt.want.String(), version.String())

			_, err = parseServerVersion(tt.input, false)
			if tt.strictError {
				var invalidVersion InvalidVersionError
				require.ErrorAs(t, err, &invalidVersion)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestServerVersionInvalidCore(t *testing.T) {
	t.Parallel()

	for _, input := range []string{"", "8.0", "8..4-custom", "8.a.4-custom", "8.0.x-custom", "256.0.4-custom", "8.256.4-custom", "8.0.256-custom", "8.0.4.1-custom"} {
		t.Run(input, func(t *testing.T) {
			t.Parallel()

			for _, ignoreBuild := range []bool{false, true} {
				_, err := parseServerVersion(input, ignoreBuild)
				var invalidVersion InvalidVersionError
				require.ErrorAs(t, err, &invalidVersion)
			}
		})
	}
}
