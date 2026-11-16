package mdb //nolint:testpackage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
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

func TestServerVersionBuildFallback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  ServerVersion
	}{
		{name: "numeric build", input: "8.0.4-2", want: ServerVersion{8, 0, 4, 2}},
		{name: "no build", input: "8.0.4", want: ServerVersion{8, 0, 4}},
		{name: "empty build", input: "8.0.4-", want: ServerVersion{8, 0, 4}},
		{name: "arbitrary build", input: "8.0.4-custom", want: ServerVersion{8, 0, 4}},
		{name: "custom numeric build", input: "8.0.4-2-custom", want: ServerVersion{8, 0, 4}},
		{name: "overflow build", input: "8.0.4-256", want: ServerVersion{8, 0, 4}},
		{name: "negative build", input: "8.0.4--1", want: ServerVersion{8, 0, 4}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			version, err := parseServerVersion(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.want, version)
			assert.Equal(t, tt.want.Build(), version.Build())
			assert.Equal(t, tt.want.IsPSMDB(), version.IsPSMDB())
			assert.Equal(t, tt.want.String(), version.String())
		})
	}
}

func TestServerVersionInvalidCore(t *testing.T) {
	t.Parallel()

	for _, input := range []string{
		"",
		"8.0",
		"8..4-custom",
		"8.a.4-custom",
		"8.0.x-custom",
		"256.0.4-custom",
		"8.256.4-custom",
		"8.0.256-custom",
		"8.0.4.1-custom",
	} {
		t.Run(input, func(t *testing.T) {
			t.Parallel()

			_, err := parseServerVersion(input)
			var invalidVersion InvalidVersionError
			require.ErrorAs(t, err, &invalidVersion)
		})
	}
}

func TestParseFCV(t *testing.T) {
	t.Parallel()

	got, err := parseFCV("8.0")
	require.NoError(t, err)
	assert.Equal(t, FCV{8, 0}, got)
	assert.Equal(t, "8.0", got.String())

	got, err = parseFCV("8.10")
	require.NoError(t, err)
	assert.Equal(t, FCV{8, 10}, got)

	for _, input := range []string{"", "8", "8.", ".0", "a.b", "8.x", "-1.0", "8.-1", "8.0.1"} {
		t.Run(input, func(t *testing.T) {
			t.Parallel()

			_, err := parseFCV(input)
			var invalid InvalidFCVError
			require.ErrorAs(t, err, &invalid)
		})
	}
}

func TestFCVFromRaw(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		doc     bson.D
		want    FCV
		wantErr bool
	}{
		{
			name: "document",
			doc:  bson.D{{"featureCompatibilityVersion", bson.D{{"version", "8.0"}}}},
			want: FCV{8, 0},
		},
		{
			name: "legacy string",
			doc:  bson.D{{"featureCompatibilityVersion", "4.2"}},
			want: FCV{4, 2},
		},
		{name: "missing", doc: bson.D{{"ok", 1}}, wantErr: true},
		{name: "wrong type", doc: bson.D{{"featureCompatibilityVersion", 8}}, wantErr: true},
		{
			name:    "document without version",
			doc:     bson.D{{"featureCompatibilityVersion", bson.D{{"targetVersion", "8.0"}}}},
			wantErr: true,
		},
		{
			name:    "invalid value",
			doc:     bson.D{{"featureCompatibilityVersion", bson.D{{"version", "eight"}}}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			raw, err := bson.Marshal(tt.doc)
			require.NoError(t, err)

			got, err := fcvFromRaw(raw)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCheckFCVCompat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		source  FCV
		target  FCV
		wantErr bool
	}{
		{name: "equal", source: FCV{8, 0}, target: FCV{8, 0}},
		{name: "source lower", source: FCV{7, 0}, target: FCV{8, 0}},
		{name: "source lower minor", source: FCV{8, 0}, target: FCV{8, 2}},
		{name: "numeric not lexical", source: FCV{8, 2}, target: FCV{8, 10}},
		{name: "source higher major", source: FCV{8, 0}, target: FCV{7, 0}, wantErr: true},
		{name: "source higher minor", source: FCV{8, 2}, target: FCV{8, 0}, wantErr: true},
		{name: "numeric not lexical, higher", source: FCV{8, 10}, target: FCV{8, 2}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := CheckFCVCompat(tt.source, tt.target)
			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var fcvErr FCVDowngradeError
			require.ErrorAs(t, err, &fcvErr)
			assert.Equal(t, tt.source, fcvErr.Source)
			assert.Equal(t, tt.target, fcvErr.Target)
			assert.Contains(t, err.Error(), "source FCV "+tt.source.String()+
				" is higher than target FCV "+tt.target.String())
		})
	}
}

func TestFCVFromRawTransition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		doc  bson.D
		want FCVTransitionError
	}{
		{
			name: "upgrading",
			doc: bson.D{{"featureCompatibilityVersion", bson.D{
				{"version", "7.0"}, {"targetVersion", "8.0"},
			}}},
			want: FCVTransitionError{Version: "7.0", Target: "8.0"},
		},
		{
			name: "downgrading",
			doc: bson.D{{"featureCompatibilityVersion", bson.D{
				{"version", "7.0"}, {"targetVersion", "7.0"}, {"previousVersion", "8.0"},
			}}},
			want: FCVTransitionError{Version: "7.0", Target: "7.0", Previous: "8.0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			raw, err := bson.Marshal(tt.doc)
			require.NoError(t, err)

			_, err = fcvFromRaw(raw)

			var transition FCVTransitionError
			require.ErrorAs(t, err, &transition)
			assert.Equal(t, tt.want, transition)
			assert.Contains(t, err.Error(), "change in progress")
		})
	}
}

func TestFCVFromDoc(t *testing.T) {
	t.Parallel()

	raw, err := bson.Marshal(bson.D{{"_id", "featureCompatibilityVersion"}, {"version", "7.0"}})
	require.NoError(t, err)

	got, err := fcvFromDoc(raw)
	require.NoError(t, err)
	assert.Equal(t, FCV{7, 0}, got)

	raw, err = bson.Marshal(bson.D{
		{"_id", "featureCompatibilityVersion"}, {"version", "7.0"}, {"targetVersion", "8.0"},
	})
	require.NoError(t, err)

	_, err = fcvFromDoc(raw)
	var transition FCVTransitionError
	require.ErrorAs(t, err, &transition)

	raw, err = bson.Marshal(bson.D{{"_id", "featureCompatibilityVersion"}})
	require.NoError(t, err)

	_, err = fcvFromDoc(raw)
	var invalid InvalidFCVError
	require.ErrorAs(t, err, &invalid)
}

func TestFCVMalformedTransitionFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		field bson.E
	}{
		{name: "targetVersion not a string", field: bson.E{"targetVersion", 8}},
		{name: "targetVersion empty", field: bson.E{"targetVersion", ""}},
		{name: "targetVersion null", field: bson.E{"targetVersion", nil}},
		{name: "previousVersion not a string", field: bson.E{"previousVersion", bson.D{{"v", "8.0"}}}},
		{name: "previousVersion empty", field: bson.E{"previousVersion", ""}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fcvDoc := bson.D{{"version", "7.0"}, tt.field}

			// getParameter shape.
			raw, err := bson.Marshal(bson.D{{"featureCompatibilityVersion", fcvDoc}})
			require.NoError(t, err)

			_, err = fcvFromRaw(raw)
			var invalid InvalidFCVError
			require.ErrorAs(t, err, &invalid)

			// admin.system.version shape.
			raw, err = bson.Marshal(append(bson.D{{"_id", "featureCompatibilityVersion"}}, fcvDoc...))
			require.NoError(t, err)

			_, err = fcvFromDoc(raw)
			require.ErrorAs(t, err, &invalid)
		})
	}
}
