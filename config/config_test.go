package config_test

import (
	"fmt"
	"testing"

	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/percona/percona-clustersync-mongodb/config"
)

func TestResolveCloneSegmentSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     *config.Config
		value   *string
		want    int64
		wantErr string
	}{
		{
			name:    "nil value - falls back to config default (empty)",
			cfg:     &config.Config{},
			value:   nil,
			want:    config.AutoCloneSegmentSize,
			wantErr: "",
		},
		{
			name: "nil value - falls back to config value",
			cfg: &config.Config{
				Clone: config.CloneConfig{
					SegmentSize: "1GiB",
				},
			},
			value:   nil,
			want:    humanize.GiByte,
			wantErr: "",
		},
		{
			name:    "valid size 500MB (above minimum)",
			cfg:     &config.Config{},
			value:   toPtr("500MB"),
			want:    500 * humanize.MByte,
			wantErr: "",
		},
		{
			name:    "valid size 1GiB",
			cfg:     &config.Config{},
			value:   toPtr("1GiB"),
			want:    humanize.GiByte,
			wantErr: "",
		},
		{
			name:    "zero value (auto)",
			cfg:     &config.Config{},
			value:   toPtr("0"),
			want:    0,
			wantErr: "",
		},
		{
			name:    "below minimum (100MB)",
			cfg:     &config.Config{},
			value:   toPtr("100MB"),
			wantErr: "cloneSegmentSize must be at least",
		},
		{
			name:    "above maximum",
			cfg:     &config.Config{},
			value:   toPtr("100GiB"),
			wantErr: "cloneSegmentSize must be at most",
		},
		{
			name:    "at minimum boundary (using exact bytes)",
			cfg:     &config.Config{},
			value:   toPtr(fmt.Sprintf("%dB", config.MinCloneSegmentSizeBytes)),
			want:    int64(config.MinCloneSegmentSizeBytes),
			wantErr: "",
		},
		{
			name:    "at maximum boundary",
			cfg:     &config.Config{},
			value:   toPtr("64GiB"),
			want:    int64(config.MaxCloneSegmentSizeBytes),
			wantErr: "",
		},
		{
			name:    "invalid format",
			cfg:     &config.Config{},
			value:   toPtr("abc"),
			wantErr: "invalid cloneSegmentSize value",
		},
		{
			name:    "empty string",
			cfg:     &config.Config{},
			value:   toPtr(""),
			wantErr: "invalid cloneSegmentSize value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := config.ResolveCloneSegmentSize(tt.cfg, tt.value)

			if tt.wantErr == "" {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestResolveCloneReadBatchSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     *config.Config
		value   *string
		want    int32
		wantErr string
	}{
		{
			name:    "nil value - falls back to config default (empty)",
			cfg:     &config.Config{},
			value:   nil,
			want:    0,
			wantErr: "",
		},
		{
			name: "nil value - falls back to config value",
			cfg: &config.Config{
				Clone: config.CloneConfig{
					ReadBatchSize: "32MiB",
				},
			},
			value:   nil,
			want:    32 * humanize.MiByte,
			wantErr: "",
		},
		{
			name:    "valid size 16MiB",
			cfg:     &config.Config{},
			value:   toPtr("16MiB"),
			want:    16 * humanize.MiByte,
			wantErr: "",
		},
		{
			name:    "valid size 48MB",
			cfg:     &config.Config{},
			value:   toPtr("48MB"),
			want:    48 * humanize.MByte,
			wantErr: "",
		},
		{
			name:    "zero value (auto)",
			cfg:     &config.Config{},
			value:   toPtr("0"),
			want:    0,
			wantErr: "",
		},
		{
			name:    "below minimum",
			cfg:     &config.Config{},
			value:   toPtr("1KB"),
			wantErr: "cloneReadBatchSize must be at least",
		},
		{
			name:    "at minimum boundary (using exact bytes)",
			cfg:     &config.Config{},
			value:   toPtr(fmt.Sprintf("%dB", config.MinCloneReadBatchSizeBytes)),
			want:    config.MinCloneReadBatchSizeBytes,
			wantErr: "",
		},
		{
			name:    "invalid format",
			cfg:     &config.Config{},
			value:   toPtr("xyz"),
			wantErr: "invalid cloneReadBatchSize value",
		},
		{
			name:    "empty string",
			cfg:     &config.Config{},
			value:   toPtr(""),
			wantErr: "invalid cloneReadBatchSize value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := config.ResolveCloneReadBatchSize(tt.cfg, tt.value)

			if tt.wantErr == "" {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestUseTargetClientCompressors(t *testing.T) {
	tests := []struct {
		name    string
		envVal  string
		want    []string
		wantNil bool
	}{
		{
			name:    "empty env - returns nil",
			envVal:  "",
			want:    nil,
			wantNil: true,
		},
		{
			name:   "single valid compressor zstd",
			envVal: "zstd",
			want:   []string{"zstd"},
		},
		{
			name:   "single valid compressor zlib",
			envVal: "zlib",
			want:   []string{"zlib"},
		},
		{
			name:   "single valid compressor snappy",
			envVal: "snappy",
			want:   []string{"snappy"},
		},
		{
			name:   "multiple valid compressors",
			envVal: "zstd,zlib,snappy",
			want:   []string{"zstd", "zlib", "snappy"},
		},
		{
			name:   "compressors with spaces",
			envVal: " zstd , zlib , snappy ",
			want:   []string{"zstd", "zlib", "snappy"},
		},
		{
			name:   "invalid compressor ignored",
			envVal: "zstd,invalid,zlib",
			want:   []string{"zstd", "zlib"},
		},
		{
			name:   "all invalid compressors - returns empty slice",
			envVal: "invalid,gzip,lz4",
			want:   []string{},
		},
		{
			name:   "duplicate compressors - deduplicated",
			envVal: "zstd,zstd,zlib,zstd",
			want:   []string{"zstd", "zlib"},
		},
		{
			name:    "whitespace only - returns nil",
			envVal:  "   ",
			want:    nil,
			wantNil: true,
		},
		{
			name:   "mixed valid and invalid with spaces",
			envVal: " zstd , invalid , snappy ",
			want:   []string{"zstd", "snappy"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("PCSM_DEV_TARGET_CLIENT_COMPRESSORS", tt.envVal)

			got := config.UseTargetClientCompressors()

			if tt.wantNil {
				assert.Nil(t, got)
			} else {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func toPtr(s string) *string { return &s }
