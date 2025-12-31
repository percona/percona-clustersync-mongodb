package config_test

import (
	"fmt"
	"testing"

	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/percona/percona-clustersync-mongodb/config"
)

func TestParseAndValidateCloneSegmentSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		want    int64
		wantErr string
	}{
		{
			name:  "valid size 500MB (above minimum)",
			value: "500MB",
			want:  500 * humanize.MByte,
		},
		{
			name:  "valid size 1GiB",
			value: "1GiB",
			want:  humanize.GiByte,
		},
		{
			name:  "zero value (auto)",
			value: "0",
			want:  0,
		},
		{
			name:    "below minimum (100MB)",
			value:   "100MB",
			wantErr: "cloneSegmentSize must be at least",
		},
		{
			name:    "above maximum",
			value:   "100GiB",
			wantErr: "cloneSegmentSize must be at most",
		},
		{
			name:  "at minimum boundary (using exact bytes)",
			value: fmt.Sprintf("%dB", config.MinCloneSegmentSizeBytes),
			want:  int64(config.MinCloneSegmentSizeBytes),
		},
		{
			name:  "at maximum boundary",
			value: "64GiB",
			want:  int64(config.MaxCloneSegmentSizeBytes),
		},
		{
			name:    "invalid format",
			value:   "abc",
			wantErr: "invalid cloneSegmentSize value",
		},
		{
			name:    "empty string",
			value:   "",
			wantErr: "invalid cloneSegmentSize value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := config.ParseAndValidateCloneSegmentSize(tt.value)

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

func TestParseAndValidateCloneReadBatchSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		want    int32
		wantErr string
	}{
		{
			name:  "valid size 16MiB",
			value: "16MiB",
			want:  16 * humanize.MiByte,
		},
		{
			name:  "valid size 48MB",
			value: "48MB",
			want:  48 * humanize.MByte,
		},
		{
			name:  "zero value (auto)",
			value: "0",
			want:  0,
		},
		{
			name:    "below minimum",
			value:   "1KB",
			wantErr: "cloneReadBatchSize must be at least",
		},
		{
			name:  "at minimum boundary (using exact bytes)",
			value: fmt.Sprintf("%dB", config.MinCloneReadBatchSizeBytes),
			want:  config.MinCloneReadBatchSizeBytes,
		},
		{
			name:    "invalid format",
			value:   "xyz",
			wantErr: "invalid cloneReadBatchSize value",
		},
		{
			name:    "empty string",
			value:   "",
			wantErr: "invalid cloneReadBatchSize value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := config.ParseAndValidateCloneReadBatchSize(tt.value)

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
