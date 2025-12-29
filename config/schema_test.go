package config_test

import (
	"testing"

	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/assert"

	"github.com/percona/percona-clustersync-mongodb/config"
)

func TestCloneConfig_SegmentSizeBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		segmentSize string
		want        int64
	}{
		{
			name:        "empty string - returns auto (0)",
			segmentSize: "",
			want:        config.AutoCloneSegmentSize,
		},
		{
			name:        "valid size 1GiB",
			segmentSize: "1GiB",
			want:        humanize.GiByte,
		},
		{
			name:        "valid size 500MB",
			segmentSize: "500MB",
			want:        500 * humanize.MByte,
		},
		{
			name:        "valid size 2GiB",
			segmentSize: "2GiB",
			want:        2 * humanize.GiByte,
		},
		{
			name:        "invalid format - returns auto (0)",
			segmentSize: "invalid",
			want:        config.AutoCloneSegmentSize,
		},
		{
			name:        "zero value string - returns auto (0)",
			segmentSize: "0",
			want:        config.AutoCloneSegmentSize,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &config.CloneConfig{
				SegmentSize: tt.segmentSize,
			}

			got := cfg.SegmentSizeBytes()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCloneConfig_ReadBatchSizeBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		readBatchSize string
		want          int32
	}{
		{
			name:          "empty string - returns 0",
			readBatchSize: "",
			want:          0,
		},
		{
			name:          "valid size 16MiB",
			readBatchSize: "16MiB",
			want:          16 * humanize.MiByte,
		},
		{
			name:          "valid size 32MB",
			readBatchSize: "32MB",
			want:          32 * humanize.MByte,
		},
		{
			name:          "valid size 48MB",
			readBatchSize: "48MB",
			want:          48 * humanize.MByte,
		},
		{
			name:          "invalid format - returns 0",
			readBatchSize: "invalid",
			want:          0,
		},
		{
			name:          "zero value string - returns 0",
			readBatchSize: "0",
			want:          0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &config.CloneConfig{
				ReadBatchSize: tt.readBatchSize,
			}

			got := cfg.ReadBatchSizeBytes()
			assert.Equal(t, tt.want, got)
		})
	}
}
