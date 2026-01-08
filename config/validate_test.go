package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/percona/percona-clustersync-mongodb/config"
)

func TestValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     *config.Config
		wantErr string
	}{
		{
			name: "valid config",
			cfg: &config.Config{
				Port:   8080,
				Source: "mongodb://source:27017",
				Target: "mongodb://target:27017",
			},
			wantErr: "",
		},
		{
			name: "port zero uses default - valid",
			cfg: &config.Config{
				Port:   0,
				Source: "mongodb://source:27017",
				Target: "mongodb://target:27017",
			},
			wantErr: "",
		},
		{
			name: "port at lower bound (1025) - valid",
			cfg: &config.Config{
				Port:   1025,
				Source: "mongodb://source:27017",
				Target: "mongodb://target:27017",
			},
			wantErr: "",
		},
		{
			name: "port at upper bound (65535) - valid",
			cfg: &config.Config{
				Port:   65535,
				Source: "mongodb://source:27017",
				Target: "mongodb://target:27017",
			},
			wantErr: "",
		},
		{
			name: "port below range (1024)",
			cfg: &config.Config{
				Port:   1024,
				Source: "mongodb://source:27017",
				Target: "mongodb://target:27017",
			},
			wantErr: "port value is outside the supported range",
		},
		{
			name: "port above range (65536)",
			cfg: &config.Config{
				Port:   65536,
				Source: "mongodb://source:27017",
				Target: "mongodb://target:27017",
			},
			wantErr: "port value is outside the supported range",
		},
		{
			name: "source empty",
			cfg: &config.Config{
				Port:   8080,
				Source: "",
				Target: "mongodb://target:27017",
			},
			wantErr: "source URI is empty",
		},
		{
			name: "target empty",
			cfg: &config.Config{
				Port:   8080,
				Source: "mongodb://source:27017",
				Target: "",
			},
			wantErr: "target URI is empty",
		},
		{
			name: "both source and target empty",
			cfg: &config.Config{
				Port:   8080,
				Source: "",
				Target: "",
			},
			wantErr: "source URI and target URI are empty",
		},
		{
			name: "source equals target",
			cfg: &config.Config{
				Port:   8080,
				Source: "mongodb://same:27017",
				Target: "mongodb://same:27017",
			},
			wantErr: "source URI and target URI are identical",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := config.Validate(tt.cfg)

			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestValidate_PortBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		port    int
		wantErr bool
	}{
		// Invalid ports (at or below 1024)
		{"port 1 - invalid", 1, true},
		{"port 100 - invalid", 100, true},
		{"port 1023 - invalid", 1023, true},
		{"port 1024 - invalid (boundary)", 1024, true},

		// Valid ports (above 1024 and up to 65535)
		{"port 1025 - valid (lower boundary)", 1025, false},
		{"port 2242 - valid (default)", 2242, false},
		{"port 8080 - valid (common)", 8080, false},
		{"port 27017 - valid (MongoDB default)", 27017, false},
		{"port 65535 - valid (upper boundary)", 65535, false},

		// Invalid ports (above 65535)
		{"port 65536 - invalid (above max)", 65536, true},
		{"port 70000 - invalid", 70000, true},
		{"port 100000 - invalid", 100000, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &config.Config{
				Port:   tt.port,
				Source: "mongodb://source:27017",
				Target: "mongodb://target:27017",
			}

			err := config.Validate(cfg)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "port value is outside")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateCloneSegmentSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		sizeBytes uint64
		wantErr   string
	}{
		{
			name:      "zero (auto) - valid",
			sizeBytes: 0,
			wantErr:   "",
		},
		{
			name:      "at minimum boundary - valid",
			sizeBytes: config.MinCloneSegmentSizeBytes,
			wantErr:   "",
		},
		{
			name:      "above minimum - valid",
			sizeBytes: config.MinCloneSegmentSizeBytes + 1,
			wantErr:   "",
		},
		{
			name:      "at maximum boundary - valid",
			sizeBytes: config.MaxCloneSegmentSizeBytes,
			wantErr:   "",
		},
		{
			name:      "below minimum",
			sizeBytes: config.MinCloneSegmentSizeBytes - 1,
			wantErr:   "cloneSegmentSize must be at least",
		},
		{
			name:      "above maximum",
			sizeBytes: config.MaxCloneSegmentSizeBytes + 1,
			wantErr:   "cloneSegmentSize must be at most",
		},
		{
			name:      "1 byte (below minimum)",
			sizeBytes: 1,
			wantErr:   "cloneSegmentSize must be at least",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := config.ValidateCloneSegmentSize(tt.sizeBytes)

			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestValidateCloneReadBatchSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		sizeBytes uint64
		wantErr   string
	}{
		{
			name:      "zero (auto) - valid",
			sizeBytes: 0,
			wantErr:   "",
		},
		{
			name:      "at minimum boundary - valid",
			sizeBytes: uint64(config.MinCloneReadBatchSizeBytes),
			wantErr:   "",
		},
		{
			name:      "above minimum - valid",
			sizeBytes: uint64(config.MinCloneReadBatchSizeBytes) + 1,
			wantErr:   "",
		},
		{
			name:      "at maximum boundary - valid",
			sizeBytes: uint64(config.MaxCloneReadBatchSizeBytes),
			wantErr:   "",
		},
		{
			name:      "below minimum",
			sizeBytes: uint64(config.MinCloneReadBatchSizeBytes) - 1,
			wantErr:   "cloneReadBatchSize must be at least",
		},
		{
			name:      "above maximum",
			sizeBytes: uint64(config.MaxCloneReadBatchSizeBytes) + 1,
			wantErr:   "cloneReadBatchSize must be at most",
		},
		{
			name:      "1 byte (below minimum)",
			sizeBytes: 1,
			wantErr:   "cloneReadBatchSize must be at least",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := config.ValidateCloneReadBatchSize(tt.sizeBytes)

			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}
