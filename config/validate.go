package config

import (
	"github.com/dustin/go-humanize"

	"github.com/percona/percona-clustersync-mongodb/errors"
)

// DefaultServerPort is the default port for the PCSM HTTP server.
const DefaultServerPort = 2242

// Validate validates the Config for required fields and value ranges.
func Validate(cfg *Config) error {
	port := cfg.Port
	if port == 0 {
		port = DefaultServerPort
	}

	if port <= 1024 || port > 65535 {
		return errors.New("port value is outside the supported range [1024 - 65535]")
	}

	switch {
	case cfg.Source == "" && cfg.Target == "":
		return errors.New("source URI and target URI are empty")
	case cfg.Source == "":
		return errors.New("source URI is empty")
	case cfg.Target == "":
		return errors.New("target URI is empty")
	case cfg.Source == cfg.Target:
		return errors.New("source URI and target URI are identical")
	}

	return nil
}

// ValidateCloneSegmentSize validates a clone segment size value in bytes.
// It allows 0 (auto) or values within [MinCloneSegmentSizeBytes, MaxCloneSegmentSizeBytes].
func ValidateCloneSegmentSize(sizeBytes uint64) error {
	if sizeBytes == 0 {
		return nil // 0 means auto
	}

	if sizeBytes < MinCloneSegmentSizeBytes {
		return errors.Errorf("cloneSegmentSize must be at least %s, got %s",
			humanize.Bytes(MinCloneSegmentSizeBytes),
			humanize.Bytes(sizeBytes))
	}

	if sizeBytes > MaxCloneSegmentSizeBytes {
		return errors.Errorf("cloneSegmentSize must be at most %s, got %s",
			humanize.Bytes(MaxCloneSegmentSizeBytes),
			humanize.Bytes(sizeBytes))
	}

	return nil
}

// ValidateCloneReadBatchSize validates a clone read batch size value in bytes.
// It allows 0 (auto) or values within [MinCloneReadBatchSizeBytes, MaxCloneReadBatchSizeBytes].
func ValidateCloneReadBatchSize(sizeBytes uint64) error {
	if sizeBytes == 0 {
		return nil // 0 means auto
	}

	if sizeBytes < uint64(MinCloneReadBatchSizeBytes) {
		return errors.Errorf("cloneReadBatchSize must be at least %s, got %s",
			humanize.Bytes(uint64(MinCloneReadBatchSizeBytes)),
			humanize.Bytes(sizeBytes))
	}

	if sizeBytes > uint64(MaxCloneReadBatchSizeBytes) {
		return errors.Errorf("cloneReadBatchSize must be at most %s, got %s",
			humanize.Bytes(uint64(MaxCloneReadBatchSizeBytes)),
			humanize.Bytes(sizeBytes))
	}

	return nil
}
