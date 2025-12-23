package config

import (
	"math"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/spf13/viper"
)

// UseCollectionBulkWrite returns whether to use collection-level bulk write.
// This is an internal option, not exposed via HTTP API.
//
// Configuration sources (in order of precedence):
//   - CLI flag: --use-collection-bulk-write (hidden)
//   - Env var: PCSM_USE_COLLECTION_BULK_WRITE
//   - Default: false
func UseCollectionBulkWrite() bool {
	return viper.GetBool("use-collection-bulk-write")
}

// CloneNumParallelCollections returns the number of collections to clone in parallel.
// Configurable via CLI flag only (no env var support per decision #2).
//
// Configuration sources (in order of precedence):
//   - CLI flag: --clone-num-parallel-collections
//   - Default: 0 (auto)
func CloneNumParallelCollections() int {
	return viper.GetInt("clone-num-parallel-collections")
}

// CloneNumReadWorkers returns the number of read workers.
// Configurable via CLI flag only (no env var support per decision #2).
//
// Configuration sources (in order of precedence):
//   - CLI flag: --clone-num-read-workers
//   - Default: 0 (auto)
func CloneNumReadWorkers() int {
	return viper.GetInt("clone-num-read-workers")
}

// CloneNumInsertWorkers returns the number of insert workers.
// Configurable via CLI flag only (no env var support per decision #2).
//
// Configuration sources (in order of precedence):
//   - CLI flag: --clone-num-insert-workers
//   - Default: 0 (auto)
func CloneNumInsertWorkers() int {
	return viper.GetInt("clone-num-insert-workers")
}

// CloneSegmentSizeBytes returns the segment size in bytes.
// Configurable via CLI flag only (no env var support per decision #2).
//
// Configuration sources (in order of precedence):
//   - CLI flag: --clone-segment-size
//   - Default: AutoCloneSegmentSize (0 = auto)
func CloneSegmentSizeBytes() int64 {
	sizeStr := viper.GetString("clone-segment-size")
	if sizeStr == "" {
		return AutoCloneSegmentSize
	}

	segmentSizeBytes, _ := humanize.ParseBytes(sizeStr)
	if segmentSizeBytes == 0 {
		return AutoCloneSegmentSize
	}

	return int64(min(segmentSizeBytes, math.MaxInt64)) //nolint:gosec
}

// CloneReadBatchSizeBytes returns the read batch size in bytes.
// Configurable via CLI flag only (no env var support per decision #2).
//
// Configuration sources (in order of precedence):
//   - CLI flag: --clone-read-batch-size
//   - Default: 0 (uses MaxWriteBatchSizeBytes)
func CloneReadBatchSizeBytes() int32 {
	sizeStr := viper.GetString("clone-read-batch-size")
	if sizeStr == "" {
		return 0
	}

	batchSizeBytes, _ := humanize.ParseBytes(sizeStr)

	return int32(min(batchSizeBytes, math.MaxInt32)) //nolint:gosec
}

// UseTargetClientCompressors returns a list of enabled compressors (from "zstd", "zlib", "snappy")
// for the target MongoDB client connection, as specified by the comma-separated environment
// variable PCSM_DEV_TARGET_CLIENT_COMPRESSORS. If unset or empty, returns nil.
func UseTargetClientCompressors() []string {
	s := strings.TrimSpace(os.Getenv("PCSM_DEV_TARGET_CLIENT_COMPRESSORS"))
	if s == "" {
		return nil
	}

	allowCompressors := []string{"zstd", "zlib", "snappy"}

	rv := make([]string, 0, min(len(s), len(allowCompressors)))
	for a := range strings.SplitSeq(s, ",") {
		a = strings.TrimSpace(a)
		if slices.Contains(allowCompressors, a) && !slices.Contains(rv, a) {
			rv = append(rv, a)
		}
	}

	return rv
}

// MongoDBOperationTimeout returns the timeout for MongoDB client operations.
//
// Configuration sources (in order of precedence):
//   - CLI flag: --mongodb-cli-operation-timeout
//   - Env var: PCSM_MONGODB_CLI_OPERATION_TIMEOUT
//   - Default: 5m
func MongoDBOperationTimeout() time.Duration {
	timeoutStr := viper.GetString("mongodb-cli-operation-timeout")
	if timeoutStr != "" {
		d, err := time.ParseDuration(timeoutStr)
		if err == nil && d > 0 {
			return d
		}
	}

	return DefaultMongoDBCliOperationTimeout
}

// OperationMongoDBCliTimeout is an alias for MongoDBOperationTimeout for backward compatibility.
//
// Deprecated: Use MongoDBOperationTimeout instead.
func OperationMongoDBCliTimeout() time.Duration {
	return MongoDBOperationTimeout()
}
