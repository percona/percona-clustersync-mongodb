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
// Internal option, not exposed via HTTP API.
func UseCollectionBulkWrite() bool {
	return viper.GetBool("use-collection-bulk-write")
}

// CloneNumParallelCollections returns the number of collections to clone in parallel.
// Returns 0 for auto-detection.
func CloneNumParallelCollections() int {
	return viper.GetInt("clone-num-parallel-collections")
}

// CloneNumReadWorkers returns the number of read workers.
// Returns 0 for auto-detection.
func CloneNumReadWorkers() int {
	return viper.GetInt("clone-num-read-workers")
}

// CloneNumInsertWorkers returns the number of insert workers.
// Returns 0 for auto-detection.
func CloneNumInsertWorkers() int {
	return viper.GetInt("clone-num-insert-workers")
}

// CloneSegmentSizeBytes returns the segment size in bytes.
// Returns 0 (AutoCloneSegmentSize) for auto-detection.
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
// Returns 0 to use MaxWriteBatchSizeBytes default.
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
