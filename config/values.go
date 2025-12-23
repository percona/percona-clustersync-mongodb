package config

import (
	"math"
	"os"
	"slices"
	"strconv"
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

// CloneNumParallelCollections returns the number of collections cloned in parallel
// during the clone process. Default is 0.
func CloneNumParallelCollections() int {
	numColl, _ := strconv.ParseInt(os.Getenv("PCSM_CLONE_NUM_PARALLEL_COLLECTIONS"), 10, 32)

	return int(numColl)
}

// CloneNumReadWorkers returns the number of read workers used during the clone. Default is 0.
// Note: Workers are shared across all collections.
func CloneNumReadWorkers() int {
	numReadWorker, _ := strconv.ParseInt(os.Getenv("PCSM_CLONE_NUM_READ_WORKERS"), 10, 32)

	return int(numReadWorker)
}

// CloneNumInsertWorkers returns the number of insert workers used during the clone. Default is 0.
// Note: Workers are shared across all collections.
func CloneNumInsertWorkers() int {
	numInsertWorker, _ := strconv.ParseInt(os.Getenv("PCSM_CLONE_NUM_INSERT_WORKERS"), 10, 32)

	return int(numInsertWorker)
}

// CloneSegmentSizeBytes returns the segment size in bytes used during the clone.
// A segment is a range within a collection (by _id) that enables concurrent read/insert
// operations by splitting the collection into multiple parallelizable units.
// Zero or less enables auto size (per each collection). Default is [AutoCloneSegmentSize].
func CloneSegmentSizeBytes() int64 {
	segmentSizeBytes, _ := humanize.ParseBytes(os.Getenv("PCSM_CLONE_SEGMENT_SIZE"))
	if segmentSizeBytes == 0 {
		return AutoCloneSegmentSize
	}

	return int64(min(segmentSizeBytes, math.MaxInt64)) //nolint:gosec
}

// CloneReadBatchSizeBytes returns the read batch size in bytes used during the clone. Default is 0.
func CloneReadBatchSizeBytes() int32 {
	batchSizeBytes, _ := humanize.ParseBytes(os.Getenv("PCSM_CLONE_READ_BATCH_SIZE"))

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
