package config

import (
	"math"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/dustin/go-humanize"
)

// UseCollectionBulkWrite determines whether to use the Collection Bulk Write API
// instead of the Client Bulk Write API (introduced in MongoDB v8.0).
// Enabled when the PLM_USE_COLLECTION_BULK_WRITE environment variable is set to "1".
func UseCollectionBulkWrite() bool {
	return os.Getenv("PLM_USE_COLLECTION_BULK_WRITE") == "1"
}

// CloneNumParallelCollections returns the number of collections cloned in parallel
// during the clone process. Default is 0.
func CloneNumParallelCollections() int {
	numColl, _ := strconv.ParseInt(os.Getenv("PLM_CLONE_NUM_PARALLEL_COLLECTIONS"), 10, 32)

	return int(numColl)
}

// CloneNumReadWorkers returns the number of read workers used during the clone. Default is 0.
// Note: Workers are shared across all collections.
func CloneNumReadWorkers() int {
	numReadWorker, _ := strconv.ParseInt(os.Getenv("PLM_CLONE_NUM_READ_WORKERS"), 10, 32)

	return int(numReadWorker)
}

// CloneNumInsertWorkers returns the number of insert workers used during the clone. Default is 0.
// Note: Workers are shared across all collections.
func CloneNumInsertWorkers() int {
	numInsertWorker, _ := strconv.ParseInt(os.Getenv("PLM_CLONE_NUM_INSERT_WORKERS"), 10, 32)

	return int(numInsertWorker)
}

// CloneSegmentSizeBytes returns the segment size in bytes used during the clone.
// A segment is a range within a collection (by _id) that enables concurrent read/insert
// operations by splitting the collection into multiple parallelizable units.
// Zero or less enables auto size (per each collection). Default is [AutoCloneSegmentSize].
func CloneSegmentSizeBytes() int64 {
	segmentSizeBytes, _ := humanize.ParseBytes(os.Getenv("PLM_CLONE_SEGMENT_SIZE"))
	if segmentSizeBytes == 0 {
		return AutoCloneSegmentSize
	}

	return int64(min(segmentSizeBytes, math.MaxInt64)) //nolint:gosec
}

// CloneReadBatchSizeBytes returns the read batch size in bytes used during the clone. Default is 0.
func CloneReadBatchSizeBytes() int32 {
	batchSizeBytes, _ := humanize.ParseBytes(os.Getenv("PLM_CLONE_READ_BATCH_SIZE"))

	return int32(min(batchSizeBytes, math.MaxInt32)) //nolint:gosec
}

// UseTargetClientCompressors returns a list of enabled compressors (from "zstd", "zlib", "snappy")
// for the target MongoDB client connection, as specified by the comma-separated environment
// variable PLM_DEV_TARGET_CLIENT_COMPRESSORS. If unset or empty, returns nil.
func UseTargetClientCompressors() []string {
	s := strings.TrimSpace(os.Getenv("PLM_DEV_TARGET_CLIENT_COMPRESSORS"))
	if s == "" {
		return nil
	}

	allowCompressors := []string{"zstd", "zlib", "snappy"}

	rv := make([]string, 0, min(len(s), len(allowCompressors)))
	for _, a := range strings.Split(s, ",") {
		a = strings.TrimSpace(a)
		if slices.Contains(allowCompressors, a) && !slices.Contains(rv, a) {
			rv = append(rv, a)
		}
	}

	return rv
}
