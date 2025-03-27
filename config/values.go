package config

import (
	"math"
	"os"
	"runtime"
	"strconv"

	"github.com/dustin/go-humanize"
)

// UseCollectionBulkWrite determines whether to use the Collection Bulk Write API
// instead of the Client Bulk Write API (introduced in MongoDB v8.0).
// Enabled when the PML_USE_COLLECTION_BULK_WRITE environment variable is set to "1".
func UseCollectionBulkWrite() bool {
	return os.Getenv("PML_USE_COLLECTION_BULK_WRITE") == "1"
}

// CloneNumParallelCollection returns the number of collections cloned in parallel
// during the clone process. Falls back to DefaultCloneNumParallelCollection if
// the env var is not set or zero.
func CloneNumParallelCollection() int {
	numColl, _ := strconv.ParseInt(os.Getenv("PML_CLONE_NUM_PARALLEL_COLL"), 10, 32)
	if numColl == 0 {
		return DefaultCloneNumParallelCollection
	}

	return int(numColl)
}

// CloneNumReadWorker returns the number of read workers used during the clone.
// Defaults to half the number of CPU cores if the env var is not set or zero.
// Note: Workers are shared across all collections.
func CloneNumReadWorker() int {
	numReadWorker, _ := strconv.ParseInt(os.Getenv("PML_CLONE_NUM_READ_WORKER"), 10, 32)
	if numReadWorker == 0 {
		return runtime.NumCPU() / 2 //nolint:mnd
	}

	return int(numReadWorker)
}

// CloneNumInsertWorker returns the number of insert workers used during the clone.
// Defaults to half the number of CPU cores if the env var is not set or zero.
// Note: Workers are shared across all collections.
func CloneNumInsertWorker() int {
	numInsertWorker, _ := strconv.ParseInt(os.Getenv("PML_CLONE_NUM_INSERT_WORKER"), 10, 32)
	if numInsertWorker == 0 {
		return runtime.NumCPU() / 2 //nolint:mnd
	}

	return int(numInsertWorker)
}

// CloneSegmentSizeBytes returns the segment size in bytes used during the clone.
// A segment is a range within a collection (by _id) that enables concurrent read/insert
// operations by splitting the collection into multiple parallelizable units.
// Zero or less enables auto size (per each collection).
func CloneSegmentSizeBytes() int64 {
	segmentSizeBytes, _ := humanize.ParseBytes(os.Getenv("PML_CLONE_SEGMENT_SIZE"))
	if segmentSizeBytes == 0 {
		return AutoCloneSegmentSize
	}

	return int64(min(segmentSizeBytes, math.MaxInt64)) //nolint:gosec
}

// CloneReadBatchSizeBytes returns the read batch size in bytes used during the clone.
// Falls back to DefaultCloneReadBatchSizeBytes if the env var is not set or zero.
func CloneReadBatchSizeBytes() int32 {
	batchSizeBytes, _ := humanize.ParseBytes(os.Getenv("PML_CLONE_READ_BATCH_SIZE"))
	if batchSizeBytes == 0 {
		return DefaultCloneReadBatchSizeBytes
	}

	return int32(min(batchSizeBytes, math.MaxInt32)) //nolint:gosec
}
