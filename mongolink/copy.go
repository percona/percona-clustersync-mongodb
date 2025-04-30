package mongolink

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona-lab/percona-mongolink/config"
	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
	"github.com/percona-lab/percona-mongolink/metrics"
	"github.com/percona-lab/percona-mongolink/topo"
	"github.com/percona-lab/percona-mongolink/util"
)

var (
	// errEOC indicates the end of a collection is reached.
	errEOC = errors.New("end of collection")
	// errEOS indicates the end of a segment is reached.
	errEOS = errors.New("end of segment")
)

// CopyManager orchestrates the cloning process by managing read and insert workers,
// handling parallel collection cloning, batching, and segmentation.
// It encapsulates the logic needed to coordinate concurrent operations and maintain progress.
type CopyManager struct {
	source  *mongo.Client      // source MongoDB client
	target  *mongo.Client      // target MongoDB client
	options CopyManagerOptions // user-defined options for the clone process

	insertQueue chan insertBatchTask // channel for insert batch tasks
	close       func()               // function to stop workers and clean up resources
	collGroup   sync.WaitGroup       // tracks active collections being processed
	readLimit   chan struct{}        // semaphore to limit concurrent read workers
}

// CopyGetCollSpecFunc defines a function type that retrieves a collection's specification,
// including its type and options, required during the clone operation.
type CopyGetCollSpecFunc func(ctx context.Context) (*topo.CollectionSpecification, error)

// CopyUpdate represents the result of a clone operation update, including any error,
// the size of data transferred in bytes, and the number of documents processed.
type CopyUpdate struct {
	// Err is the error encountered during the operation, if any.
	Err error
	// SizeBytes is the size of documents inserted in bytes.
	SizeBytes uint64
	// Count is the number of documents inserted.
	Count int
}

// CopyManagerOptions configures the behavior of CopyManager.
// It controls concurrency settings and memory limits for collection cloning operations.
type CopyManagerOptions struct {
	// NumReadWorkers is the total number of concurrent read workers.
	// min: 1; default: [runtime.NumCPU] / 4.
	NumReadWorkers int
	// NumInsertWorkers is the total number of concurrent insert workers.
	// min: 1; default: [runtime.NumCPU] * 4.
	NumInsertWorkers int
	// SegmentSizeBytes is the logical segment size in bytes for splitting collections.
	// min: 192MB [config.MinCloneSegmentSizeBytes].
	// min: 64GiB [config.MaxCloneSegmentSizeBytes].
	// default: auto (per collection) [config.AutoCloneSegmentSize].
	SegmentSizeBytes int64
	// ReadBatchSizeBytes is the maximum read batch size in bytes.
	// min: 16MiB [config.MinCloneReadBatchSizeBytes].
	// max: 2GiB [config.MaxCloneReadBatchSizeBytes].
	// default: 96MB [config.DefaultCloneReadBatchSizeBytes].
	ReadBatchSizeBytes int32
}

func NewCopyManager(source, target *mongo.Client, options CopyManagerOptions) *CopyManager {
	if options.NumReadWorkers < 1 {
		options.NumReadWorkers = max(runtime.NumCPU()/4, 1) //nolint:mnd
	}
	if options.NumInsertWorkers < 1 {
		options.NumInsertWorkers = runtime.NumCPU() * 2 //nolint:mnd
	}

	if options.SegmentSizeBytes < 0 {
		options.SegmentSizeBytes = config.AutoCloneSegmentSize
	} else if options.SegmentSizeBytes > 0 {
		options.SegmentSizeBytes = max(options.SegmentSizeBytes, config.MinCloneSegmentSizeBytes)
		options.SegmentSizeBytes = min(options.SegmentSizeBytes, config.MaxCloneSegmentSizeBytes)
	}

	if options.ReadBatchSizeBytes == 0 {
		options.ReadBatchSizeBytes = config.DefaultCloneReadBatchSizeBytes
	} else {
		options.ReadBatchSizeBytes = max(options.ReadBatchSizeBytes, config.MinCloneReadBatchSizeBytes)
		options.ReadBatchSizeBytes = min(options.ReadBatchSizeBytes, config.MaxCloneReadBatchSizeBytes)
	}

	lg := log.New("copy")
	lg.Debugf("NumReadWorkers: %d", options.NumReadWorkers)
	lg.Debugf("NumInsertWorkers: %d", options.NumInsertWorkers)
	if options.SegmentSizeBytes == config.AutoCloneSegmentSize {
		lg.Debug("SegmentSizeBytes: auto") //nolint:gosec
	} else {
		lg.Debugf("SegmentSizeBytes: %d (%s)", options.SegmentSizeBytes,
			humanize.Bytes(uint64(options.SegmentSizeBytes))) //nolint:gosec
	}
	lg.Debugf("ReadBatchSizeBytes: %d (%s)", options.ReadBatchSizeBytes,
		humanize.Bytes(uint64(options.ReadBatchSizeBytes))) //nolint:gosec

	insertCtx, cancelInsert := context.WithCancel(context.Background())

	cm := &CopyManager{
		source:  source,
		target:  target,
		options: options,

		insertQueue: make(chan insertBatchTask),
		readLimit:   make(chan struct{}, options.NumReadWorkers),
	}

	// Start an insert worker goroutine that processes insertBatchTask from the queue.
	// Each worker receives document batches and inserts them into the target collection.
	for id := range cm.options.NumInsertWorkers {
		go func() {
			lg := log.New(fmt.Sprintf("copy:w:i:%d", id+1))
			lg.Tracef("Insert Worker %d has started", id+1)

			for t := range cm.insertQueue {
				l := lg.With(log.NS(t.Namespace.Database, t.Namespace.Collection))
				cm.insertBatch(l.WithContext(insertCtx), t)
			}
		}()
	}

	var once sync.Once
	cm.close = func() {
		once.Do(func() {
			cancelInsert()
			cm.collGroup.Wait()
			close(cm.readLimit)
			close(cm.insertQueue)
		})
	}

	return cm
}

// Close gracefully stops all background workers and releases resources held by the CopyManager.
// It waits for all active copy operations to complete before shutting down.
func (cm *CopyManager) Close() {
	cm.close()
}

// Do starts a clone operation for the specified namespace.
// It launches asynchronous workers to read from the source collection and insert into the target
// collection.
// The provided getSpec function retrieves the collection specification needed before cloning.
// It returns a channel of CopyUpdate values that report progress or errors from the operation.
func (cm *CopyManager) Do(
	ctx context.Context,
	namespace Namespace,
	getSpec CopyGetCollSpecFunc,
) <-chan CopyUpdate {
	updateC := make(chan CopyUpdate, cm.options.NumInsertWorkers)

	cm.collGroup.Add(1)
	go func() {
		defer func() { close(updateC); cm.collGroup.Done() }()

		lg := log.New("copy").With(log.NS(namespace.Database, namespace.Collection))
		err := cm.copyCollection(lg.WithContext(ctx), namespace, getSpec, updateC)
		if err != nil {
			updateC <- CopyUpdate{Err: err}
		}
	}()

	return updateC
}

type (
	nextSegmentFunc func(context.Context) (*mongo.Cursor, error)
	nextBatchIDFunc func() uint32
)

func (cm *CopyManager) copyCollection(
	ctx context.Context,
	namespace Namespace,
	getSpec CopyGetCollSpecFunc,
	updateC chan<- CopyUpdate,
) error {
	spec, err := getSpec(ctx)
	if err != nil {
		return errors.Wrap(err, "get namespace specification")
	}

	switch spec.Type {
	case topo.TypeTimeseries:
		return ErrTimeseriesUnsupported
	case topo.TypeView:
		return nil
	}

	isCapped, _ := spec.Options.Lookup("capped").BooleanOK()

	var nextSegment nextSegmentFunc
	if isCapped { //nolint:nestif
		segmenter, err := NewCappedSegmenter(ctx,
			cm.source, namespace, cm.options.ReadBatchSizeBytes)
		if err != nil {
			if errors.Is(err, errEOC) {
				return nil
			}

			return errors.Wrap(err, "create capped segmenter")
		}

		nextSegment = segmenter.Next

		log.New("clone").With(log.NS(namespace.Database, namespace.Collection)).
			Debugf("Capped collection %q: copy sequentially", namespace)
	} else {
		segmenter, err := NewSegmenter(ctx, cm.source, namespace, SegmentOptions{
			SegmentSizeBytes: cm.options.SegmentSizeBytes,
			BatchSizeBytes:   cm.options.ReadBatchSizeBytes,
			AutoNumSegment:   cm.options.NumReadWorkers,
		})
		if err != nil {
			if errors.Is(err, errEOC) {
				return nil
			}

			return errors.Wrap(err, "create segmenter")
		}

		nextSegment = segmenter.Next
	}

	collectionReadCtx, stopCollectionRead := context.WithCancel(ctx)

	// pendingSegments tracks in-progress read segments
	pendingSegments := &sync.WaitGroup{}
	readResultC := make(chan readBatchResult)

	allBatchesSent := make(chan struct{}) // closes when all batches are sent to inserters

	// pendingInserts tracks in-progress insert batches
	pendingInserts := &sync.WaitGroup{}
	insertResultC := make(chan insertBatchResult, cm.options.NumInsertWorkers)

	go func() { // cleanup
		<-collectionReadCtx.Done() // EOC or read error
		pendingSegments.Wait()     // all segments is read (EOS)
		close(readResultC)         // no more read batches: release send inserts routine
		<-allBatchesSent           // wait until no more new batches for inserters
		pendingInserts.Wait()      // all batches inserted
		close(insertResultC)       // exit
	}()

	// spawn readSegment in loop until the collection is exhausted or canceled.
	go func() {
		var segmentID uint32
		var batchID atomic.Uint32
		var nextID nextBatchIDFunc = func() uint32 { return batchID.Add(1) }

		readStopped := collectionReadCtx.Done()

		for {
			select {
			case <-readStopped: // when a worker fails
				return

			case cm.readLimit <- struct{}{}:
				segmentID++
			}

			ctx := log.New(fmt.Sprintf("copy:w:r:%d", segmentID)).
				With(log.NS(namespace.Database, namespace.Collection)).
				WithContext(collectionReadCtx)

			cursor, err := nextSegment(ctx)
			if err != nil {
				<-cm.readLimit

				if errors.Is(err, errEOC) {
					pendingSegments.Wait() // wait all readers finish
				} else {
					updateC <- CopyUpdate{Err: errors.Wrap(err, "next segment")}
				}

				stopCollectionRead()

				return
			}

			pendingSegments.Add(1)
			go func() {
				defer func() {
					<-cm.readLimit
					pendingSegments.Done()

					err := util.CtxWithTimeout(context.Background(),
						config.CloseCursorTimeout, cursor.Close)
					if err != nil {
						log.Ctx(ctx).Error(err, "Close cursor")
					}
				}()

				err = cm.readSegment(ctx, readResultC, cursor, nextID)
				if err != nil {
					updateC <- CopyUpdate{Err: errors.Wrap(err, "read worker")}

					stopCollectionRead()
				}
			}()

			if isCapped {
				pendingSegments.Wait()
			}
		}
	}()

	// receives readBatchResult from read workers and sends them to insert workers.
	// For capped collections, inserting is serialized by waiting for each batch's insertion.
	go func() {
		defer close(allBatchesSent) // notify: no more new batches for inserters

		// collect batches from read workers
		for readResult := range readResultC {
			// send the batch to an insert worker
			pendingInserts.Add(1)

			cm.insertQueue <- insertBatchTask{
				Namespace: namespace,
				ID:        readResult.ID,
				SizeBytes: readResult.SizeBytes,
				Documents: readResult.Documents,
				ResultC:   insertResultC,
			}

			if isCapped {
				pendingInserts.Wait() // insert next batch after the current is inserted
			}
		}
	}()

	// collect results from insert workers. notify caller
	for insertResult := range insertResultC {
		pendingInserts.Done()

		updateC <- CopyUpdate{
			Err:       insertResult.Err,
			SizeBytes: uint64(insertResult.SizeBytes), //nolint:gosec
			Count:     insertResult.Count,
		}
	}

	return nil
}

type readBatchResult struct {
	ID        uint32
	Documents []any
	SizeBytes int
}

// readSegment reads documents from a segment cursor and sends readBatchResult to the result
// channel. It batches documents until the configured maximum batch size is reached or the cursor is
// exhausted. Each batch includes the documents, their total size, and a unique batch ID.
// Returns when the segment ends or yields nothing. Does not close the resultC channel.
// Metrics are collected for performance monitoring.
func (cm *CopyManager) readSegment(
	ctx context.Context,
	resultC chan<- readBatchResult,
	cur *mongo.Cursor,
	nextID nextBatchIDFunc,
) error {
	zl := log.Ctx(ctx).Unwrap()
	batchID := nextID()
	documents := make([]any, 0, config.MaxInsertBatchSize)
	sizeBytes := 0
	lastSentAt := time.Now()

	for cur.Next(ctx) {
		if sizeBytes+len(cur.Current) > config.MaxWriteBatchSizeBytes ||
			len(documents) == config.MaxInsertBatchSize {
			elapsed := time.Since(lastSentAt)

			zl.Trace().
				Uint32("id", batchID).
				Int("count", len(documents)).
				Int("size_bytes", sizeBytes).
				Dur("elapsed", elapsed.Round(time.Millisecond)).
				Msgf("read batch %d", batchID)

			metrics.AddCopyReadSize(uint64(sizeBytes)) //nolint:gosec
			metrics.AddCopyReadDocumentCount(len(documents))
			metrics.SetCopyReadBatchDurationSeconds(elapsed)

			resultC <- readBatchResult{
				ID:        batchID,
				Documents: documents,
				SizeBytes: sizeBytes,
			}

			batchID = nextID()
			documents = make([]any, 0, config.MaxInsertBatchSize)
			sizeBytes = 0
			lastSentAt = time.Now()
		}

		documents = append(documents, cur.Current)
		sizeBytes += len(cur.Current)
	}

	err := cur.Err()
	if err != nil {
		zl.Trace().
			Err(err).
			Uint32("id", batchID).
			Int("count", len(documents)).
			Int("size_bytes", sizeBytes).
			Dur("elapsed", time.Since(lastSentAt).Round(time.Millisecond)).
			Msgf("read batch %d", batchID)

		return errors.Wrap(err, "getMore")
	}

	if len(documents) == 0 {
		return nil
	}

	elapsed := time.Since(lastSentAt)

	zl.Trace().
		Uint32("id", batchID).
		Int("count", len(documents)).
		Int("size_bytes", sizeBytes).
		Dur("elapsed", elapsed.Round(time.Millisecond)).
		Msgf("read batch %d", batchID)

	metrics.AddCopyReadSize(uint64(sizeBytes)) //nolint:gosec
	metrics.AddCopyReadDocumentCount(len(documents))
	metrics.SetCopyReadBatchDurationSeconds(elapsed)

	resultC <- readBatchResult{
		ID:        batchID,
		Documents: documents,
		SizeBytes: sizeBytes,
	}

	return nil
}

type insertBatchTask struct {
	Namespace Namespace
	ID        uint32
	Documents []any
	SizeBytes int

	ResultC chan<- insertBatchResult
}

type insertBatchResult struct {
	ID        uint32
	SizeBytes int
	Count     int
	Err       error
}

//nolint:gochecknoglobals
var insertOptions = options.InsertMany().SetOrdered(false).SetBypassDocumentValidation(true)

// insertBatch inserts a batch of documents into the target collection. It retries once if a
// retryable write error occurs, and tolerates duplicate key errors.
// On success, it emits an insertBatchResult with size, count, and ID to the result channel.
// Metrics are collected for performance monitoring.
func (cm *CopyManager) insertBatch(ctx context.Context, task insertBatchTask) {
	zl := log.Ctx(ctx).Unwrap()

	startedAt := time.Now()

	collection := cm.target.Database(task.Namespace.Database).Collection(task.Namespace.Collection)
	_, err := collection.InsertMany(ctx, task.Documents, insertOptions)
	if topo.IsRetryableWrite(err) {
		zl.Warn().
			Err(err).
			Uint32("id", task.ID).
			Int("size_bytes", task.SizeBytes).
			Int("count", len(task.Documents)).
			Dur("elapsed", time.Since(startedAt).Round(time.Millisecond)).
			Msgf("insert batch %d [RetryableWrite]", task.ID)

		startedAt = time.Now()

		// try one more time
		_, err = collection.InsertMany(ctx, task.Documents, insertOptions)
	}

	count := len(task.Documents)

	if err != nil {
		var bulkError mongo.BulkWriteException
		if !errors.As(err, &bulkError) || bulkError.WriteConcernError != nil {
			task.ResultC <- insertBatchResult{ID: task.ID, Err: err}

			return
		}

		for _, e := range bulkError.WriteErrors {
			if !mongo.IsDuplicateKeyError(e) {
				task.ResultC <- insertBatchResult{ID: task.ID, Err: err}

				return
			}

			count-- // doc already inserted
		}

		zl.Trace().
			Str("err", err.Error()).
			Uint32("id", task.ID).
			Int("size_bytes", task.SizeBytes).
			Int("count", count).
			Dur("elapsed", time.Since(startedAt).Round(time.Millisecond)).
			Msgf("insert batch %d", task.ID)
	}

	elapsed := time.Since(startedAt)

	zl.Trace().
		Uint32("id", task.ID).
		Int("size_bytes", task.SizeBytes).
		Int("count", count).
		Dur("elapsed", elapsed.Round(time.Millisecond)).
		Msgf("inserted batch %d", task.ID)

	metrics.AddCopyInsertSize(uint64(task.SizeBytes)) //nolint:gosec
	metrics.AddCopyInsertDocumentCount(len(task.Documents))
	metrics.SetCopyInsertBatchDurationSeconds(elapsed)

	task.ResultC <- insertBatchResult{
		ID:        task.ID,
		SizeBytes: task.SizeBytes,
		Count:     count,
	}
}

// Segmenter splits a MongoDB collection into logical segments based on _id ranges.
// It enables concurrent reads over non-overlapping segments by tracking min and max keys.
// Segmenter operates sequentially through segments and supports collections with heterogeneous _id
// types.
type Segmenter struct {
	lock        sync.Mutex
	mcoll       *mongo.Collection
	segmentSize int64
	batchSize   int32
	keyRanges   []keyRange
	currIDRange keyRange
}

type keyRange struct {
	Min segmentKey `bson:"minKey"`
	Max segmentKey `bson:"maxKey"`
}

func (k *keyRange) IsZero() bool {
	return k.Min.IsZero() && k.Max.IsZero()
}

type segmentKey = bson.RawValue

var nilSegmentID segmentKey //nolint:gochecknoglobals

// SegmentOptions configures how a MongoDB collection is segmented during cloning.
// It defines the logical segment size in bytes, the read batch size, or the exact number of
// segments to create.
type SegmentOptions struct {
	SegmentSizeBytes int64
	BatchSizeBytes   int32
	AutoNumSegment   int
}

// NewSegmenter initializes a Segmenter for a given MongoDB namespace.
// It uses collection statistics to compute the segment size and read batch size.
// Based on the _id value distribution, it creates one or more key ranges:
// - If all _id values are of the same BSON type, a single range is used.
// - If heterogeneous, each type range is segmented and processed sequentially.
// Returns ErrEOC if the collection is empty.
func NewSegmenter(
	ctx context.Context,
	m *mongo.Client,
	ns Namespace,
	options SegmentOptions,
) (*Segmenter, error) {
	stats, err := topo.GetCollStats(ctx, m, ns.Database, ns.Collection)
	if err != nil {
		return nil, errors.Wrap(err, "$collStats")
	}

	if stats.AvgObjSize == 0 {
		return nil, errEOC
	}

	// AvgObjSize must be less than or equal to 16MiB [config.MaxBSONSize]
	var segmentSize int64
	if options.SegmentSizeBytes == config.AutoCloneSegmentSize {
		segmentSize = max(stats.Size/int64(options.AutoNumSegment), config.MinCloneSegmentSizeBytes)

		log.Ctx(ctx).Debugf("SegmentSizeBytes (auto): %d (%s)",
			segmentSize, humanize.Bytes(uint64(segmentSize))) //nolint:gosec
	} else {
		segmentSize = options.SegmentSizeBytes / stats.AvgObjSize
	}

	//nolint:gosec
	batchSize := int32(min(int64(options.BatchSizeBytes)/stats.AvgObjSize, math.MaxInt32))

	mcoll := m.Database(ns.Database).Collection(ns.Collection)

	idKeyRange, err := getIDKeyRange(ctx, mcoll)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, errEOC // empty collection
		}

		return nil, errors.Wrap(err, "get ID key range")
	}

	if idKeyRange.Min.Type == idKeyRange.Max.Type {
		s := &Segmenter{
			mcoll:       mcoll,
			segmentSize: segmentSize,
			batchSize:   batchSize,
			currIDRange: idKeyRange,
		}

		return s, nil
	}

	keyRangeByType, err := getIDKeyRangeByType(ctx, mcoll)
	if err != nil {
		return nil, errors.Wrap(err, "get ID key range by type")
	}

	if len(keyRangeByType) == 0 {
		return nil, errEOC // empty collection
	}

	currIDRange := keyRangeByType[0]
	keyRanges := keyRangeByType[1:]

	s := &Segmenter{
		mcoll:       mcoll,
		segmentSize: segmentSize,
		batchSize:   batchSize,
		keyRanges:   keyRanges,
		currIDRange: currIDRange,
	}

	return s, nil
}

// Next returns a cursor over the next segment of the collection based on the current _id range.
// It advances through the collection by updating internal state with the next key range.
// Returns ErrEOC when the end of the collection is reached, or ErrEOS if the current segment is
// exhausted.
func (seg *Segmenter) Next(ctx context.Context) (*mongo.Cursor, error) {
	seg.lock.Lock()
	defer seg.lock.Unlock()

	if seg.currIDRange.IsZero() {
		return nil, errEOC
	}

	for {
		cur, err := seg.doNext(ctx)
		if err == nil {
			return cur, nil // OK
		}

		if !errors.Is(err, errEOS) {
			return nil, errors.Wrap(err, "next cursor")
		}

		if len(seg.keyRanges) == 0 {
			seg.currIDRange = keyRange{}

			return nil, errEOC
		}

		seg.currIDRange = seg.keyRanges[0]

		if len(seg.keyRanges) == 1 {
			seg.keyRanges = nil
		} else {
			seg.keyRanges = seg.keyRanges[1:len(seg.keyRanges)]
		}
	}
}

// doNext executes a query for the next logical segment within the current key range.
// It determines the upper bound of the segment using findSegmentMaxKey,
// and issues a Find for [_id >= Min, _id <= Max]. It updates the current range
// so that the next call progresses to the next segment.
//
// Returns:
// - ErrEOS if the current segment is exhausted or contains no documents.
// - A cursor over the current segment if documents are found.
func (seg *Segmenter) doNext(ctx context.Context) (*mongo.Cursor, error) {
	if seg.currIDRange.Max.IsZero() {
		return nil, errEOS // previous segment was the last one
	}

	maxKey, err := seg.findSegmentMaxKey(ctx, seg.currIDRange.Min, seg.currIDRange.Max)
	if err != nil {
		return nil, errors.Wrap(err, "find segment max _id")
	}

	log.New("seg").With(log.NS(seg.mcoll.Database().Name(), seg.mcoll.Name())).
		Tracef("[%v <=> %v]", seg.currIDRange.Min, maxKey)

	cur, err := seg.mcoll.Find(ctx,
		bson.D{{"_id", bson.D{{"$gte", seg.currIDRange.Min}, {"$lte", maxKey}}}},
		options.Find().SetSort(bson.D{{"_id", 1}}).SetBatchSize(seg.batchSize))
	if err != nil {
		return nil, errors.Wrap(err, "query")
	}

	if maxKey.Equal(seg.currIDRange.Max) {
		seg.currIDRange.Max = nilSegmentID // it is the end of collection. return EOC next time
	} else {
		seg.currIDRange.Min = maxKey
	}

	return cur, nil
}

// findSegmentMaxKey determines the upper _id boundary for the current segment.
// It issues a sorted query skipping segmentSize documents, then reads the _id at that offset.
// If fewer documents are found, it returns maxKey to indicate the end of the range.
// This enables evenly sized segments when document sizes vary.
func (seg *Segmenter) findSegmentMaxKey(
	ctx context.Context,
	minKey segmentKey,
	maxKey segmentKey,
) (segmentKey, error) {
	raw, err := seg.mcoll.FindOne(ctx,
		bson.D{{"_id", bson.D{{"$gt", minKey}, {"$lte", maxKey}}}},
		options.FindOne().
			SetSort(bson.D{{"_id", 1}}).
			SetSkip(seg.segmentSize).
			SetProjection(bson.D{{"_id", 1}}),
	).Raw()
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return maxKey, nil
		}

		return nilSegmentID, err //nolint:wrapcheck
	}

	return raw.Lookup("_id"), nil
}

// getIDKeyRange returns the minimum and maximum _id values in the collection.
// It uses two FindOne operations with sort directions of 1 (ascending) and -1 (descending)
// to determine the full _id range. This is used to define the collection boundaries
// when the _id type is uniform across all documents.
func getIDKeyRange(ctx context.Context, mcoll *mongo.Collection) (keyRange, error) {
	findOptions := options.FindOne().SetSort(bson.D{{"_id", 1}}).SetProjection(bson.D{{"_id", 1}})
	minRaw, err := mcoll.FindOne(ctx, bson.D{}, findOptions).Raw()
	if err != nil {
		return keyRange{}, errors.Wrap(err, "min _id")
	}

	findOptions = options.FindOne().SetSort(bson.D{{"_id", -1}}).SetProjection(bson.D{{"_id", 1}})
	maxRaw, err := mcoll.FindOne(ctx, bson.D{}, findOptions).Raw()
	if err != nil {
		return keyRange{}, errors.Wrap(err, "max _id")
	}

	ret := keyRange{
		Min: minRaw.Lookup("_id"),
		Max: maxRaw.Lookup("_id"),
	}

	return ret, nil
}

// getIDKeyRangeByType returns a slice of keyRange grouped by the BSON type of the _id field.
// It performs an aggregation that groups documents by _id type, computing the min and max _id
// for each group. This allows the Segmenter to handle collections with heterogeneous _id types
// by processing each type range sequentially.
func getIDKeyRangeByType(ctx context.Context, mcoll *mongo.Collection) ([]keyRange, error) {
	cur, err := mcoll.Aggregate(ctx, mongo.Pipeline{
		bson.D{{"$group", bson.D{
			{"_id", bson.D{{"type", bson.D{{"$type", "$_id"}}}}},
			{"minKey", bson.D{{"$min", "$_id"}}},
			{"maxKey", bson.D{{"$max", "$_id"}}},
		}}},
	})
	if err != nil {
		return nil, errors.Wrap(err, "query")
	}

	var segmentRanges []keyRange
	err = cur.All(ctx, &segmentRanges)
	if err != nil {
		return nil, errors.Wrap(err, "all")
	}

	return segmentRanges, nil
}

// CappedSegmenter provides sequential cursor access for capped collections.
// Unlike Segmenter, it does not split the collection into multiple segments.
// It returns a single forward-only cursor over the entire collection ordered by $natural.
type CappedSegmenter struct {
	lock      sync.Mutex
	mcoll     *mongo.Collection
	batchSize int32
	endOfColl bool
}

// NewCappedSegmenter initializes a CappedSegmenter for the given capped collection.
// It estimates the optimal batch size using average document size and returns a segmenter that
// produces one sequential cursor per collection.
// Returns ErrEOC if the collection is empty.
func NewCappedSegmenter(
	ctx context.Context,
	m *mongo.Client,
	ns Namespace,
	batchSizeBytes int32,
) (*CappedSegmenter, error) {
	stats, err := topo.GetCollStats(ctx, m, ns.Database, ns.Collection)
	if err != nil {
		return nil, errors.Wrap(err, "$collStats")
	}

	if stats.AvgObjSize == 0 {
		return nil, errEOC
	}

	batchSize := int32(min(int64(batchSizeBytes)/stats.AvgObjSize, math.MaxInt32)) //nolint:gosec
	mcoll := m.Database(ns.Database).Collection(ns.Collection)

	cs := &CappedSegmenter{
		mcoll:     mcoll,
		batchSize: batchSize,
	}

	return cs, nil
}

func (cs *CappedSegmenter) Next(ctx context.Context) (*mongo.Cursor, error) {
	cs.lock.Lock()
	defer cs.lock.Unlock()

	if cs.endOfColl {
		return nil, errEOC
	}

	cur, err := cs.mcoll.Find(ctx, bson.D{},
		options.Find().SetHint(bson.D{{"$natural", 1}}).SetBatchSize(cs.batchSize))
	if err != nil {
		return nil, errors.Wrap(err, "query")
	}

	cs.endOfColl = true

	return cur, nil
}
