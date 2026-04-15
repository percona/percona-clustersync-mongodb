package repl

import (
	"context"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/log"
	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/metrics"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
)

//nolint:gochecknoglobals
var yes = true // for ref

//nolint:gochecknoglobals
var simpleCollation = &options.Collation{Locale: "simple"}

//nolint:gochecknoglobals
var clientBulkOptions = options.ClientBulkWrite().
	SetOrdered(true).
	SetBypassDocumentValidation(false)

//nolint:gochecknoglobals
var collectionBulkOptions = options.BulkWrite().
	SetOrdered(true).
	SetBypassDocumentValidation(false)

const (
	followUpOverflowActionFail = "fail"
	followUpOverflowActionWarn = "warn"
)

type followUpGuard struct {
	maxOps int
	action string
}

// Pipeline $set operation limits to prevent MongoDB's BufBuilder overflow (error 13548).
// MongoDB's BufBuilder accumulates across ALL stages within a single pipeline, so splitting
// into multiple $set stages within one pipeline has no effect. Instead, when the batched $set
// fields exceed these limits, they are emitted as separate standard (non-pipeline) updateOne
// operations, each with its own BufBuilder. maxBytesPerSetOp is the primary guard;
// maxFieldsPerSetOp is a secondary guard against degenerate cases with many tiny fields.
const (
	maxFieldsPerSetOp = 100        //nolint:mnd
	maxBytesPerSetOp  = 512 * 1024 //nolint:mnd // 512KB
)

// updateOps holds the result of building update operations from a change stream event.
// When pipeline $set fields exceed size limits, they are split into follow-up standard
// (non-pipeline) $set operations to prevent MongoDB's BufBuilder overflow (error 13548).
// Follow-ups use standard $set (bson.D) because pipeline $set treats numeric path
// components as document field names rather than array indices (e.g. "arr.0.d" in a
// pipeline creates field "0" in each element instead of navigating to arr[0].d).
// Standard $set correctly navigates array indices via dotted paths.
type updateOps struct {
	// primary is the main update: either bson.D (simple update) or bson.A (pipeline).
	primary any
	// followUp contains additional update operations split out of the primary update to
	// avoid BufBuilder/AST overflow risks. Each element is either:
	//   - bson.D (standard $set update)
	//   - bson.A (pipeline update with bounded $concatArrays stages)
	// Ordered bulk writes guarantee these follow-up operations execute sequentially.
	followUp []any
	// stats captures chunking/follow-up diagnostics for observability.
	stats updateOpStats
}

type updateOpStats struct {
	followUpTotal    int
	followUpStandard int
	followUpPipeline int

	arrayChunks             int
	arrayChunkLimitByBytes  int
	arrayChunkLimitByStages int
	arrayMaxStagesPerChunk  int

	nonArrayChunks             int
	nonArrayChunkLimitByBytes  int
	nonArrayChunkLimitByStages int

	chunkingTriggered bool
}

type bulkWriter interface {
	Full() bool
	Empty() bool
	Do(ctx context.Context, m *mongo.Client) (int, error)

	Insert(ns catalog.Namespace, event *InsertEvent)
	Update(ns catalog.Namespace, event *UpdateEvent)
	Replace(ns catalog.Namespace, event *ReplaceEvent)
	Delete(ns catalog.Namespace, event *DeleteEvent)
}

type clientBulkWrite struct {
	useSimpleCollation bool
	guard              followUpGuard
	pendingErr         error
	writes             []mongo.ClientBulkWrite
}

func newClientBulkWriter(size int, useSimpleCollation bool, guard followUpGuard) *clientBulkWrite {
	return &clientBulkWrite{
		useSimpleCollation: useSimpleCollation,
		guard:              guard,
		writes:             make([]mongo.ClientBulkWrite, 0, size),
	}
}

func (cbw *clientBulkWrite) Full() bool {
	return len(cbw.writes) == cap(cbw.writes)
}

func (cbw *clientBulkWrite) Empty() bool {
	return len(cbw.writes) == 0
}

func (cbw *clientBulkWrite) Do(ctx context.Context, m *mongo.Client) (int, error) {
	if cbw.pendingErr != nil {
		return 0, cbw.pendingErr
	}

	totalSize := len(cbw.writes)

	err := cbw.doWithRetry(ctx, m, cbw.writes)
	if err != nil {
		return 0, err
	}

	clear(cbw.writes)
	cbw.writes = cbw.writes[:0]

	return totalSize, nil
}

// doWithRetry executes bulk write operations with retry logic for duplicate key errors.
// In ordered mode, when an error occurs at index N, operations 0..N-1 are applied,
// operation N fails, and N+1..end are never executed. This function handles operation N
// and retries the remaining operations recursively.
func (cbw *clientBulkWrite) doWithRetry(
	ctx context.Context,
	m *mongo.Client,
	bulkWrites []mongo.ClientBulkWrite,
) error {
	if len(bulkWrites) == 0 {
		return nil
	}

	var bulkErr error

	err := mdb.RetryWithBackoff(ctx, func() error {
		_, err := m.BulkWrite(ctx, bulkWrites, clientBulkOptions)
		bulkErr = err

		return errors.Wrap(err, "bulk write")
	}, isNonTransient, mdb.DefaultRetryInterval, maxWriteRetryDelay, 0)
	if err == nil {
		return nil
	}

	// Try to handle duplicate key error with fallback
	idx, replacement := cbw.extractDuplicateKeyReplacement(bulkErr, bulkWrites)
	if replacement == nil {
		return err //nolint:wrapcheck
	}

	write := bulkWrites[idx]
	coll := m.Database(write.Database).Collection(write.Collection)

	err = handleDuplicateKeyError(ctx, coll, replacement)
	if err != nil {
		return err
	}

	// Retry remaining operations (from index+1 onwards)
	// These operations were never executed due to ordered semantics
	return cbw.doWithRetry(ctx, m, bulkWrites[idx+1:])
}

// extractDuplicateKeyReplacement checks if the error is a duplicate key error on a ReplaceOne
// operation and returns the index and replacement document. Returns -1, nil if not applicable.
func (cbw *clientBulkWrite) extractDuplicateKeyReplacement(
	bulkErr error,
	writes []mongo.ClientBulkWrite,
) (int, any) {
	var bwe mongo.ClientBulkWriteException
	if !errors.As(bulkErr, &bwe) || len(bwe.WriteErrors) == 0 {
		return -1, nil
	}

	// Find the minimum index in the WriteErrors map
	// (in ordered mode, there should only be one error)
	minIdx := -1
	for idx := range bwe.WriteErrors {
		if minIdx == -1 || idx < minIdx {
			minIdx = idx
		}
	}

	firstErr := bwe.WriteErrors[minIdx]
	if !mongo.IsDuplicateKeyError(firstErr) || minIdx < 0 || minIdx >= len(writes) {
		return -1, nil
	}

	replaceModel, ok := writes[minIdx].Model.(*mongo.ClientReplaceOneModel)
	if !ok {
		return -1, nil
	}

	return minIdx, replaceModel.Replacement
}

func (cbw *clientBulkWrite) Insert(ns catalog.Namespace, event *InsertEvent) {
	m := &mongo.ClientReplaceOneModel{
		Filter:      event.DocumentKey,
		Replacement: event.FullDocument,
		Upsert:      &yes,
	}

	if ns.Sharded && cbw.useSimpleCollation {
		m.Collation = simpleCollation
	}

	bw := mongo.ClientBulkWrite{
		Database:   ns.Database,
		Collection: ns.Collection,
		Model:      m,
	}

	cbw.writes = append(cbw.writes, bw)
}

func (cbw *clientBulkWrite) Update(ns catalog.Namespace, event *UpdateEvent) {
	ops := collectUpdateOps(event)
	observeUpdateOpStats(ops.stats)
	logLargeUpdateChunking(ns, ops.stats)
	if err := checkFollowUpGuard(cbw.guard, ns, ops.stats); err != nil {
		cbw.pendingErr = err
		return
	}

	m := &mongo.ClientUpdateOneModel{
		Filter: event.DocumentKey,
		Update: ops.primary,
	}

	if ns.Sharded && cbw.useSimpleCollation {
		m.Collation = simpleCollation
	}

	cbw.writes = append(cbw.writes, mongo.ClientBulkWrite{
		Database: ns.Database, Collection: ns.Collection, Model: m,
	})

	// Follow-up operations for fields split out due to BufBuilder/AST limits.
	// Each is a separate updateOne (bson.D) so MongoDB resets its BufBuilder per operation.
	// Ordered bulk writes guarantee sequential execution.
	for _, followUp := range ops.followUp {
		fm := &mongo.ClientUpdateOneModel{
			Filter: event.DocumentKey,
			Update: followUp,
		}

		if ns.Sharded && cbw.useSimpleCollation {
			fm.Collation = simpleCollation
		}

		cbw.writes = append(cbw.writes, mongo.ClientBulkWrite{
			Database: ns.Database, Collection: ns.Collection, Model: fm,
		})
	}
}

func (cbw *clientBulkWrite) Replace(ns catalog.Namespace, event *ReplaceEvent) {
	m := &mongo.ClientReplaceOneModel{
		Filter:      event.DocumentKey,
		Replacement: event.FullDocument,
	}

	if ns.Sharded && cbw.useSimpleCollation {
		m.Collation = simpleCollation
	}

	bw := mongo.ClientBulkWrite{
		Database:   ns.Database,
		Collection: ns.Collection,
		Model:      m,
	}

	cbw.writes = append(cbw.writes, bw)
}

func (cbw *clientBulkWrite) Delete(ns catalog.Namespace, event *DeleteEvent) {
	m := &mongo.ClientDeleteOneModel{
		Filter: event.DocumentKey,
	}

	if ns.Sharded && cbw.useSimpleCollation {
		m.Collation = simpleCollation
	}

	bw := mongo.ClientBulkWrite{
		Database:   ns.Database,
		Collection: ns.Collection,
		Model:      m,
	}

	cbw.writes = append(cbw.writes, bw)
}

type collectionBulkWrite struct {
	useSimpleCollation bool
	guard              followUpGuard
	pendingErr         error
	max                int
	count              int
	writes             map[string][]mongo.WriteModel
}

func newCollectionBulkWriter(size int, nonDefaultCollationSupport bool, guard followUpGuard) *collectionBulkWrite {
	return &collectionBulkWrite{
		useSimpleCollation: nonDefaultCollationSupport,
		guard:              guard,
		max:                size,
		writes:             make(map[string][]mongo.WriteModel),
	}
}

func (cbw *collectionBulkWrite) Full() bool {
	return cbw.count == cbw.max
}

func (cbw *collectionBulkWrite) Empty() bool {
	return cbw.count == 0
}

func (cbw *collectionBulkWrite) Do(ctx context.Context, m *mongo.Client) (int, error) {
	if cbw.pendingErr != nil {
		return 0, cbw.pendingErr
	}

	var total atomic.Int64

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.SetLimit(runtime.NumCPU())

	for ns, ops := range cbw.writes {
		namespace, err := catalog.ParseNamespace(ns)
		if err != nil {
			return 0, errors.Wrapf(err, "parse namespace %q", namespace)
		}

		grp.Go(func() error {
			mcoll := m.Database(namespace.Database).Collection(namespace.Collection)

			err := cbw.doWithRetry(grpCtx, mcoll, namespace, ops)
			if err != nil {
				return err
			}

			total.Add(int64(len(ops)))

			return nil
		})
	}

	err := grp.Wait()
	if err != nil {
		return 0, err //nolint:wrapcheck
	}

	clear(cbw.writes)
	cbw.count = 0

	return int(total.Load()), nil
}

// doWithRetry executes bulk write operations for a single namespace with retry logic for duplicate key errors.
// In ordered mode, when an error occurs at index N, operations 0..N-1 are applied,
// operation N fails, and N+1..end are never executed. This function handles operation N
// and retries the remaining operations recursively.
func (cbw *collectionBulkWrite) doWithRetry(
	ctx context.Context,
	coll *mongo.Collection,
	ns catalog.Namespace,
	bulkWrites []mongo.WriteModel,
) error {
	if len(bulkWrites) == 0 {
		return nil
	}

	var bulkErr error

	err := mdb.RetryWithBackoff(ctx, func() error {
		_, err := coll.BulkWrite(ctx, bulkWrites, collectionBulkOptions)
		bulkErr = err

		return errors.Wrapf(err, "bulk write %q", ns)
	}, isNonTransient, mdb.DefaultRetryInterval, maxWriteRetryDelay, 0)
	if err == nil {
		return nil
	}

	// Try to handle duplicate key error with fallback
	idx, replacement := cbw.extractDuplicateKeyReplacement(bulkErr, bulkWrites)
	if replacement == nil {
		return err //nolint:wrapcheck
	}

	err = handleDuplicateKeyError(ctx, coll, replacement)
	if err != nil {
		return err
	}

	// Retry remaining operations (from index+1 onwards)
	// These operations were never executed due to ordered semantics
	return cbw.doWithRetry(ctx, coll, ns, bulkWrites[idx+1:])
}

// extractDuplicateKeyReplacement checks if the error is a duplicate key error on a ReplaceOne
// operation and returns the index and replacement document. Returns -1, nil if not applicable.
func (cbw *collectionBulkWrite) extractDuplicateKeyReplacement(
	bulkErr error,
	ops []mongo.WriteModel,
) (int, any) {
	var bwe mongo.BulkWriteException
	if !errors.As(bulkErr, &bwe) || len(bwe.WriteErrors) == 0 {
		return -1, nil
	}

	firstErr := bwe.WriteErrors[0]
	if !mongo.IsDuplicateKeyError(firstErr) || firstErr.Index < 0 || firstErr.Index >= len(ops) {
		return -1, nil
	}

	replaceModel, ok := ops[firstErr.Index].(*mongo.ReplaceOneModel)
	if !ok {
		return -1, nil
	}

	return firstErr.Index, replaceModel.Replacement
}

func (cbw *collectionBulkWrite) Insert(ns catalog.Namespace, event *InsertEvent) {
	missingShardKeys := bson.D{}

	if ns.Sharded && ns.ShardKey != nil {
		for _, k := range ns.ShardKey {
			_, err := event.FullDocument.LookupErr(k.Key)
			if err != nil {
				missingShardKeys = append(missingShardKeys, bson.E{Key: k.Key, Value: nil})
			}
		}
	}

	// we need to add shard key fields with null values to the filter for replaceOne to work
	// for documents missing shard key fields in the fullDocument
	// This is requered for MongodDB versions before 8.0
	event.DocumentKey = append(event.DocumentKey, missingShardKeys...)

	m := &mongo.ReplaceOneModel{
		Filter:      event.DocumentKey,
		Replacement: event.FullDocument,
		Upsert:      &yes,
	}

	if ns.Sharded && cbw.useSimpleCollation {
		m.Collation = simpleCollation
	}

	cbw.writes[ns.String()] = append(cbw.writes[ns.String()], m)

	cbw.count++
}

func (cbw *collectionBulkWrite) Update(ns catalog.Namespace, event *UpdateEvent) {
	ops := collectUpdateOps(event)
	observeUpdateOpStats(ops.stats)
	logLargeUpdateChunking(ns, ops.stats)
	if err := checkFollowUpGuard(cbw.guard, ns, ops.stats); err != nil {
		cbw.pendingErr = err
		return
	}

	m := &mongo.UpdateOneModel{
		Filter: event.DocumentKey,
		Update: ops.primary,
	}

	if ns.Sharded && cbw.useSimpleCollation {
		m.Collation = simpleCollation
	}

	cbw.writes[ns.String()] = append(cbw.writes[ns.String()], m)
	cbw.count++

	// Follow-up operations for fields split out due to BufBuilder/AST limits.
	for _, followUp := range ops.followUp {
		fm := &mongo.UpdateOneModel{
			Filter: event.DocumentKey,
			Update: followUp,
		}

		if ns.Sharded && cbw.useSimpleCollation {
			fm.Collation = simpleCollation
		}

		cbw.writes[ns.String()] = append(cbw.writes[ns.String()], fm)
		cbw.count++
	}
}

func (cbw *collectionBulkWrite) Replace(ns catalog.Namespace, event *ReplaceEvent) {
	m := &mongo.ReplaceOneModel{
		Filter:      event.DocumentKey,
		Replacement: event.FullDocument,
	}

	if ns.Sharded && cbw.useSimpleCollation {
		m.Collation = simpleCollation
	}

	cbw.writes[ns.String()] = append(cbw.writes[ns.String()], m)

	cbw.count++
}

func (cbw *collectionBulkWrite) Delete(ns catalog.Namespace, event *DeleteEvent) {
	m := &mongo.DeleteOneModel{
		Filter: event.DocumentKey,
	}

	if ns.Sharded && cbw.useSimpleCollation {
		m.Collation = simpleCollation
	}

	cbw.writes[ns.String()] = append(cbw.writes[ns.String()], m)

	cbw.count++
}

// handleDuplicateKeyError handles a duplicate key error on ReplaceOne by performing delete+insert.
func handleDuplicateKeyError(ctx context.Context, coll *mongo.Collection, replacement any) error {
	// Extract _id from the replacement document
	var doc bson.D

	data, err := bson.Marshal(replacement)
	if err != nil {
		return errors.Wrap(err, "marshal replacement document")
	}

	err = bson.Unmarshal(data, &doc)
	if err != nil {
		return errors.Wrap(err, "unmarshal replacement document")
	}

	// Find _id in document
	var docID any
	for _, elem := range doc {
		if elem.Key == "_id" {
			docID = elem.Value

			break
		}
	}

	if docID == nil {
		return errors.New("no _id found in replacement document")
	}

	log.Ctx(ctx).With(log.NS(coll.Database().Name(), coll.Name())).
		Infof("Retrying with delete+insert fallback for _id: %v", docID)

	_, err = coll.DeleteOne(ctx, bson.D{{"_id", docID}})
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return errors.Wrap(err, "delete before insert")
	}

	_, err = coll.InsertOne(ctx, replacement)
	if err != nil {
		return errors.Wrap(err, "insert after delete")
	}

	return nil
}

func collectUpdateOps(event *UpdateEvent) updateOps {
	for _, trunc := range event.UpdateDescription.TruncatedArrays {
		for _, update := range event.UpdateDescription.UpdatedFields {
			if update.Key == trunc.Field || strings.HasPrefix(update.Key, trunc.Field+".") {
				return collectUpdateOpsWithPipeline(event) // there is conflict field update
			}
		}
	}

	ops := make(bson.D, 0, 1)

	if len(event.UpdateDescription.UpdatedFields) != 0 {
		ops = append(ops, bson.E{"$set", event.UpdateDescription.UpdatedFields})
	}

	if len(event.UpdateDescription.RemovedFields) != 0 {
		fields := make(bson.D, len(event.UpdateDescription.RemovedFields))
		for i, field := range event.UpdateDescription.RemovedFields {
			fields[i].Key = field
			fields[i].Value = 1
		}

		ops = append(ops, bson.E{"$unset", fields})
	}

	if len(event.UpdateDescription.TruncatedArrays) != 0 {
		fields := make(bson.D, len(event.UpdateDescription.TruncatedArrays))
		for i, field := range event.UpdateDescription.TruncatedArrays {
			fields[i].Key = field.Field
			fields[i].Value = bson.D{{"$each", bson.A{}}, {"$slice", field.NewSize}}
		}

		ops = append(ops, bson.E{"$push", fields})
	}

	return updateOps{primary: ops}
}

func hasNumericPathComponent(path string) bool {
	for _, part := range strings.Split(path, ".") {
		if part == "" {
			continue
		}

		if _, err := strconv.Atoi(part); err == nil {
			return true
		}
	}

	return false
}

// collectUpdateOpsNestedTruncationSafe rebuilds truncation+update conflicts as an ordered
// sequence of standard update modifiers (no pipeline stages). This avoids pipeline numeric-path
// ambiguity for nested paths like "attributes.4.value", and avoids large pipeline expression trees
// under high-rate replay.
func collectUpdateOpsNestedTruncationSafe(event *UpdateEvent) updateOps {
	var (
		primary  any
		followUp []any
		stats    updateOpStats
	)

	if len(event.UpdateDescription.TruncatedArrays) != 0 {
		fields := make(bson.D, len(event.UpdateDescription.TruncatedArrays))
		for i, field := range event.UpdateDescription.TruncatedArrays {
			fields[i].Key = field.Field
			fields[i].Value = bson.D{{"$each", bson.A{}}, {"$slice", field.NewSize}}
		}

		primary = bson.D{{Key: "$push", Value: fields}}
	}

	if len(event.UpdateDescription.UpdatedFields) != 0 {
		setChunks, chunkStats := chunkSetFields(event.UpdateDescription.UpdatedFields)
		stats.arrayChunks = len(setChunks)
		stats.arrayChunkLimitByBytes = chunkStats.limitByBytes
		stats.arrayChunkLimitByStages = chunkStats.limitByStages
		stats.arrayMaxStagesPerChunk = chunkStats.maxFieldsPerChunk
		stats.nonArrayChunks = len(setChunks)
		stats.nonArrayChunkLimitByBytes = chunkStats.limitByBytes
		stats.nonArrayChunkLimitByStages = chunkStats.limitByStages

		start := 0
		if primary == nil && len(setChunks) > 0 {
			primary = bson.D{{Key: "$set", Value: setChunks[0]}}
			start = 1
		}

		for i := start; i < len(setChunks); i++ {
			followUp = append(followUp, bson.D{{Key: "$set", Value: setChunks[i]}})
		}
	}

	if len(event.UpdateDescription.RemovedFields) != 0 {
		fields := make(bson.D, len(event.UpdateDescription.RemovedFields))
		for i, field := range event.UpdateDescription.RemovedFields {
			fields[i].Key = field
			fields[i].Value = 1
		}

		if primary == nil {
			primary = bson.D{{Key: "$unset", Value: fields}}
		} else {
			followUp = append(followUp, bson.D{{Key: "$unset", Value: fields}})
		}
	}

	if primary == nil {
		primary = bson.D{}
	}

	for _, op := range followUp {
		switch op.(type) {
		case bson.D:
			stats.followUpStandard++
		case bson.A:
			stats.followUpPipeline++
		}
	}

	stats.followUpTotal = len(followUp)
	stats.chunkingTriggered = stats.followUpTotal > 0

	return updateOps{primary: primary, followUp: followUp, stats: stats}
}

func collectUpdateOpsWithPipeline(event *UpdateEvent) updateOps {
	// Nested truncation paths with numeric components (e.g. "attributes.4.value") are unsafe
	// in pipeline $set form because numeric path components are interpreted with aggregation
	// semantics. Replay these conflicts as ordered standard modifiers instead.
	for _, ta := range event.UpdateDescription.TruncatedArrays {
		if hasNumericPathComponent(ta.Field) {
			return collectUpdateOpsNestedTruncationSafe(event)
		}
	}

	s := len(event.UpdateDescription.UpdatedFields) +
		len(event.UpdateDescription.RemovedFields) +
		len(event.UpdateDescription.TruncatedArrays)
	pipeline := make(bson.A, 0, s)

	var dp map[string][]any

	if event.UpdateDescription.DisambiguatedPaths != nil {
		dp = make(map[string][]any, len(event.UpdateDescription.DisambiguatedPaths))
		for _, path := range event.UpdateDescription.DisambiguatedPaths {
			dp[path.Key] = path.Value.(bson.A) //nolint:forcetypeassert
		}
	}

	truncatedFields := make(map[string]struct{}, len(event.UpdateDescription.TruncatedArrays))
	for _, ta := range event.UpdateDescription.TruncatedArrays {
		truncatedFields[ta.Field] = struct{}{}
	}

	// Handle truncated arrays
	for _, truncation := range event.UpdateDescription.TruncatedArrays {
		stage := bson.D{{Key: "$set", Value: bson.D{
			{Key: truncation.Field, Value: bson.D{
				{Key: "$slice", Value: bson.A{"$" + truncation.Field, truncation.NewSize}},
			}},
		}}}

		pipeline = append(pipeline, stage)
	}

	// Handle updated fields
	var arrayStages []bson.D
	var arrayFields bson.D
	var nonArrayFields bson.D

	for _, field := range event.UpdateDescription.UpdatedFields {
		if !isArrayPath(field.Key, dp, truncatedFields) {
			nonArrayFields = append(nonArrayFields, bson.E{Key: field.Key, Value: field.Value})

			continue
		}

		arrayFields = append(arrayFields, bson.E{Key: field.Key, Value: field.Value})

		parts := strings.Split(field.Key, ".")
		fieldName := strings.Join(parts[:len(parts)-1], ".")
		fieldIdx, _ := strconv.Atoi(parts[len(parts)-1])
		fieldExpr := "$" + fieldName

		stage := bson.D{{
			"$set", bson.D{
				{fieldName, bson.D{
					{"$concatArrays", bson.A{
						bson.D{{"$slice", bson.A{fieldExpr, fieldIdx}}},
						bson.A{field.Value},
						bson.D{{
							"$slice",
							bson.A{fieldExpr, fieldIdx + 1, bson.D{{"$max", bson.A{1, bson.D{{"$size", fieldExpr}}}}}},
						}},
					}},
				}},
			},
		}}

		arrayStages = append(arrayStages, stage)
	}

	// Emit non-array $set fields as separate standard (non-pipeline) $set operations.
	// They MUST NOT go into the primary pipeline for two reasons:
	// 1. MongoDB's BufBuilder accumulates across ALL stages within a single pipeline,
	//    so even a single $set stage with many large dotted-path values can overflow.
	// 2. Pipeline $set treats numeric dotted-path components as document field names
	//    rather than array indices (e.g. "arr.0.d" creates field "0" in each element
	//    instead of navigating to arr[0].d). Standard $set handles this correctly.
	// Fields are chunked by size/count to keep individual updates manageable.
	var (
		followUp []any
		stats    updateOpStats
	)

	nonArraySetChunks, nonArrayChunkStats := chunkSetFields(nonArrayFields)
	for _, setChunk := range nonArraySetChunks {
		followUp = append(followUp, bson.D{{Key: "$set", Value: setChunk}})
	}

	stats.nonArrayChunks = len(nonArraySetChunks)
	stats.nonArrayChunkLimitByBytes = nonArrayChunkStats.limitByBytes
	stats.nonArrayChunkLimitByStages = nonArrayChunkStats.limitByStages

	// For nested truncation-conflict events (e.g. "attributes.4.value" + "attributes.4.value.N"),
	// avoid applying array index updates via pipeline $concatArrays stages. In production we
	// observed this shape can still hit target-side BufBuilder/AST limits under load.
	//
	// Ordered bulk writes preserve deterministic operation order:
	//   1) truncation pipeline stage(s)
	//   2) standard $set chunks for array index updates
	useStandardArrayFollowUps := false
	if len(arrayFields) > 0 {
		for _, ta := range event.UpdateDescription.TruncatedArrays {
			// Keep legacy behavior for top-level arrays like "arr.N" to avoid broad semantic
			// changes. Apply safer standard follow-ups for nested truncated arrays only.
			if !strings.Contains(ta.Field, ".") {
				continue
			}

			prefix := ta.Field + "."
			for _, af := range arrayFields {
				if strings.HasPrefix(af.Key, prefix) {
					useStandardArrayFollowUps = true
					break
				}
			}

			if useStandardArrayFollowUps {
				break
			}
		}
	}

	if useStandardArrayFollowUps {
		arraySetChunks, arrayChunkStats := chunkSetFields(arrayFields)
		for _, setChunk := range arraySetChunks {
			followUp = append(followUp, bson.D{{Key: "$set", Value: setChunk}})
		}

		stats.arrayChunks = len(arraySetChunks)
		stats.arrayChunkLimitByBytes = arrayChunkStats.limitByBytes
		stats.arrayChunkLimitByStages = arrayChunkStats.limitByStages
		stats.arrayMaxStagesPerChunk = arrayChunkStats.maxFieldsPerChunk
	} else {
		// Split array-path $concatArrays stages into bounded pipeline operations. This
		// prevents building one very large pipeline AST for updates with many array index
		// updates in a single oplog event.
		arrayChunks, chunkStats := chunkPipelineStages(arrayStages)
		stats.arrayChunks = len(arrayChunks)
		stats.arrayChunkLimitByBytes = chunkStats.limitByBytes
		stats.arrayChunkLimitByStages = chunkStats.limitByStages
		stats.arrayMaxStagesPerChunk = chunkStats.maxStagesPerChunk
		if len(arrayChunks) > 0 {
			// Keep the first chunk in the primary pipeline and emit remaining chunks as
			// follow-up pipeline updates (ordered execution).
			pipeline = append(pipeline, toPipelineArray(arrayChunks[0])...)
			for i := 1; i < len(arrayChunks); i++ {
				followUp = append(followUp, toPipelineArray(arrayChunks[i]))
			}
		}
	}

	// Handle removed fields
	if len(event.UpdateDescription.RemovedFields) != 0 {
		pipeline = append(
			pipeline,
			bson.D{{Key: "$unset", Value: event.UpdateDescription.RemovedFields}},
		)
	}

	for _, op := range followUp {
		switch op.(type) {
		case bson.D:
			stats.followUpStandard++
		case bson.A:
			stats.followUpPipeline++
		}
	}

	stats.followUpTotal = len(followUp)
	stats.chunkingTriggered = stats.followUpTotal > 0

	return updateOps{primary: pipeline, followUp: followUp, stats: stats}
}

type pipelineChunkStats struct {
	limitByBytes      int
	limitByStages     int
	maxStagesPerChunk int
}

type setChunkStats struct {
	limitByBytes      int
	limitByStages     int
	maxFieldsPerChunk int
}

func chunkSetFields(fields bson.D) ([]bson.D, setChunkStats) {
	if len(fields) == 0 {
		return nil, setChunkStats{}
	}

	stats := setChunkStats{}
	chunks := make([]bson.D, 0, len(fields)/maxFieldsPerSetOp+1)
	start := 0
	chunkBytes := 0

	for i := range fields {
		b, _ := bson.Marshal(bson.D{fields[i]})
		chunkBytes += len(b)

		limitByBytes := chunkBytes >= maxBytesPerSetOp
		limitByStages := (i - start + 1) >= maxFieldsPerSetOp
		if !limitByBytes && !limitByStages {
			continue
		}

		if limitByBytes {
			stats.limitByBytes++
		}

		if limitByStages {
			stats.limitByStages++
		}

		chunk := fields[start : i+1]
		chunks = append(chunks, chunk)
		if len(chunk) > stats.maxFieldsPerChunk {
			stats.maxFieldsPerChunk = len(chunk)
		}

		start = i + 1
		chunkBytes = 0
	}

	if start < len(fields) {
		chunk := fields[start:]
		chunks = append(chunks, chunk)
		if len(chunk) > stats.maxFieldsPerChunk {
			stats.maxFieldsPerChunk = len(chunk)
		}
	}

	return chunks, stats
}

func chunkPipelineStages(stages []bson.D) ([][]bson.D, pipelineChunkStats) {
	if len(stages) == 0 {
		return nil, pipelineChunkStats{}
	}

	stats := pipelineChunkStats{}
	chunks := make([][]bson.D, 0, len(stages)/maxFieldsPerSetOp+1)
	start := 0
	chunkBytes := 0

	for i := range stages {
		b, _ := bson.Marshal(stages[i])
		chunkBytes += len(b)

		limitByBytes := chunkBytes >= maxBytesPerSetOp
		limitByStages := (i - start + 1) >= maxFieldsPerSetOp
		if !limitByBytes && !limitByStages {
			continue
		}

		if limitByBytes {
			stats.limitByBytes++
		}

		if limitByStages {
			stats.limitByStages++
		}

		stagesInChunk := i - start + 1
		if stagesInChunk > stats.maxStagesPerChunk {
			stats.maxStagesPerChunk = stagesInChunk
		}

		if limitByBytes || limitByStages {
			chunks = append(chunks, stages[start:i+1])
			start = i + 1
			chunkBytes = 0
		}
	}

	if start < len(stages) {
		remaining := stages[start:]
		chunks = append(chunks, remaining)
		if len(remaining) > stats.maxStagesPerChunk {
			stats.maxStagesPerChunk = len(remaining)
		}
	}

	return chunks, stats
}

func toPipelineArray(stages []bson.D) bson.A {
	pipeline := make(bson.A, 0, len(stages))
	for _, stage := range stages {
		pipeline = append(pipeline, stage)
	}

	return pipeline
}

func observeUpdateOpStats(stats updateOpStats) {
	if !stats.chunkingTriggered {
		return
	}

	metrics.IncReplUpdateChunkingTriggered()
	metrics.ObserveReplUpdateFollowUpPerEvent(stats.followUpTotal)
	metrics.ObserveReplUpdateArrayChunksPerEvent(stats.arrayChunks)

	if stats.arrayMaxStagesPerChunk > 0 {
		metrics.ObserveReplUpdateArrayMaxStagesPerChunk(stats.arrayMaxStagesPerChunk)
	}

	if stats.followUpStandard > 0 {
		metrics.AddReplUpdateFollowUpOps("standard", stats.followUpStandard)
	}

	if stats.followUpPipeline > 0 {
		metrics.AddReplUpdateFollowUpOps("pipeline", stats.followUpPipeline)
	}

	if stats.nonArrayChunkLimitByBytes > 0 {
		metrics.AddReplUpdateChunkLimitHits("non_array_set", "bytes", stats.nonArrayChunkLimitByBytes)
	}

	if stats.nonArrayChunkLimitByStages > 0 {
		metrics.AddReplUpdateChunkLimitHits("non_array_set", "stages", stats.nonArrayChunkLimitByStages)
	}

	if stats.arrayChunkLimitByBytes > 0 {
		metrics.AddReplUpdateChunkLimitHits("array_pipeline", "bytes", stats.arrayChunkLimitByBytes)
	}

	if stats.arrayChunkLimitByStages > 0 {
		metrics.AddReplUpdateChunkLimitHits("array_pipeline", "stages", stats.arrayChunkLimitByStages)
	}
}

func logLargeUpdateChunking(ns catalog.Namespace, stats updateOpStats) {
	if !stats.chunkingTriggered {
		return
	}

	// Log only pathological events to keep production logs quiet.
	if stats.followUpTotal < 10 && stats.arrayChunks < 8 {
		return
	}

	log.New("repl:bulk").
		With(
			log.NS(ns.Database, ns.Collection),
			log.Int64("follow_up_total", int64(stats.followUpTotal)),
			log.Int64("follow_up_standard", int64(stats.followUpStandard)),
			log.Int64("follow_up_pipeline", int64(stats.followUpPipeline)),
			log.Int64("array_chunks", int64(stats.arrayChunks)),
			log.Int64("array_max_stages_per_chunk", int64(stats.arrayMaxStagesPerChunk)),
			log.Int64("array_limit_hits_bytes", int64(stats.arrayChunkLimitByBytes)),
			log.Int64("array_limit_hits_stages", int64(stats.arrayChunkLimitByStages)),
			log.Int64("non_array_limit_hits_bytes", int64(stats.nonArrayChunkLimitByBytes)),
			log.Int64("non_array_limit_hits_stages", int64(stats.nonArrayChunkLimitByStages)),
		).
		Warn("Large update event was split into multiple follow-up operations")
}

func checkFollowUpGuard(guard followUpGuard, ns catalog.Namespace, stats updateOpStats) error {
	if guard.maxOps <= 0 || stats.followUpTotal <= guard.maxOps {
		return nil
	}

	metrics.IncReplUpdateFollowUpOverflow(guard.action)

	msg := errors.Errorf(
		"follow-up operation limit exceeded for %s.%s: got %d (max %d)",
		ns.Database,
		ns.Collection,
		stats.followUpTotal,
		guard.maxOps,
	)

	switch guard.action {
	case followUpOverflowActionWarn:
		log.New("repl:bulk").With(
			log.NS(ns.Database, ns.Collection),
			log.Int64("follow_up_total", int64(stats.followUpTotal)),
			log.Int64("follow_up_limit", int64(guard.maxOps)),
		).Warn("Follow-up operation count exceeded configured limit (warn mode)")
		return nil
	default:
		return msg
	}
}

// isArrayPath checks if the path is an path to an array index (e.g. "a.b.1").
func isArrayPath(field string, disambiguatedPaths map[string][]any, truncatedFields map[string]struct{}) bool {
	// Case 1: disambiguatedPaths[field] exists → check LAST component only
	if path, ok := disambiguatedPaths[field]; ok {
		if len(path) == 0 {
			return false
		}

		lastComponent := path[len(path)-1]
		switch lastComponent.(type) {
		case int, int8, int16, int32, int64:
			return true
		default:
			return false
		}
	}

	// Case 2: disambiguatedPaths is nil (MongoDB <6.1) → use truncatedFields only.
	//
	// Without disambiguatedPaths, a path like "arr.0.10" is ambiguous: "10" could be
	// an array index (→ $concatArrays needed) or a document field name (→ standard $set).
	// The previous depth heuristic (len > 2 → assume array index) caused data corruption
	// for documents with numeric-string field names inside array elements.
	//
	// The only reliable indicator available without disambiguatedPaths is whether the
	// direct parent path is in truncatedFields. $concatArrays is needed precisely when
	// the truncated array is the direct parent of the updated index — e.g., "arr.5" when
	// "arr" was truncated. For deeper paths like "arr.0.10", the parent "arr.0" was not
	// truncated, so standard $set handles it correctly regardless of whether "10" is an
	// index or a field name.
	if disambiguatedPaths == nil {
		parts := strings.Split(field, ".")
		if len(parts) < 2 { //nolint:mnd
			return false
		}

		// Check if last segment is numeric
		_, err := strconv.Atoi(parts[len(parts)-1])
		if err != nil {
			return false
		}

		// Only use $concatArrays when the direct parent was truncated.
		parentPath := strings.Join(parts[:len(parts)-1], ".")
		_, ok := truncatedFields[parentPath]

		return ok
	}

	// Case 3: disambiguatedPaths non-nil but field not in it → Atoi fallback (unambiguous)
	parts := strings.Split(field, ".")
	if len(parts) < 2 { //nolint:mnd
		return false
	}

	_, err := strconv.Atoi(parts[len(parts)-1])

	return err == nil
}
