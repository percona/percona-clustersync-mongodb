package repl

import (
	"context"
	"runtime"
	"strings"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/log"
	"github.com/percona/percona-clustersync-mongodb/mdb"
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

// Update operation chunking limits to prevent MongoDB's 125 MB BufBuilder overflow (error
// 13548) and to keep individual update payloads bounded. When a change event combines
// truncations with conflicting array-index updates, the array-index updates are split off
// into separate $set follow-up operations, each its own update with its own MongoDB-side
// BufBuilder. maxBytesPerSetOp is the primary guard; maxFieldsPerSetOp is a secondary guard
// against degenerate cases with many tiny fields.
//
// maxBulkBytes caps the aggregate wire size of a single bulk command, well under MongoDB's
// 48 MB MaxMessageSizeBytes default. Combined with the per-update bounds above, this keeps
// both individual operations and the bulk command within MongoDB's wire-protocol limits.
const (
	maxFieldsPerSetOp = 100              //nolint:mnd
	maxBytesPerSetOp  = 512 * 1024       //nolint:mnd // 512 KiB
	maxBulkBytes      = 32 * 1024 * 1024 //nolint:mnd // 32 MiB - well under 48 MB MaxMessageSizeBytes
)

// updateOps holds the result of building update operations from a change stream event.
// When a change event combines an array truncation with conflicting indexed-write updates,
// the conflicting updates are split into follow-up $set operations so that no single update
// document carries both a $push (truncation) and a $set on the same array path - which
// MongoDB rejects as a path conflict - and so that no individual update accumulates enough
// work to exhaust MongoDB's 125 MB BufBuilder ceiling (error 13548).
type updateOps struct {
	// primary is the main update document (bson.D).
	primary any
	// followUp contains additional $set operations for array-index updates that conflict
	// with a truncation in the primary update. Each element is an update document (bson.D)
	// with a single $set operator. nil when not needed.
	followUp []bson.D
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
	bytes              int
	writes             []mongo.ClientBulkWrite
}

func newClientBulkWriter(size int, useSimpleCollation bool) *clientBulkWrite {
	return &clientBulkWrite{
		useSimpleCollation: useSimpleCollation,
		writes:             make([]mongo.ClientBulkWrite, 0, size),
	}
}

func (cbw *clientBulkWrite) Full() bool {
	return len(cbw.writes) == cap(cbw.writes) || cbw.bytes >= maxBulkBytes
}

func (cbw *clientBulkWrite) Empty() bool {
	return len(cbw.writes) == 0
}

func (cbw *clientBulkWrite) Do(ctx context.Context, m *mongo.Client) (int, error) {
	totalSize := len(cbw.writes)

	err := cbw.doWithRetry(ctx, m, cbw.writes)
	if err != nil {
		return 0, err
	}

	clear(cbw.writes)
	cbw.writes = cbw.writes[:0]
	cbw.bytes = 0

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
	cbw.bytes += estimateBytes(event.DocumentKey, event.FullDocument)
}

func (cbw *clientBulkWrite) Update(ns catalog.Namespace, event *UpdateEvent) {
	ops := collectUpdateOps(event)

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
	cbw.bytes += estimateBytes(event.DocumentKey, ops.primary)

	// Follow-up $set operations for fields split out due to BufBuilder limits.
	// Each is a separate updateOne so MongoDB resets its BufBuilder per operation.
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
		cbw.bytes += estimateBytes(event.DocumentKey, followUp)
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
	cbw.bytes += estimateBytes(event.DocumentKey, event.FullDocument)
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
	cbw.bytes += estimateBytes(event.DocumentKey, nil)
}

type collectionBulkWrite struct {
	useSimpleCollation bool
	max                int
	count              int
	bytes              int
	writes             map[string][]mongo.WriteModel
}

func newCollectionBulkWriter(size int, nonDefaultCollationSupport bool) *collectionBulkWrite {
	return &collectionBulkWrite{
		useSimpleCollation: nonDefaultCollationSupport,
		max:                size,
		writes:             make(map[string][]mongo.WriteModel),
	}
}

func (cbw *collectionBulkWrite) Full() bool {
	return cbw.count == cbw.max || cbw.bytes >= maxBulkBytes
}

func (cbw *collectionBulkWrite) Empty() bool {
	return cbw.count == 0
}

func (cbw *collectionBulkWrite) Do(ctx context.Context, m *mongo.Client) (int, error) {
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
	cbw.bytes = 0

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
	cbw.bytes += estimateBytes(event.DocumentKey, event.FullDocument)
}

func (cbw *collectionBulkWrite) Update(ns catalog.Namespace, event *UpdateEvent) {
	ops := collectUpdateOps(event)

	m := &mongo.UpdateOneModel{
		Filter: event.DocumentKey,
		Update: ops.primary,
	}

	if ns.Sharded && cbw.useSimpleCollation {
		m.Collation = simpleCollation
	}

	cbw.writes[ns.String()] = append(cbw.writes[ns.String()], m)
	cbw.count++
	cbw.bytes += estimateBytes(event.DocumentKey, ops.primary)

	// Follow-up $set operations for fields split out due to BufBuilder limits.
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
		cbw.bytes += estimateBytes(event.DocumentKey, followUp)
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
	cbw.bytes += estimateBytes(event.DocumentKey, event.FullDocument)
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
	cbw.bytes += estimateBytes(event.DocumentKey, nil)
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

// estimateBytes approximates the wire size contribution of a bulk write operation by
// marshaling its filter and payload (update doc or replacement doc). Used as the per-bulk
// byte budget signal for Full() to keep aggregate command size below MongoDB's
// MaxMessageSizeBytes wire-protocol limit. Marshal errors yield a zero estimate, which is
// fine because the count-based Full() condition still applies.
func estimateBytes(filter, payload any) int {
	return marshalLen(filter) + marshalLen(payload)
}

// marshalLen returns the marshaled BSON length of v, or 0 if v is nil or marshaling fails.
func marshalLen(v any) int {
	if v == nil {
		return 0
	}

	b, err := bson.Marshal(v)
	if err != nil {
		return 0
	}

	return len(b)
}

func collectUpdateOps(event *UpdateEvent) updateOps {
	for _, trunc := range event.UpdateDescription.TruncatedArrays {
		for _, update := range event.UpdateDescription.UpdatedFields {
			if update.Key == trunc.Field || strings.HasPrefix(update.Key, trunc.Field+".") {
				return collectUpdateOpsWithConflicts(event) // there is conflict field update
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

// collectUpdateOpsWithConflicts builds update operations for change events where one or
// more updated fields target a path inside a truncated array (e.g. truncation of "arr"
// together with a write to "arr.5"). MongoDB rejects an update document that combines a
// $push (truncation) with a $set on the same array path, so the work is split across
// multiple ordered update operations:
//
//	primary update:   $push (truncations) + $unset (removed) + $set (non-conflicting fields)
//	follow-up update: $set (conflicting fields), chunked by maxBytesPerSetOp /
//	                  maxFieldsPerSetOp to keep each individual update bounded
//
// Splitting also bounds per-update work on the target side, keeping each update under
// MongoDB's 125 MB BufBuilder ceiling (error 13548, PCSM-305).
func collectUpdateOpsWithConflicts(event *UpdateEvent) updateOps {
	primary := make(bson.D, 0, 3) //nolint:mnd

	// Truncations → $push: {field: {$each: [], $slice: NewSize}}.
	if len(event.UpdateDescription.TruncatedArrays) != 0 {
		fields := make(bson.D, len(event.UpdateDescription.TruncatedArrays))
		for i, ta := range event.UpdateDescription.TruncatedArrays {
			fields[i].Key = ta.Field
			fields[i].Value = bson.D{{Key: "$each", Value: bson.A{}}, {Key: "$slice", Value: ta.NewSize}}
		}

		primary = append(primary, bson.E{Key: "$push", Value: fields})
	}

	// Removed fields → $unset.
	if len(event.UpdateDescription.RemovedFields) != 0 {
		fields := make(bson.D, len(event.UpdateDescription.RemovedFields))
		for i, field := range event.UpdateDescription.RemovedFields {
			fields[i].Key = field
			fields[i].Value = 1
		}

		primary = append(primary, bson.E{Key: "$unset", Value: fields})
	}

	truncatedPrefixes := make([]string, 0, len(event.UpdateDescription.TruncatedArrays))
	for _, ta := range event.UpdateDescription.TruncatedArrays {
		truncatedPrefixes = append(truncatedPrefixes, ta.Field)
	}

	// Updated fields are partitioned into:
	//  - nonConflicting: emitted in primary $set (no overlap with any truncated path).
	//  - conflicting:    deferred to follow-up $set to avoid same-path $push+$set conflict.
	var nonConflicting, conflicting bson.D

	for _, field := range event.UpdateDescription.UpdatedFields {
		if conflictsWithTruncation(field.Key, truncatedPrefixes) {
			conflicting = append(conflicting, bson.E{Key: field.Key, Value: field.Value})

			continue
		}

		nonConflicting = append(nonConflicting, bson.E{Key: field.Key, Value: field.Value})
	}

	if len(nonConflicting) != 0 {
		primary = append(primary, bson.E{Key: "$set", Value: nonConflicting})
	}

	// Follow-up $set ops carry only conflicting (array-index) fields. They are chunked
	// by maxBytesPerSetOp / maxFieldsPerSetOp so each individual update is bounded - this
	// caps the per-update wire size and keeps target plan execution cheap. Each follow-up
	// is a separate ordered update operation; MongoDB resets BufBuilder between operations.
	var followUp []bson.D

	chunkStart := 0
	chunkBytes := 0

	for i := range conflicting {
		b, _ := bson.Marshal(bson.D{conflicting[i]})
		chunkBytes += len(b)

		if chunkBytes >= maxBytesPerSetOp || (i-chunkStart+1) >= maxFieldsPerSetOp {
			followUp = append(followUp, bson.D{{Key: "$set", Value: conflicting[chunkStart : i+1]}})
			chunkStart = i + 1
			chunkBytes = 0
		}
	}

	if chunkStart < len(conflicting) {
		followUp = append(followUp, bson.D{{Key: "$set", Value: conflicting[chunkStart:]}})
	}

	return updateOps{primary: primary, followUp: followUp}
}

// conflictsWithTruncation reports whether key is the same as, or a sub-path of, any
// truncated-array path in prefixes. A conflict means the field cannot share an update
// document with the truncation $push and must be deferred to a separate follow-up $set.
func conflictsWithTruncation(key string, prefixes []string) bool {
	for _, p := range prefixes {
		if key == p || strings.HasPrefix(key, p+".") {
			return true
		}
	}

	return false
}
