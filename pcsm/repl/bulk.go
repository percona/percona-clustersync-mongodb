package repl

import (
	"context"
	"encoding/hex"
	"runtime"
	"strings"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-clustersync-mongodb/config"
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
// truncations with conflicting array-index updates or removed sub-paths, the conflicting
// operations are split off into separate $set/$unset follow-up operations, each its own
// update with its own MongoDB-side BufBuilder. maxBytesPerSetOp is the primary guard;
// maxFieldsPerSetOp is a secondary guard against degenerate cases with many tiny fields.
//
// The aggregate wire size of a single bulk command is capped at config.MaxWriteBatchSizeBytes
// (MongoDB's MaxMessageSizeBytes minus the standard message-header reserve). Per-op envelope
// overhead (model wrappers, nsInfo, command metadata) is not explicitly accounted for in
// cbw.bytes; the header reserve plus the empty-bulk exception in WouldOverflow provide the
// safety margin in practice.
const (
	maxFieldsPerSetOp = 100        //nolint:mnd
	maxBytesPerSetOp  = 512 * 1024 //nolint:mnd // 512 KiB
)

// updateOps holds the result of building update operations from a change stream event.
// When a change event combines an array truncation with conflicting indexed-write updates
// or removed sub-paths, the conflicting updates are split into follow-up $set/$unset
// operations so that no single update document carries both a $push (truncation) and a
// $set/$unset on the same array path - which MongoDB rejects as a path conflict - and so
// that no individual update accumulates enough work to exhaust MongoDB's 125 MB BufBuilder
// ceiling (error 13548).
type updateOps struct {
	// primary is the main update document.
	primary bson.D
	// followUp contains additional update documents (each with a single $set or $unset
	// operator) for fields that conflict with a truncation in the primary update. nil
	// when not needed.
	followUp []bson.D
}

type bulkWriter interface {
	Full() bool
	Empty() bool
	// WouldOverflow reports whether appending an operation of the given estimated
	// BSON byte size would push the bulk past config.MaxWriteBatchSizeBytes. It
	// returns false when the bulk is empty so a single oversized event can still be
	// sent on its own.
	WouldOverflow(estimate int) bool
	Do(ctx context.Context, m *mongo.Client) (int, error)

	Insert(change *ChangeEvent, event *InsertEvent)
	Update(change *ChangeEvent, event *UpdateEvent)
	Replace(change *ChangeEvent, event *ReplaceEvent)
	Delete(change *ChangeEvent, event *DeleteEvent)
}

type clientBulkWrite struct {
	uuidMap            catalog.UUIDMap
	useSimpleCollation bool
	maxOpsSize         int
	bytes              int
	writes             []mongo.ClientBulkWrite
}

func newClientBulkWriter(size int, useSimpleCollation bool, uuidMap catalog.UUIDMap) *clientBulkWrite {
	return &clientBulkWrite{
		uuidMap:            uuidMap,
		useSimpleCollation: useSimpleCollation,
		maxOpsSize:         size,
		writes:             make([]mongo.ClientBulkWrite, 0, size),
	}
}

//go:inline
func findNamespaceByUUID(uuidMap catalog.UUIDMap, change *ChangeEvent) catalog.Namespace {
	if change.CollectionUUID != nil {
		if ns, ok := uuidMap[hex.EncodeToString(change.CollectionUUID.Data)]; ok {
			return ns
		}
	}

	for _, ns := range uuidMap {
		if ns.Database == change.Namespace.Database && ns.Collection == change.Namespace.Collection {
			return ns
		}
	}

	return change.Namespace
}

func (cbw *clientBulkWrite) Full() bool {
	return len(cbw.writes) >= cbw.maxOpsSize || cbw.bytes >= config.MaxWriteBatchSizeBytes
}

func (cbw *clientBulkWrite) Empty() bool {
	return len(cbw.writes) == 0
}

func (cbw *clientBulkWrite) WouldOverflow(estimate int) bool {
	return len(cbw.writes) > 0 && cbw.bytes+estimate > config.MaxWriteBatchSizeBytes
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

func (cbw *clientBulkWrite) Insert(change *ChangeEvent, event *InsertEvent) {
	ns := findNamespaceByUUID(cbw.uuidMap, change)

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

func (cbw *clientBulkWrite) Update(change *ChangeEvent, event *UpdateEvent) {
	ns := findNamespaceByUUID(cbw.uuidMap, change)
	ops := collectUpdateOps(event)

	// Marshal the document filter once and reuse for primary + every follow-up.
	filterLen := marshalLen(event.DocumentKey)

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
	cbw.bytes += filterLen + marshalLen(ops.primary)

	// Follow-up $set/$unset operations for fields split out due to truncation path
	// conflicts and BufBuilder limits. Each is a separate updateOne so MongoDB resets
	// its BufBuilder per operation. Ordered bulk writes guarantee sequential execution.
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
		cbw.bytes += filterLen + marshalLen(followUp)
	}
}

func (cbw *clientBulkWrite) Replace(change *ChangeEvent, event *ReplaceEvent) {
	ns := findNamespaceByUUID(cbw.uuidMap, change)

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

func (cbw *clientBulkWrite) Delete(change *ChangeEvent, event *DeleteEvent) {
	ns := findNamespaceByUUID(cbw.uuidMap, change)

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
	uuidMap            catalog.UUIDMap
	useSimpleCollation bool
	maxOpsSize         int
	count              int
	bytes              int
	writes             map[string][]mongo.WriteModel
}

func newCollectionBulkWriter(size int, nonDefaultCollationSupport bool, uuidMap catalog.UUIDMap) *collectionBulkWrite {
	return &collectionBulkWrite{
		uuidMap:            uuidMap,
		useSimpleCollation: nonDefaultCollationSupport,
		maxOpsSize:         size,
		writes:             make(map[string][]mongo.WriteModel),
	}
}

func (cbw *collectionBulkWrite) Full() bool {
	return cbw.count >= cbw.maxOpsSize || cbw.bytes >= config.MaxWriteBatchSizeBytes
}

func (cbw *collectionBulkWrite) Empty() bool {
	return cbw.count == 0
}

func (cbw *collectionBulkWrite) WouldOverflow(estimate int) bool {
	return cbw.count > 0 && cbw.bytes+estimate > config.MaxWriteBatchSizeBytes
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

func (cbw *collectionBulkWrite) Insert(change *ChangeEvent, event *InsertEvent) {
	ns := findNamespaceByUUID(cbw.uuidMap, change)
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

func (cbw *collectionBulkWrite) Update(change *ChangeEvent, event *UpdateEvent) {
	ns := findNamespaceByUUID(cbw.uuidMap, change)
	ops := collectUpdateOps(event)

	filterLen := marshalLen(event.DocumentKey)

	m := &mongo.UpdateOneModel{
		Filter: event.DocumentKey,
		Update: ops.primary,
	}

	if ns.Sharded && cbw.useSimpleCollation {
		m.Collation = simpleCollation
	}

	cbw.writes[ns.String()] = append(cbw.writes[ns.String()], m)
	cbw.count++
	cbw.bytes += filterLen + marshalLen(ops.primary)

	// Follow-up $set/$unset operations for fields split out due to truncation path
	// conflicts and BufBuilder limits.
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
		cbw.bytes += filterLen + marshalLen(followUp)
	}
}

func (cbw *collectionBulkWrite) Replace(change *ChangeEvent, event *ReplaceEvent) {
	ns := findNamespaceByUUID(cbw.uuidMap, change)

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

func (cbw *collectionBulkWrite) Delete(change *ChangeEvent, event *DeleteEvent) {
	ns := findNamespaceByUUID(cbw.uuidMap, change)

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

	_, err = coll.DeleteOne(ctx, bson.D{{Key: "_id", Value: docID}})
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
// Special-cases bson.Raw (already a complete BSON document) to avoid a redundant marshal.
func marshalLen(v any) int {
	if v == nil {
		return 0
	}

	if r, ok := v.(bson.Raw); ok {
		return len(r)
	}

	b, err := bson.Marshal(v)
	if err != nil {
		return 0
	}

	return len(b)
}

// estimateEventBytes returns the approximate BSON byte size that an event will contribute
// to a bulk write. Used by the worker to preflight WouldOverflow before adding to the
// current bulk so the byte cap is honored without ever appending past it.
func estimateEventBytes(parsed any) int {
	switch e := parsed.(type) {
	case InsertEvent:
		return estimateBytes(e.DocumentKey, e.FullDocument)
	case ReplaceEvent:
		return estimateBytes(e.DocumentKey, e.FullDocument)
	case DeleteEvent:
		return estimateBytes(e.DocumentKey, nil)
	case UpdateEvent:
		ops := collectUpdateOps(&e)

		filterLen := marshalLen(e.DocumentKey)
		total := filterLen + marshalLen(ops.primary)

		for _, fu := range ops.followUp {
			total += filterLen + marshalLen(fu)
		}

		return total
	default:
		return 0
	}
}

// collectUpdateOps builds the update operations for a change stream UpdateEvent. When no
// truncated array path conflicts with any updated or removed field, a single classic
// update document is emitted in updateOps.primary. Otherwise it dispatches to
// collectUpdateOpsWithConflicts to split conflicting writes into ordered follow-up updates.
func collectUpdateOps(event *UpdateEvent) updateOps {
	if len(event.UpdateDescription.TruncatedArrays) != 0 {
		prefixes := truncatedPrefixes(event)

		for _, u := range event.UpdateDescription.UpdatedFields {
			if conflictsWithTruncation(u.Key, prefixes) {
				return collectUpdateOpsWithConflicts(event, prefixes)
			}
		}

		for _, rf := range event.UpdateDescription.RemovedFields {
			if conflictsWithTruncation(rf, prefixes) {
				return collectUpdateOpsWithConflicts(event, prefixes)
			}
		}
	}

	ops := make(bson.D, 0, 3) //nolint:mnd

	if len(event.UpdateDescription.UpdatedFields) != 0 {
		ops = append(ops, bson.E{Key: "$set", Value: event.UpdateDescription.UpdatedFields})
	}

	if len(event.UpdateDescription.RemovedFields) != 0 {
		fields := make(bson.D, len(event.UpdateDescription.RemovedFields))
		for i, field := range event.UpdateDescription.RemovedFields {
			fields[i].Key = field
			fields[i].Value = 1
		}

		ops = append(ops, bson.E{Key: "$unset", Value: fields})
	}

	if len(event.UpdateDescription.TruncatedArrays) != 0 {
		fields := make(bson.D, len(event.UpdateDescription.TruncatedArrays))
		for i, field := range event.UpdateDescription.TruncatedArrays {
			fields[i].Key = field.Field
			fields[i].Value = bson.D{{Key: "$each", Value: bson.A{}}, {Key: "$slice", Value: field.NewSize}}
		}

		ops = append(ops, bson.E{Key: "$push", Value: fields})
	}

	return updateOps{primary: ops}
}

// collectUpdateOpsWithConflicts builds update operations for change events where one or
// more updated fields or removed fields target a path inside a truncated array (e.g.
// truncation of "arr" together with a write to "arr.5" or a removal of "arr.0.x"). MongoDB
// rejects an update document that combines a $push (truncation) with a $set or $unset on
// the same array path, so the work is split across multiple ordered update operations:
//
//	primary update:   $push (truncations) + $unset (non-conflicting removed) +
//	                  $set (non-conflicting updated)
//	follow-up update: $set (conflicting updated) and $unset (conflicting removed),
//	                  chunked by maxBytesPerSetOp / maxFieldsPerSetOp to keep each
//	                  individual update bounded
//
// Splitting also bounds per-update work on the target side, keeping each update under
// MongoDB's 125 MB BufBuilder ceiling (error 13548, PCSM-305).
func collectUpdateOpsWithConflicts(event *UpdateEvent, prefixes []string) updateOps {
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

	// Partition removed fields: non-conflicting → primary $unset; conflicting → follow-up.
	nonConflictingUnset := make(bson.D, 0, len(event.UpdateDescription.RemovedFields))
	conflictingUnset := make(bson.D, 0, len(event.UpdateDescription.RemovedFields))

	for _, field := range event.UpdateDescription.RemovedFields {
		if conflictsWithTruncation(field, prefixes) {
			conflictingUnset = append(conflictingUnset, bson.E{Key: field, Value: 1})

			continue
		}

		nonConflictingUnset = append(nonConflictingUnset, bson.E{Key: field, Value: 1})
	}

	if len(nonConflictingUnset) != 0 {
		primary = append(primary, bson.E{Key: "$unset", Value: nonConflictingUnset})
	}

	// Partition updated fields: non-conflicting → primary $set; conflicting → follow-up.
	nonConflictingSet := make(bson.D, 0, len(event.UpdateDescription.UpdatedFields))
	conflictingSet := make(bson.D, 0, len(event.UpdateDescription.UpdatedFields))

	for _, field := range event.UpdateDescription.UpdatedFields {
		if conflictsWithTruncation(field.Key, prefixes) {
			conflictingSet = append(conflictingSet, bson.E{Key: field.Key, Value: field.Value})

			continue
		}

		nonConflictingSet = append(nonConflictingSet, bson.E{Key: field.Key, Value: field.Value})
	}

	if len(nonConflictingSet) != 0 {
		primary = append(primary, bson.E{Key: "$set", Value: nonConflictingSet})
	}

	// Follow-up ops carry only conflicting (truncation-path) fields. They are chunked by
	// maxBytesPerSetOp / maxFieldsPerSetOp so each individual update is bounded - this
	// caps the per-update wire size and keeps target plan execution cheap. Each follow-up
	// is a separate ordered update operation; MongoDB resets BufBuilder between operations.
	var followUp []bson.D

	followUp = appendFieldChunks(followUp, "$set", conflictingSet)
	followUp = appendFieldChunks(followUp, "$unset", conflictingUnset)

	return updateOps{primary: primary, followUp: followUp}
}

// truncatedPrefixes extracts the list of truncated array paths from an UpdateEvent so they
// can be reused by both the dispatch check and the partitioning logic.
func truncatedPrefixes(event *UpdateEvent) []string {
	prefixes := make([]string, 0, len(event.UpdateDescription.TruncatedArrays))
	for _, ta := range event.UpdateDescription.TruncatedArrays {
		prefixes = append(prefixes, ta.Field)
	}

	return prefixes
}

// appendFieldChunks splits fields into bounded follow-up update documents, each carrying a
// single op operator (e.g. "$set" or "$unset"). Chunks are bounded by maxBytesPerSetOp and
// maxFieldsPerSetOp. Returns followUp with the new chunks appended.
func appendFieldChunks(followUp []bson.D, op string, fields bson.D) []bson.D {
	if len(fields) == 0 {
		return followUp
	}

	chunkStart := 0
	chunkBytes := 0

	for i := range fields {
		chunkBytes += marshalLen(bson.D{fields[i]})

		if chunkBytes >= maxBytesPerSetOp || (i-chunkStart+1) >= maxFieldsPerSetOp {
			followUp = append(followUp, bson.D{{Key: op, Value: fields[chunkStart : i+1]}})
			chunkStart = i + 1
			chunkBytes = 0
		}
	}

	if chunkStart < len(fields) {
		followUp = append(followUp, bson.D{{Key: op, Value: fields[chunkStart:]}})
	}

	return followUp
}

// conflictsWithTruncation reports whether key is the same as, or a sub-path of, any
// truncated-array path in prefixes. A conflict means the field cannot share an update
// document with the truncation $push and must be deferred to a separate follow-up.
func conflictsWithTruncation(key string, prefixes []string) bool {
	for _, p := range prefixes {
		if key == p || strings.HasPrefix(key, p+".") {
			return true
		}
	}

	return false
}
