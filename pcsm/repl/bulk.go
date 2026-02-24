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
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/topo"
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
	writes             []mongo.ClientBulkWrite
}

func newClientBulkWriter(size int, useSimpleCollation bool) *clientBulkWrite {
	return &clientBulkWrite{
		useSimpleCollation: useSimpleCollation,
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

	err := topo.RunWithRetry(ctx, func(ctx context.Context) error {
		_, err := m.BulkWrite(ctx, bulkWrites, clientBulkOptions)
		bulkErr = err

		return errors.Wrap(err, "bulk write")
	}, topo.DefaultRetryInterval, topo.DefaultMaxRetries)
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
	m := &mongo.ClientUpdateOneModel{
		Filter: event.DocumentKey,
		Update: collectUpdateOps(event),
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
	max                int
	count              int
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
	return cbw.count == cbw.max
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

	err := topo.RunWithRetry(ctx, func(_ context.Context) error {
		_, err := coll.BulkWrite(ctx, bulkWrites, collectionBulkOptions)
		bulkErr = err

		return errors.Wrapf(err, "bulk write %q", ns)
	}, topo.DefaultRetryInterval, topo.DefaultMaxRetries)
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
	m := &mongo.UpdateOneModel{
		Filter: event.DocumentKey,
		Update: collectUpdateOps(event),
	}

	if ns.Sharded && cbw.useSimpleCollation {
		m.Collation = simpleCollation
	}

	cbw.writes[ns.String()] = append(cbw.writes[ns.String()], m)

	cbw.count++
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

func collectUpdateOps(event *UpdateEvent) any {
	for _, trunc := range event.UpdateDescription.TruncatedArrays {
		for _, update := range event.UpdateDescription.UpdatedFields {
			if strings.HasPrefix(update.Key, trunc.Field) {
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

	return ops
}

func collectUpdateOpsWithPipeline(event *UpdateEvent) bson.A {
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
	for _, field := range event.UpdateDescription.UpdatedFields {
		if isArrayPath(field.Key, dp, truncatedFields) {
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
								bson.A{fieldExpr, fieldIdx + 1, bson.D{{"$size", fieldExpr}}},
							}},
						}},
					}},
				},
			}}

			pipeline = append(pipeline, stage)
		} else {
			stage := bson.D{{Key: "$set", Value: bson.D{
				{Key: field.Key, Value: field.Value},
			}}}

			pipeline = append(pipeline, stage)
		}
	}

	// Handle removed fields
	if len(event.UpdateDescription.RemovedFields) != 0 {
		pipeline = append(
			pipeline,
			bson.D{{Key: "$unset", Value: event.UpdateDescription.RemovedFields}},
		)
	}

	return pipeline
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

	// Case 2: disambiguatedPaths is nil (MongoDB <6.1) → use truncatedFields + depth heuristic
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

		// If parent is in truncatedFields, definitely an array path
		parentPath := strings.Join(parts[:len(parts)-1], ".")
		if _, ok := truncatedFields[parentPath]; ok {
			return true
		}

		// Parent not in truncatedFields. Use depth heuristic:
		// - Depth > 2 (e.g., b.0.1): likely nested array, return true
		// - Depth == 2 (e.g., f2.1): ambiguous, could be object key, return false
		return len(parts) > 2 //nolint:mnd
	}

	// Case 3: disambiguatedPaths non-nil but field not in it → Atoi fallback (unambiguous)
	parts := strings.Split(field, ".")
	if len(parts) < 2 { //nolint:mnd
		return false
	}

	_, err := strconv.Atoi(parts[len(parts)-1])

	return err == nil
}
