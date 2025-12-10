package pcsm

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

type bulkWrite interface {
	Full() bool
	Empty() bool
	Do(ctx context.Context, m *mongo.Client) (int, error)

	Insert(ns Namespace, event *InsertEvent)
	Update(ns Namespace, event *UpdateEvent)
	Replace(ns Namespace, event *ReplaceEvent)
	Delete(ns Namespace, event *DeleteEvent)
}

type clientBulkWrite struct {
	useSimpleCollation bool
	writes             []mongo.ClientBulkWrite
}

func newClientBulkWrite(size int, useSimpleCollation bool) *clientBulkWrite {
	return &clientBulkWrite{
		useSimpleCollation: useSimpleCollation,
		writes:             make([]mongo.ClientBulkWrite, 0, size),
	}
}

func (o *clientBulkWrite) Full() bool {
	return len(o.writes) == cap(o.writes)
}

func (o *clientBulkWrite) Empty() bool {
	return len(o.writes) == 0
}

func (o *clientBulkWrite) Do(ctx context.Context, m *mongo.Client) (int, error) {
	totalSize := len(o.writes)

	err := o.doWithRetry(ctx, m, o.writes)
	if err != nil {
		return 0, err // nolint:wrapcheck
	}

	clear(o.writes)
	o.writes = o.writes[:0]

	return totalSize, nil
}

// doWithRetry executes bulk write operations with retry logic for duplicate key errors.
// In ordered mode, when an error occurs at index N, operations 0..N-1 are applied,
// operation N fails, and N+1..end are never executed. This function handles operation N
// and retries the remaining operations recursively.
func (o *clientBulkWrite) doWithRetry(ctx context.Context, m *mongo.Client, writes []mongo.ClientBulkWrite) error {
	if len(writes) == 0 {
		return nil
	}

	var bulkErr error

	err := topo.RunWithRetry(ctx, func(ctx context.Context) error {
		_, err := m.BulkWrite(ctx, writes, clientBulkOptions)
		bulkErr = err

		return errors.Wrap(err, "bulk write")
	}, topo.DefaultRetryInterval, topo.DefaultMaxRetries)
	if err != nil {
		var bwe mongo.ClientBulkWriteException

		if errors.As(bulkErr, &bwe) {
			// Find the first error by looking for the minimum index in the map
			// (in ordered mode, there should only be one error)
			if len(bwe.WriteErrors) > 0 {
				// Find the minimum index in the WriteErrors map
				minIdx := -1
				for idx := range bwe.WriteErrors {
					if minIdx == -1 || idx < minIdx {
						minIdx = idx
					}
				}

				firstErr := bwe.WriteErrors[minIdx]

				// Only handle duplicate key errors on ReplaceOne operations
				if mongo.IsDuplicateKeyError(firstErr) {
					if minIdx >= 0 && minIdx < len(writes) {
						write := writes[minIdx]

						replaceModel, ok := write.Model.(*mongo.ClientReplaceOneModel)
						if ok {
							// Handle the duplicate key error with delete+insert
							coll := m.Database(write.Database).Collection(write.Collection)

							err := handleDuplicateKeyError(ctx, coll, replaceModel.Replacement)
							if err != nil {
								return err
							}

							// Retry remaining operations (from index+1 onwards)
							// These operations were never executed due to ordered semantics
							remainingWrites := writes[minIdx+1:]

							return o.doWithRetry(ctx, m, remainingWrites)
						}
					}
				}
			}
		}

		return err // nolint:wrapcheck
	}

	return nil
}

func (o *clientBulkWrite) Insert(ns Namespace, event *InsertEvent) {
	m := &mongo.ClientReplaceOneModel{
		Filter:      event.DocumentKey,
		Replacement: event.FullDocument,
		Upsert:      &yes,
	}

	if ns.Sharded && o.useSimpleCollation {
		m.Collation = simpleCollation
	}

	bw := mongo.ClientBulkWrite{
		Database:   ns.Database,
		Collection: ns.Collection,
		Model:      m,
	}

	o.writes = append(o.writes, bw)
}

func (o *clientBulkWrite) Update(ns Namespace, event *UpdateEvent) {
	m := &mongo.ClientUpdateOneModel{
		Filter: event.DocumentKey,
		Update: collectUpdateOps(event),
	}

	if ns.Sharded && o.useSimpleCollation {
		m.Collation = simpleCollation
	}

	bw := mongo.ClientBulkWrite{
		Database:   ns.Database,
		Collection: ns.Collection,
		Model:      m,
	}

	o.writes = append(o.writes, bw)
}

func (o *clientBulkWrite) Replace(ns Namespace, event *ReplaceEvent) {
	m := &mongo.ClientReplaceOneModel{
		Filter:      event.DocumentKey,
		Replacement: event.FullDocument,
	}

	if ns.Sharded && o.useSimpleCollation {
		m.Collation = simpleCollation
	}

	bw := mongo.ClientBulkWrite{
		Database:   ns.Database,
		Collection: ns.Collection,
		Model:      m,
	}

	o.writes = append(o.writes, bw)
}

func (o *clientBulkWrite) Delete(ns Namespace, event *DeleteEvent) {
	m := &mongo.ClientDeleteOneModel{
		Filter: event.DocumentKey,
	}

	if ns.Sharded && o.useSimpleCollation {
		m.Collation = simpleCollation
	}

	bw := mongo.ClientBulkWrite{
		Database:   ns.Database,
		Collection: ns.Collection,
		Model:      m,
	}

	o.writes = append(o.writes, bw)
}

type collectionBulkWrite struct {
	useSimpleCollation bool
	max                int
	count              int
	writes             map[string][]mongo.WriteModel
}

func newCollectionBulkWrite(size int, nonDefaultCollationSupport bool) *collectionBulkWrite {
	return &collectionBulkWrite{
		useSimpleCollation: nonDefaultCollationSupport,
		max:                size,
		writes:             make(map[string][]mongo.WriteModel),
	}
}

func (o *collectionBulkWrite) Full() bool {
	return o.count == o.max
}

func (o *collectionBulkWrite) Empty() bool {
	return o.count == 0
}

func (o *collectionBulkWrite) Do(ctx context.Context, m *mongo.Client) (int, error) {
	var total atomic.Int64

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.SetLimit(runtime.NumCPU())

	for ns, ops := range o.writes {
		namespace, err := parseNamespace(ns)
		if err != nil {
			return 0, errors.Wrapf(err, "parse namespace %q", namespace)
		}

		grp.Go(func() error {
			mcoll := m.Database(namespace.Database).Collection(namespace.Collection)

			err := o.doWithRetry(grpCtx, mcoll, namespace, ops)
			if err != nil {
				return err // nolint:wrapcheck
			}

			total.Add(int64(len(ops)))

			return nil
		})
	}

	err := grp.Wait()
	if err != nil {
		return 0, err // nolint:wrapcheck
	}

	clear(o.writes)
	o.count = 0

	return int(total.Load()), nil
}

// doWithRetry executes bulk write operations for a single namespace with retry logic for duplicate key errors.
// In ordered mode, when an error occurs at index N, operations 0..N-1 are applied,
// operation N fails, and N+1..end are never executed. This function handles operation N
// and retries the remaining operations recursively.
func (o *collectionBulkWrite) doWithRetry(
	ctx context.Context,
	coll *mongo.Collection,
	namespace Namespace,
	ops []mongo.WriteModel,
) error {
	if len(ops) == 0 {
		return nil
	}

	var bulkErr error

	err := topo.RunWithRetry(ctx, func(_ context.Context) error {
		_, err := coll.BulkWrite(ctx, ops, collectionBulkOptions)
		bulkErr = err

		return errors.Wrapf(err, "bulk write %q", namespace)
	}, topo.DefaultRetryInterval, topo.DefaultMaxRetries)
	if err != nil {
		var bwe mongo.BulkWriteException

		if errors.As(bulkErr, &bwe) {
			// Find the first error (in ordered mode, there should only be one)
			if len(bwe.WriteErrors) > 0 {
				firstErr := bwe.WriteErrors[0]

				// Only handle duplicate key errors on ReplaceOne operations
				if mongo.IsDuplicateKeyError(firstErr) {
					if firstErr.Index >= 0 && firstErr.Index < len(ops) {
						op := ops[firstErr.Index]

						replaceModel, ok := op.(*mongo.ReplaceOneModel)
						if ok {
							// Handle the duplicate key error with delete+insert
							err := handleDuplicateKeyError(ctx, coll, replaceModel.Replacement)
							if err != nil {
								return err
							}

							// Retry remaining operations (from index+1 onwards)
							// These operations were never executed due to ordered semantics
							remainingOps := ops[firstErr.Index+1:]

							return o.doWithRetry(ctx, coll, namespace, remainingOps)
						}
					}
				}
			}
		}

		return err // nolint:wrapcheck
	}

	return nil
}

func (o *collectionBulkWrite) Insert(ns Namespace, event *InsertEvent) {
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

	if ns.Sharded && o.useSimpleCollation {
		m.Collation = simpleCollation
	}

	o.writes[ns.String()] = append(o.writes[ns.String()], m)

	o.count++
}

func (o *collectionBulkWrite) Update(ns Namespace, event *UpdateEvent) {
	m := &mongo.UpdateOneModel{
		Filter: event.DocumentKey,
		Update: collectUpdateOps(event),
	}

	if ns.Sharded && o.useSimpleCollation {
		m.Collation = simpleCollation
	}

	o.writes[ns.String()] = append(o.writes[ns.String()], m)

	o.count++
}

func (o *collectionBulkWrite) Replace(ns Namespace, event *ReplaceEvent) {
	m := &mongo.ReplaceOneModel{
		Filter:      event.DocumentKey,
		Replacement: event.FullDocument,
	}

	if ns.Sharded && o.useSimpleCollation {
		m.Collation = simpleCollation
	}

	o.writes[ns.String()] = append(o.writes[ns.String()], m)

	o.count++
}

func (o *collectionBulkWrite) Delete(ns Namespace, event *DeleteEvent) {
	m := &mongo.DeleteOneModel{
		Filter: event.DocumentKey,
	}

	if ns.Sharded && o.useSimpleCollation {
		m.Collation = simpleCollation
	}

	o.writes[ns.String()] = append(o.writes[ns.String()], m)

	o.count++
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

	ops := make(bson.D, 0, 1) //nolint:mnd

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
		if isArrayPath(field.Key, dp) {
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
func isArrayPath(field string, disambiguatedPaths map[string][]any) bool {
	if path, ok := disambiguatedPaths[field]; ok {
		for _, p := range path {
			switch p.(type) {
			case int, int8, int16, int32, int64:
				return true
			}

			continue
		}

		return false
	}

	parts := strings.Split(field, ".")
	if len(parts) < 2 { //nolint:mnd
		return false
	}

	_, err := strconv.Atoi(parts[len(parts)-1])

	return err == nil
}

func parseNamespace(ns string) (Namespace, error) {
	parts := strings.SplitN(ns, ".", 2) //nolint:mnd

	if len(parts) != 2 { //nolint:mnd
		return Namespace{}, errors.Errorf("invalid namespace %q", ns)
	}

	return Namespace{
		Database:   parts[0],
		Collection: parts[1],
	}, nil
}
