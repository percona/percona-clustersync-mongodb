package plm

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

	"github.com/percona/percona-link-mongodb/errors"
)

//nolint:gochecknoglobals
var yes = true // for ref

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
	writes []mongo.ClientBulkWrite
}

func newClientBulkWrite(size int) *clientBulkWrite {
	return &clientBulkWrite{
		make([]mongo.ClientBulkWrite, 0, size),
	}
}

func (o *clientBulkWrite) Full() bool {
	return len(o.writes) == cap(o.writes)
}

func (o *clientBulkWrite) Empty() bool {
	return len(o.writes) == 0
}

func (o *clientBulkWrite) Do(ctx context.Context, m *mongo.Client) (int, error) {
	_, err := m.BulkWrite(ctx, o.writes, clientBulkOptions)
	if err != nil {
		return 0, errors.Wrap(err, "bulk write")
	}

	size := len(o.writes)
	clear(o.writes)
	o.writes = o.writes[:0]

	return size, nil
}

func (o *clientBulkWrite) Insert(ns Namespace, event *InsertEvent) {
	bw := mongo.ClientBulkWrite{
		Database:   ns.Database,
		Collection: ns.Collection,
		Model: &mongo.ClientReplaceOneModel{
			Filter:      event.DocumentKey,
			Replacement: event.FullDocument,
			Upsert:      &yes,
		},
	}

	o.writes = append(o.writes, bw)
}

func (o *clientBulkWrite) Update(ns Namespace, event *UpdateEvent) {
	bw := mongo.ClientBulkWrite{
		Database:   ns.Database,
		Collection: ns.Collection,
		Model: &mongo.ClientUpdateOneModel{
			Filter: event.DocumentKey,
			Update: collectUpdateOps(event),
		},
	}

	o.writes = append(o.writes, bw)
}

func (o *clientBulkWrite) Replace(ns Namespace, event *ReplaceEvent) {
	bw := mongo.ClientBulkWrite{
		Database:   ns.Database,
		Collection: ns.Collection,
		Model: &mongo.ClientReplaceOneModel{
			Filter:      event.DocumentKey,
			Replacement: event.FullDocument,
		},
	}

	o.writes = append(o.writes, bw)
}

func (o *clientBulkWrite) Delete(ns Namespace, event *DeleteEvent) {
	bw := mongo.ClientBulkWrite{
		Database:   ns.Database,
		Collection: ns.Collection,
		Model: &mongo.ClientDeleteOneModel{
			Filter: event.DocumentKey,
		},
	}

	o.writes = append(o.writes, bw)
}

type collectionBulkWrite struct {
	max    int
	count  int
	writes map[Namespace][]mongo.WriteModel
}

func newCollectionBulkWrite(size int) *collectionBulkWrite {
	return &collectionBulkWrite{
		max:    size,
		writes: make(map[Namespace][]mongo.WriteModel),
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
		grp.Go(func() error {
			mcoll := m.Database(ns.Database).Collection(ns.Collection)
			_, err := mcoll.BulkWrite(grpCtx, ops, collectionBulkOptions)
			if err != nil {
				return errors.Wrapf(err, "bulkWrite %q", ns)
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

func (o *collectionBulkWrite) Insert(ns Namespace, event *InsertEvent) {
	o.writes[ns] = append(o.writes[ns], &mongo.ReplaceOneModel{
		Filter:      event.DocumentKey,
		Replacement: event.FullDocument,
		Upsert:      &yes,
	})

	o.count++
}

func (o *collectionBulkWrite) Update(ns Namespace, event *UpdateEvent) {
	o.writes[ns] = append(o.writes[ns], &mongo.UpdateOneModel{
		Filter: event.DocumentKey,
		Update: collectUpdateOps(event),
	})

	o.count++
}

func (o *collectionBulkWrite) Replace(ns Namespace, event *ReplaceEvent) {
	o.writes[ns] = append(o.writes[ns], &mongo.ReplaceOneModel{
		Filter:      event.DocumentKey,
		Replacement: event.FullDocument,
	})

	o.count++
}

func (o *collectionBulkWrite) Delete(ns Namespace, event *DeleteEvent) {
	o.writes[ns] = append(o.writes[ns], &mongo.DeleteOneModel{
		Filter: event.DocumentKey,
	})

	o.count++
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
