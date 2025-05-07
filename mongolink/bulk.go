package mongolink

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

	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
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
	Update(ns Namespace, coll *mongo.Collection, event *UpdateEvent)
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

func (o *clientBulkWrite) Update(ns Namespace, coll *mongo.Collection, event *UpdateEvent) {
	bw := mongo.ClientBulkWrite{
		Database:   ns.Database,
		Collection: ns.Collection,
		Model: &mongo.ClientUpdateOneModel{
			Filter: event.DocumentKey,
			Update: collectUpdateOps(coll, event),
		},
	}

	log.New("update event").With(log.Fields("bulkWrite", bw)).Trace("AAAAA Update command bulk write")
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

func (o *collectionBulkWrite) Update(ns Namespace, coll *mongo.Collection, event *UpdateEvent) {
	o.writes[ns] = append(o.writes[ns], &mongo.UpdateOneModel{
		Filter: event.DocumentKey,
		Update: collectUpdateOps(coll, event),
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

func collectUpdateOps(coll *mongo.Collection, event *UpdateEvent) bson.D {
	lg := log.New("update event")

	updateDoc := bson.D{}
	setFields := bson.D{}
	unsetFields := bson.D{}

	// $set for updated fields (non-array paths)
	for _, field := range event.UpdateDescription.UpdatedFields {
		if len(event.UpdateDescription.TruncatedArrays) == 0 || !isArrayPath(field.Key) {
			setFields = append(setFields, bson.E{Key: field.Key, Value: field.Value})
		}
	}

	// $unset for removed fields
	for _, field := range event.UpdateDescription.RemovedFields {
		unsetFields = append(unsetFields, bson.E{Key: field, Value: ""})
	}

	var origDoc bson.Raw
	var err error

	if len(event.UpdateDescription.TruncatedArrays) > 0 {
		origDoc, err = coll.
			FindOne(context.Background(), bson.M{"_id": event.DocumentKey[0].Value}).Raw()
		if err != nil {
			lg.Error(err, "find original document")

			return nil
		}
	}

	for _, ta := range event.UpdateDescription.TruncatedArrays {
		origArr, ok := getArray(origDoc, ta.Field)
		if !ok {
			lg.Error(nil, "missing array field")

			return nil
		}

		newArr := origArr[:ta.NewSize]

		for _, field := range event.UpdateDescription.UpdatedFields {
			if strings.HasPrefix(field.Key, ta.Field+".") {
				idx := strings.TrimPrefix(field.Key, ta.Field+".") // extract the index
				if index, err := strconv.Atoi(idx); err == nil && index < int(ta.NewSize) {
					newArr[index] = field.Value
				}
			}
		}
		setFields = append(setFields, bson.E{Key: ta.Field, Value: newArr})
	}

	if len(setFields) > 0 {
		updateDoc = append(updateDoc, bson.E{Key: "$set", Value: setFields})
	}
	if len(unsetFields) > 0 {
		updateDoc = append(updateDoc, bson.E{Key: "$unset", Value: unsetFields})
	}

	return updateDoc
}

// isArrayPath checks if the path is an path to an array index (e.g. "a.b.1").
func isArrayPath(field string) bool {
	parts := strings.Split(field, ".")
	if len(parts) < 2 { //nolint:mnd
		return false
	}

	_, err := strconv.Atoi(parts[len(parts)-1])

	return err == nil
}

// getArray retrieves an array from a BSON document at the specified path.
func getArray(doc bson.Raw, path string) ([]any, bool) {
	parts := strings.Split(path, ".")
	current := doc

	for _, part := range parts {
		val := current.Lookup(part)
		if val.Type == 0 {
			return nil, false
		}

		if val.Type == bson.TypeEmbeddedDocument {
			current = val.Document()

			continue
		}

		if val.Type == bson.TypeArray && part == parts[len(parts)-1] {
			var arr []any
			if err := val.Unmarshal(&arr); err != nil {
				return nil, false
			}

			return arr, true
		}

		return nil, false
	}

	return nil, false
}
