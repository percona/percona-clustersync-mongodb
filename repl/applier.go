package repl

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
)

type UnsupportedEventError struct {
	OpType OperationType
}

func (e UnsupportedEventError) Error() string {
	return "unsupported type: " + string(e.OpType)
}

func IsUnsupportedEventError(err error) bool {
	return errors.As(err, &UnsupportedEventError{})
}

type InvalidatedError struct {
	ResumeToken bson.Raw
}

func (e InvalidatedError) Error() string {
	return "invalidated"
}

func IsInvalidatedError(err error) bool {
	return errors.As(err, &InvalidatedError{})
}

type eventApplier struct {
	Client       *mongo.Client
	Drop         bool
	IsSelected   FilterFunc
	IndexCatalog *IndexCatalog
}

func (h *eventApplier) Apply(ctx context.Context, data bson.Raw) (primitive.Timestamp, error) {
	var baseEvent BaseEvent
	err := bson.Unmarshal(data, &baseEvent)
	if err != nil {
		return primitive.Timestamp{}, errors.Wrap(err, "failed to decode base event")
	}

	ctx = log.WithAttrs(ctx,
		log.Scope("eventApplier.apply"),
		log.Operation(string(baseEvent.OperationType)),
		log.OpTime(baseEvent.ClusterTime),
		log.NS(baseEvent.Namespace.Database, baseEvent.Namespace.Collection))

	if !h.IsSelected(baseEvent.Namespace.Database, baseEvent.Namespace.Collection) {
		log.Debug(ctx, "not selected")
		return baseEvent.ClusterTime, nil
	}

	log.Debug(ctx, "apply event")

	switch baseEvent.OperationType {
	case Create:
		err = h.handleCreate(ctx, data)
	case Drop:
		err = h.handleDrop(ctx, data)
	case DropDatabase:
		err = h.handleDropDatabase(ctx, data)
	case CreateIndexes:
		err = h.handleCreateIndexes(ctx, data)
	case DropIndexes:
		err = h.handleDropIndexes(ctx, data)
	case Insert:
		err = h.handleInsert(ctx, data)
	case Delete:
		err = h.handleDelete(ctx, data)
	case Replace:
		err = h.handleReplace(ctx, data)
	case Update:
		err = h.handleUpdate(ctx, data)

	case Invalidate:
		event, err := parseEvent[InvalidateEvent](data)
		if err != nil {
			return baseEvent.ClusterTime, errors.Wrap(err, "invalidate: parse")
		}

		return baseEvent.ClusterTime, &InvalidatedError{event.ID}

	case Rename:
		fallthrough
	case Modify:
		fallthrough
	case ShardCollection:
		fallthrough
	case ReshardCollection:
		fallthrough
	case RefineCollectionShardKey:
		fallthrough

	default:
		return baseEvent.ClusterTime, &UnsupportedEventError{OpType: baseEvent.OperationType}
	}

	return baseEvent.ClusterTime, errors.Wrap(err, string(baseEvent.OperationType))
}

func (h *eventApplier) handleCreate(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[CreateEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	if event.IsTimeseries() {
		log.Warn(ctx, "timeseries is not supported. skip")
		return nil
	}

	if h.Drop {
		err = dropCollection(ctx,
			h.Client,
			event.Namespace.Database,
			event.Namespace.Collection)
		if err != nil {
			return errors.Wrap(err, "drop before create")
		}
	}

	if event.IsView() {
		return createView(ctx,
			h.Client,
			event.Namespace.Database,
			event.Namespace.Collection,
			event.OperationDescription,
		)
	}

	return createCollection(ctx,
		h.Client,
		event.Namespace.Database,
		event.Namespace.Collection,
		event.OperationDescription,
	)
}

func (h *eventApplier) handleDrop(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[DropEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	err = dropCollection(ctx, h.Client, event.Namespace.Database, event.Namespace.Collection)
	if err != nil {
		return err
	}

	h.IndexCatalog.DropCollection(event.Namespace.Database, event.Namespace.Collection)
	return nil
}

func (h *eventApplier) handleDropDatabase(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[DropDatabaseEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	err = h.Client.Database(event.Namespace.Database).Drop(ctx)
	if err != nil {
		return err //nolint:wrapcheck
	}

	h.IndexCatalog.DropDatabase(event.Namespace.Database)
	return nil
}

func (h *eventApplier) handleCreateIndexes(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[CreateIndexesEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	indexes := make([]mongo.IndexModel, len(event.OperationDescription.Indexes))
	for i, index := range event.OperationDescription.Indexes {
		model := options.IndexOptions{
			Name:    &index.Name,
			Version: &index.Version,
			Unique:  index.Unique,
			Sparse:  index.Sparse,
			Hidden:  index.Hidden,

			PartialFilterExpression: index.PartialFilterExpression,

			Collation: index.Collation,
		}

		indexes[i] = mongo.IndexModel{
			Keys:    index.KeysDocument,
			Options: &model,
		}
	}

	_, err = h.Client.Database(event.Namespace.Database).
		Collection(event.Namespace.Collection).
		Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return err //nolint:wrapcheck
	}

	h.IndexCatalog.CreateIndexes(
		event.Namespace.Database,
		event.Namespace.Collection,
		event.OperationDescription.Indexes)
	return nil
}

func (h *eventApplier) handleDropIndexes(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[DropIndexesEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	for _, index := range event.OperationDescription.Indexes {
		_, err = h.Client.Database(event.Namespace.Database).
			Collection(event.Namespace.Collection).
			Indexes().DropOne(ctx, index.Name)
		if err != nil {
			if !isIndexNotFound(err) {
				return errors.Wrapf(err, "drop %s index in %s", index.Name, event.Namespace)
			}

			log.Debug(ctx, "index not found "+index.Name)
		}

		h.IndexCatalog.DropIndex(
			event.Namespace.Database,
			event.Namespace.Collection,
			index.Name)
	}

	return nil
}

func isIndexNotFound(err error) bool {
	for ; err != nil; err = errors.Unwrap(err) {
		le, ok := err.(mongo.CommandError) //nolint:errorlint
		if ok && le.Name == "IndexNotFound" {
			return true
		}
	}
	return false
}

func (h *eventApplier) handleInsert(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[InsertEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	// use replaceOne to ensure the changed version
	_, err = h.Client.Database(event.Namespace.Database).
		Collection(event.Namespace.Collection).
		InsertOne(ctx, event.FullDocument)

	return err //nolint:wrapcheck
}

func (h *eventApplier) handleDelete(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[DeleteEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	_, err = h.Client.Database(event.Namespace.Database).
		Collection(event.Namespace.Collection).
		DeleteOne(ctx, event.DocumentKey)
	return err //nolint:wrapcheck
}

func (h *eventApplier) handleUpdate(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[UpdateEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	ops := bson.D{}
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

	_, err = h.Client.Database(event.Namespace.Database).
		Collection(event.Namespace.Collection).
		UpdateOne(ctx, event.DocumentKey, ops)
	return err //nolint:wrapcheck
}

func (h *eventApplier) handleReplace(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[ReplaceEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	_, err = h.Client.Database(event.Namespace.Database).
		Collection(event.Namespace.Collection).
		ReplaceOne(ctx, event.DocumentKey, event.FullDocument)
	return err //nolint:wrapcheck
}
