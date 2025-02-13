package repl

import (
	"bytes"
	"context"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
)

type ChangeReplicator struct {
	Source   *mongo.Client
	Target   *mongo.Client
	Drop     bool
	NSFilter NSFilter
	Catalog  *Catalog

	lastAppliedOpTime primitive.Timestamp
	resumeToken       bson.Raw

	mu      sync.Mutex
	err     error
	stopSig chan struct{}
	doneSig chan struct{}
	running bool
}

func (r *ChangeReplicator) GetLastAppliedOpTime() primitive.Timestamp {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.lastAppliedOpTime
}

func (r *ChangeReplicator) Wait() error {
	r.mu.Lock()
	doneC := r.doneSig
	r.mu.Unlock()
	if doneC != nil {
		<-r.doneSig
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	return r.err
}

func (r *ChangeReplicator) Start(ctx context.Context, startAt primitive.Timestamp) error {
	return r.startImpl(ctx, &startAt)
}

func (r *ChangeReplicator) Resume(ctx context.Context) error {
	return r.startImpl(ctx, nil)
}

func (r *ChangeReplicator) Pause() {
	r.mu.Lock()
	stopSig := r.stopSig
	r.mu.Unlock()

	if stopSig != nil {
		select {
		case <-stopSig:
		default:
			close(stopSig)
		}
	}

	r.pause(nil)
}

func (r *ChangeReplicator) pause(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.err = err
	r.running = false
}

func (r *ChangeReplicator) startImpl(ctx context.Context, ts *primitive.Timestamp) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopSig != nil {
		select {
		case <-r.stopSig:
		default:
			return errors.New("already running")
		}
	}

	if r.doneSig != nil {
		select {
		case <-r.doneSig:
		default:
			return errors.New("still running")
		}
	}

	if r.err != nil {
		return errors.New("cannot resume due to existing error")
	}

	opts := options.ChangeStream().SetShowExpandedEvents(true)
	if ts != nil {
		opts.SetStartAtOperationTime(ts)
	} else {
		if r.resumeToken == nil {
			return errors.New("no resume token")
		}
		opts.SetResumeAfter(r.resumeToken)
	}

	r.stopSig = make(chan struct{})
	r.doneSig = make(chan struct{})

	started := make(chan struct{})
	go func() {
		ctx, cancel := context.WithCancel(log.CopyContext(ctx, context.Background()))

		defer func() {
			log.Trace(ctx, "canceling loop")
			cancel()

			select {
			case <-r.doneSig:
			default:
				close(r.doneSig)
			}
		}()

		close(started)
		r.loop(ctx, opts)
	}()

	<-started
	return nil
}

func (r *ChangeReplicator) loop(ctx context.Context, opts *options.ChangeStreamOptions) {
	ctx = log.WithAttrs(ctx, log.Scope("repl:loop"))

	eventC := make(chan bson.Raw, 100)
	cursorCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		<-r.stopSig
		log.Trace(ctx, "not running. stopping change stream")
		cancel()
	}()

	var changeStreamError error

	go func() {
		defer close(eventC)

		cur, err := r.Source.Watch(cursorCtx, mongo.Pipeline{}, opts)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				changeStreamError = errors.Wrap(err, "open")
			}

			log.Error(ctx, err, "open cursor")
			return
		}
		defer cur.Close(cursorCtx)

		for cur.Next(cursorCtx) {
			baseEvent, _ := parseEvent[BaseEvent](cur.Current)
			ctx := log.WithAttrs(ctx,
				log.Operation(string(baseEvent.OperationType)),
				log.OpTime(baseEvent.ClusterTime),
				log.NS(baseEvent.Namespace.Database, baseEvent.Namespace.Collection),
				log.Tx(baseEvent.TxnNumber, baseEvent.LSID))

			log.Trace(ctx, "")
			eventC <- cur.Current
		}
		if err := cur.Err(); err != nil {
			if !errors.Is(err, context.Canceled) {
				changeStreamError = errors.Wrap(err, "next")
				return
			}

			log.Trace(ctx, "context canceled. exit")
			return
		}
	}()

	applyCtx := log.CopyContext(ctx, context.Background())
	var txBuffer List[bson.Raw]
	for {
		event, ok := <-eventC
		if !ok {
			r.pause(errors.Wrap(changeStreamError, "cursor"))
			log.Trace(ctx, "exit")
			return
		}

		firstTxOp := TxnEvent(event)
		for firstTxOp.IsTxn() {
			txBuffer.Push(event)
			for {
				innerEvent, ok := <-eventC
				if !ok {
					r.pause(errors.Wrap(changeStreamError, "cursor"))
					log.Trace(ctx, "exit")
					return
				}

				txOp := TxnEvent(innerEvent)
				if !firstTxOp.Equal(txOp) {
					event = innerEvent
					firstTxOp = txOp
					break
				}
				txBuffer.Push(innerEvent)
			}

			// apply all transactional ops
			for e := range txBuffer.All() {
				err := r.apply(applyCtx, e)
				if err != nil {
					log.Error(ctx, err, "apply transaction")
					r.pause(errors.Wrap(err, "repl:apply"))
					return
				}
			}
			txBuffer.Clear()
		}

		err := r.apply(applyCtx, event)
		if err != nil {
			log.Error(ctx, err, "apply op")
			r.pause(errors.Wrap(err, "repl:apply"))
			return
		}
	}
}

func (r *ChangeReplicator) apply(ctx context.Context, data bson.Raw) error {
	var baseEvent BaseEvent
	err := bson.Unmarshal(data, &baseEvent)
	if err != nil {
		return errors.Wrap(err, "failed to decode BaseEvent")
	}

	ctx = log.WithAttrs(ctx,
		log.Scope("repl:apply"),
		log.Operation(string(baseEvent.OperationType)),
		log.OpTime(baseEvent.ClusterTime),
		log.NS(baseEvent.Namespace.Database, baseEvent.Namespace.Collection),
		log.Tx(baseEvent.TxnNumber, baseEvent.LSID))

	if !r.NSFilter(baseEvent.Namespace.Database, baseEvent.Namespace.Collection) {
		log.Debug(ctx, "not selected")

		r.mu.Lock()
		r.resumeToken = baseEvent.ID
		r.lastAppliedOpTime = baseEvent.ClusterTime
		r.mu.Unlock()
		return nil
	}

	log.Trace(ctx, "")

	switch baseEvent.OperationType {
	case Create:
		err = r.handleCreate(ctx, data)
	case Drop:
		err = r.handleDrop(ctx, data)
	case DropDatabase:
		err = r.handleDropDatabase(ctx, data)
	case CreateIndexes:
		err = r.handleCreateIndexes(ctx, data)
	case DropIndexes:
		err = r.handleDropIndexes(ctx, data)

	case Modify:
		err = r.handleModify(ctx, data)

	case Insert:
		err = r.handleInsert(ctx, data)
	case Delete:
		err = r.handleDelete(ctx, data)
	case Replace:
		err = r.handleReplace(ctx, data)
	case Update:
		err = r.handleUpdate(ctx, data)

	case Invalidate:
		err := errors.New("invalidate")
		log.Error(ctx, err, "")
		return nil

	case Rename:
		fallthrough
	case ShardCollection:
		fallthrough
	case ReshardCollection:
		fallthrough
	case RefineCollectionShardKey:
		fallthrough

	default:
		log.Warn(ctx, "unsupported type: "+string(baseEvent.OperationType))
		return nil
	}

	if err != nil {
		return errors.Wrap(err, string(baseEvent.OperationType))
	}

	r.mu.Lock()
	r.resumeToken = baseEvent.ID
	r.lastAppliedOpTime = baseEvent.ClusterTime
	r.mu.Unlock()
	return nil
}

func (r *ChangeReplicator) handleCreate(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[CreateEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	if event.IsTimeseries() {
		log.Warn(ctx, "timeseries is not supported. skip")
		return nil
	}

	if r.Drop {
		err = dropCollection(ctx,
			r.Target,
			event.Namespace.Database,
			event.Namespace.Collection)
		if err != nil {
			return errors.Wrap(err, "drop before create")
		}
	}

	if event.IsView() {
		return createView(ctx,
			r.Target,
			event.Namespace.Database,
			event.Namespace.Collection,
			event.OperationDescription,
		)
	}

	return createCollection(ctx,
		r.Target,
		event.Namespace.Database,
		event.Namespace.Collection,
		event.OperationDescription,
	)
}

func (r *ChangeReplicator) handleDrop(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[DropEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	err = dropCollection(ctx, r.Target, event.Namespace.Database, event.Namespace.Collection)
	if err != nil {
		return err
	}

	r.Catalog.DropCollection(event.Namespace.Database, event.Namespace.Collection)
	return nil
}

func (r *ChangeReplicator) handleDropDatabase(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[DropDatabaseEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	err = r.Target.Database(event.Namespace.Database).Drop(ctx)
	if err != nil {
		return err //nolint:wrapcheck
	}

	r.Catalog.DropDatabase(event.Namespace.Database)
	return nil
}

func (r *ChangeReplicator) handleCreateIndexes(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[CreateIndexesEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	err = buildIndexes(ctx,
		r.Target,
		event.Namespace.Database,
		event.Namespace.Collection,
		event.OperationDescription.Indexes)
	if err != nil {
		return err //nolint:wrapcheck
	}

	r.Catalog.CreateIndexes(
		event.Namespace.Database,
		event.Namespace.Collection,
		event.OperationDescription.Indexes)
	return nil
}

func (r *ChangeReplicator) handleDropIndexes(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[DropIndexesEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	for _, index := range event.OperationDescription.Indexes {
		_, err = r.Target.Database(event.Namespace.Database).
			Collection(event.Namespace.Collection).
			Indexes().DropOne(ctx, index.Name)
		if err != nil {
			if !isIndexNotFound(err) {
				return errors.Wrapf(err, "drop %s index in %s", index.Name, event.Namespace)
			}

			log.Debug(ctx, "index not found "+index.Name)
		}

		r.Catalog.DropIndex(
			event.Namespace.Database,
			event.Namespace.Collection,
			index.Name)
	}

	return nil
}

func (r *ChangeReplicator) handleModify(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[ModifyEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	db := event.Namespace.Database
	coll := event.Namespace.Collection
	opts := event.OperationDescription

	switch {
	case opts.Index != nil:
		if opts.Index.Hidden != nil {
			res := r.Target.Database(db).RunCommand(ctx, bson.D{
				{"collMod", coll},
				{"index", bson.D{
					{"name", opts.Index.Name},
					{"hidden", *opts.Index.Hidden},
				}},
			})
			if err := res.Err(); err != nil {
				return errors.Wrap(err, "convert index: "+opts.Index.Name)
			}

			return nil
		}

		fallthrough
	default:
		return errors.New("unknown modify")
	}
}

func (r *ChangeReplicator) handleInsert(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[InsertEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	// TODO: use replaceOne to ensure the changed version
	_, err = r.Target.Database(event.Namespace.Database).
		Collection(event.Namespace.Collection).
		InsertOne(ctx, event.FullDocument)

	if mongo.IsDuplicateKeyError(err) {
		// log.Error(ctx, err, "DuplicateKeyError")
		err = nil
	}
	return err //nolint:wrapcheck
}

func (r *ChangeReplicator) handleDelete(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[DeleteEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	_, err = r.Target.Database(event.Namespace.Database).
		Collection(event.Namespace.Collection).
		DeleteOne(ctx, event.DocumentKey)
	return err //nolint:wrapcheck
}

func (r *ChangeReplicator) handleUpdate(ctx context.Context, data bson.Raw) error {
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

	_, err = r.Target.Database(event.Namespace.Database).
		Collection(event.Namespace.Collection).
		UpdateOne(ctx, event.DocumentKey, ops)
	return err //nolint:wrapcheck
}

func (r *ChangeReplicator) handleReplace(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[ReplaceEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	_, err = r.Target.Database(event.Namespace.Database).
		Collection(event.Namespace.Collection).
		ReplaceOne(ctx, event.DocumentKey, event.FullDocument)
	return err //nolint:wrapcheck
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

type txnEvent struct {
	TxnNumber int64    `bson:"txnNumber"`
	LSID      bson.Raw `bson:"lsid"`
}

func TxnEvent(data bson.Raw) txnEvent {
	var e txnEvent
	_ = bson.Unmarshal(data, &e)
	return e
}

func (t txnEvent) IsTxn() bool {
	return t.TxnNumber != 0
}

func (t txnEvent) Equal(o txnEvent) bool {
	return t.TxnNumber == o.TxnNumber && bytes.Equal(t.LSID, o.LSID)
}
