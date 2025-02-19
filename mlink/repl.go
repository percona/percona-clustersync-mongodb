package mlink

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
	"github.com/percona-lab/percona-mongolink/util"
)

type Repl struct {
	Source   *mongo.Client
	Target   *mongo.Client
	Drop     bool
	NSFilter util.NSFilter
	Catalog  *Catalog

	lastAppliedOpTime bson.Timestamp
	resumeToken       bson.Raw

	mu      sync.Mutex
	err     error
	stopSig chan struct{}
	doneSig chan struct{}
	running bool

	eventsProcessed int64
}

type ChangeReplicationStatus struct {
	LastAppliedOpTime bson.Timestamp
	EventsProcessed   int64
}

func (r *Repl) Status() ChangeReplicationStatus {
	r.mu.Lock()
	defer r.mu.Unlock()

	return ChangeReplicationStatus{
		LastAppliedOpTime: r.lastAppliedOpTime,
		EventsProcessed:   r.eventsProcessed,
	}
}

func (r *Repl) Wait() error {
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

func (r *Repl) Start(ctx context.Context, startAt bson.Timestamp) error {
	return r.startImpl(ctx, &startAt)
}

func (r *Repl) Resume(ctx context.Context) error {
	return r.startImpl(ctx, nil)
}

func (r *Repl) Pause() {
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

func (r *Repl) pause(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.err = err
	r.running = false
}

func (r *Repl) startImpl(ctx context.Context, ts *bson.Timestamp) error {
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

	opts := options.ChangeStream()
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

func (r *Repl) loop(ctx context.Context, opts *options.ChangeStreamOptionsBuilder) {
	ctx = log.WithAttrs(ctx, log.Scope("repl:loop"))

	eventC := make(chan bson.Raw, 100)
	cursorCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		<-r.stopSig
		cancel()
	}()

	var changeStreamError error

	go func() {
		defer close(eventC)

		const tickInternal = 5 * time.Second
		ticker := time.NewTicker(tickInternal)
		defer ticker.Stop()

		go func() {
			ctx := log.WithAttrs(ctx, log.Scope("repl:loop:tick"))
			coll := r.Source.Database("percona_mongolink").Collection("tick")

			for {
				select {
				case t := <-ticker.C:
					_, err := coll.UpdateOne(ctx,
						bson.D{{"_id", ""}},
						bson.D{{"$set", bson.D{{"t", t}}}},
						options.UpdateOne().SetUpsert(true))
					if err != nil {
						log.Error(ctx, err, "")
					}
				case <-r.stopSig:
					return
				}
			}
		}()

		opts.SetShowExpandedEvents(true).SetBatchSize(100)
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
			eventC <- cur.Current
			ticker.Reset(tickInternal)
		}
		if err := cur.Err(); err != nil {
			if !errors.Is(err, context.Canceled) {
				changeStreamError = errors.Wrap(err, "next")
				return
			}

			return
		}
	}()

	applyCtx := log.CopyContext(ctx, context.Background())
	var txBuffer util.List[bson.Raw]
	for {
		event, ok := <-eventC
		if !ok {
			r.pause(errors.Wrap(changeStreamError, "cursor"))
			return
		}

		firstTxOp := TxnEvent(event)
		for firstTxOp.IsTxn() {
			txBuffer.Push(event)
			for {
				innerEvent, ok := <-eventC
				if !ok {
					r.pause(errors.Wrap(changeStreamError, "cursor"))
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

func (r *Repl) apply(ctx context.Context, data bson.Raw) error {
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
	r.eventsProcessed++
	r.mu.Unlock()
	return nil
}

func (r *Repl) handleCreate(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[CreateEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	if event.IsTimeseries() {
		log.Warn(ctx, "timeseries is not supported. skip")
		return nil
	}

	if r.Drop {
		err = r.Catalog.DropCollection(ctx,
			r.Target,
			event.Namespace.Database,
			event.Namespace.Collection)
		if err != nil {
			return errors.Wrap(err, "drop before create")
		}
	}

	if event.IsView() {
		return r.Catalog.CreateView(ctx,
			r.Target,
			event.Namespace.Database,
			event.Namespace.Collection,
			&event.OperationDescription,
		)
	}

	return r.Catalog.CreateCollection(ctx,
		r.Target,
		event.Namespace.Database,
		event.Namespace.Collection,
		&event.OperationDescription,
	)
}

func (r *Repl) handleDrop(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[DropEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	err = r.Catalog.DropCollection(ctx, r.Target, event.Namespace.Database, event.Namespace.Collection)
	return err
}

func (r *Repl) handleDropDatabase(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[DropDatabaseEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	err = r.Catalog.DropDatabase(ctx, r.Target, event.Namespace.Database)
	return err
}

func (r *Repl) handleCreateIndexes(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[CreateIndexesEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	err = r.Catalog.CreateIndexes(ctx,
		r.Target,
		event.Namespace.Database,
		event.Namespace.Collection,
		event.OperationDescription.Indexes)
	return err
}

func (r *Repl) handleDropIndexes(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[DropIndexesEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	for _, index := range event.OperationDescription.Indexes {
		err = r.Catalog.DropIndex(ctx,
			r.Target,
			event.Namespace.Database,
			event.Namespace.Collection,
			index.Name)
		if err != nil {
			return errors.Wrap(err, "drop index "+index.Name)
		}
	}

	return nil
}

func (r *Repl) handleModify(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[ModifyEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	db := event.Namespace.Database
	coll := event.Namespace.Collection
	opts := event.OperationDescription

	switch {
	case opts.Index != nil:
		err = r.Catalog.ModifyIndex(ctx, r.Target, db, coll, opts.Index)
		if err != nil {
			log.Error(ctx, err, "modify index: "+opts.Index.Name)
			return nil
		}

	case opts.CappedSize != nil || opts.CappedMax != nil:
		err = r.Catalog.ModifyCappedCollection(ctx, r.Target, db, coll, opts.CappedSize, opts.CappedMax)
		if err != nil {
			log.Error(ctx, err, "resize capped collection")
			return nil
		}

	case opts.ViewOn != "":
		if strings.HasPrefix(opts.ViewOn, "system.buckets.") {
			log.Warn(ctx, "timeseries is not supported. skip")
			return nil
		}

		err = r.Catalog.ModifyView(ctx, r.Target, db, coll, opts.ViewOn, opts.Pipeline)
		if err != nil {
			log.Error(ctx, err, "modify view")
			return nil
		}

	case opts.ExpireAfterSeconds != nil:
		log.Warn(ctx, "collection ttl modification is not supported")

	case opts.ChangeStreamPreAndPostImages != nil:
		log.Warn(ctx, "changeStreamPreAndPostImages is not supported")

	case opts.Validator != nil || opts.ValidatorLevel != nil || opts.ValidatorAction != nil:
		log.Warn(ctx, "validator, validatorLevel and validatorAction are not supported")

	case opts.Unknown == nil:
		log.Debug(ctx, "empty modify event")

	default:
		log.Error(ctx, errors.New("unknown modify options"), "")
	}

	return nil
}

var insertDocOptions = options.Replace().SetUpsert(true)

func (r *Repl) handleInsert(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[InsertEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	_, err = r.Target.Database(event.Namespace.Database).
		Collection(event.Namespace.Collection).
		ReplaceOne(ctx, event.DocumentKey, event.FullDocument, insertDocOptions)

	return err //nolint:wrapcheck
}

func (r *Repl) handleDelete(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[DeleteEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	_, err = r.Target.Database(event.Namespace.Database).
		Collection(event.Namespace.Collection).
		DeleteOne(ctx, event.DocumentKey)
	return err //nolint:wrapcheck
}

func (r *Repl) handleUpdate(ctx context.Context, data bson.Raw) error {
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

func (r *Repl) handleReplace(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[ReplaceEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	_, err = r.Target.Database(event.Namespace.Database).
		Collection(event.Namespace.Collection).
		ReplaceOne(ctx, event.DocumentKey, event.FullDocument)
	return err //nolint:wrapcheck
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
