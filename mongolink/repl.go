package mongolink

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona-lab/percona-mongolink/config"
	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/list"
	"github.com/percona-lab/percona-mongolink/log"
	"github.com/percona-lab/percona-mongolink/sel"
)

// Repl handles replication from a source MongoDB to a target MongoDB.
type Repl struct {
	Source *mongo.Client // Source MongoDB client
	Target *mongo.Client // Target MongoDB client

	NSFilter sel.NSFilter // Namespace filter
	Catalog  *Catalog     // Catalog for managing collections and indexes

	lastAppliedOpTime bson.Timestamp
	resumeToken       bson.Raw

	mu      sync.Mutex
	err     error
	stopSig chan struct{}
	doneSig chan struct{}

	eventsProcessed int64
}

// ChangeReplicationStatus represents the status of change replication.
type ChangeReplicationStatus struct {
	LastAppliedOpTime bson.Timestamp // Last applied operation time
	EventsProcessed   int64          // Number of events processed
	Error             error
}

// Status returns the current replication status.
func (r *Repl) Status() ChangeReplicationStatus {
	r.mu.Lock()
	defer r.mu.Unlock()

	return ChangeReplicationStatus{
		LastAppliedOpTime: r.lastAppliedOpTime,
		EventsProcessed:   r.eventsProcessed,
		Error:             r.err,
	}
}

// Done returns a channel that is closed when the replication is done.
func (r *Repl) Done() <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.doneSig
}

// Start begins the replication process from the specified start timestamp.
func (r *Repl) Start(_ context.Context, startAt bson.Timestamp) error {
	return r.startImpl(&startAt)
}

// Resume resumes the replication process from the last known resume token.
func (r *Repl) Resume(_ context.Context) error {
	return r.startImpl(nil)
}

// Pause pauses the replication process.
func (r *Repl) Pause() {
	r.pause(nil)
}

// pause sets the replication error and marks it as not running.
func (r *Repl) pause(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopSig != nil {
		select {
		case <-r.stopSig:
		default:
			close(r.stopSig)
		}
	}

	r.err = err
}

// startImpl starts the replication process with the given options.
func (r *Repl) startImpl(ts *bson.Timestamp) error {
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
		ctx, cancel := context.WithCancel(context.Background())

		defer func() {
			cancel()

			r.mu.Lock()
			defer r.mu.Unlock()

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

// loop handles the main replication loop.
func (r *Repl) loop(ctx context.Context, opts *options.ChangeStreamOptionsBuilder) {
	lg := log.New("repl:apply")
	ctx = lg.WithContext(ctx)

	eventC := make(chan bson.Raw, config.ChangeStreamBatchSize)
	cursorCtx, stopCursor := context.WithCancel(ctx)

	defer stopCursor()

	go func() {
		<-r.stopSig
		stopCursor()
	}()

	var changeStreamError error

	go func() {
		defer close(eventC)

		lg := log.New("repl:apply")

		ticker := time.NewTicker(config.ReplTickInteral)
		defer ticker.Stop()

		go func() {
			coll := r.Source.Database(config.MongoLinkDatabase).Collection(config.TickCollection)

			for {
				select {
				case t := <-ticker.C:
					_, err := coll.UpdateOne(cursorCtx,
						bson.D{{"_id", ""}},
						bson.D{{"$set", bson.D{{"t", t}}}},
						options.UpdateOne().SetUpsert(true))
					if err != nil {
						log.New("repl:tick").Error(err, "")
					}

				case <-r.stopSig:
					return
				}
			}
		}()

		opts.SetShowExpandedEvents(true).SetBatchSize(config.ChangeStreamBatchSize)

		cur, err := r.Source.Watch(cursorCtx, mongo.Pipeline{}, opts)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				changeStreamError = errors.Wrap(err, "open")
			}

			lg.Error(err, "open cursor")

			return
		}

		defer func() {
			if err := cur.Close(cursorCtx); err != nil {
				lg.Error(err, "close change stream cursor")
			}
		}()

		for cur.Next(cursorCtx) {
			eventC <- cur.Current

			ticker.Reset(config.ReplTickInteral)
		}

		if err := cur.Err(); err != nil {
			if !errors.Is(err, context.Canceled) {
				changeStreamError = errors.Wrap(err, "next")

				return
			}

			return
		}
	}()

	applyCtx, stopApply := context.WithCancel(context.Background())
	applyCtx = lg.WithContext(applyCtx)
	defer stopApply()

	var txBuffer list.List[bson.Raw]

	for {
		event, ok := <-eventC
		if !ok {
			r.pause(errors.Wrap(changeStreamError, "cursor"))

			return
		}

		firstTxOp := parseTxnEvent(event)
		for firstTxOp.IsTxn() {
			txBuffer.Push(event)

			for {
				innerEvent, ok := <-eventC
				if !ok {
					r.pause(errors.Wrap(changeStreamError, "cursor"))

					return
				}

				txOp := parseTxnEvent(innerEvent)
				if !firstTxOp.Equal(txOp) {
					event = innerEvent
					firstTxOp = txOp

					break
				}

				txBuffer.Push(innerEvent)
			}

			// apply all transactional ops
			for event := range txBuffer.All() {
				err := r.apply(applyCtx, event)
				if err != nil {
					lg.Error(err, "apply transaction")
					r.pause(errors.Join(changeStreamError, errors.Wrap(err, "apply transaction")))

					return
				}
			}

			txBuffer.Clear()
		}

		err := r.apply(applyCtx, event)
		if err != nil {
			lg.Error(err, "apply op")
			r.pause(errors.Join(changeStreamError, errors.Wrap(err, "apply op")))

			return
		}
	}
}

// apply applies a change event to the target MongoDB.
func (r *Repl) apply(ctx context.Context, data bson.Raw) error {
	var baseEvent BaseEvent

	err := bson.Unmarshal(data, &baseEvent)
	if err != nil {
		return errors.Wrap(err, "failed to decode BaseEvent")
	}

	lg := log.Ctx(ctx).With(
		log.OpTime(baseEvent.ClusterTime.T, baseEvent.ClusterTime.I),
		log.Op(string(baseEvent.OperationType)),
		log.NS(baseEvent.Namespace.Database, baseEvent.Namespace.Collection),
		log.Tx(baseEvent.TxnNumber, baseEvent.LSID))
	ctx = lg.WithContext(ctx)

	if !r.NSFilter(baseEvent.Namespace.Database, baseEvent.Namespace.Collection) {
		lg.Debug("not selected")

		r.mu.Lock()
		r.resumeToken = baseEvent.ID
		r.lastAppliedOpTime = baseEvent.ClusterTime
		r.mu.Unlock()

		return nil
	}

	lg.Trace("")

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
		lg.Error(err, "")

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
		lg.Warn("unsupported type: " + string(baseEvent.OperationType))

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

// handleCreate handles create events.
func (r *Repl) handleCreate(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[CreateEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	if event.IsTimeseries() {
		log.Ctx(ctx).Warn("timeseries is not supported. skip")

		return nil
	}

	err = r.Catalog.DropCollection(ctx,
		r.Target,
		event.Namespace.Database,
		event.Namespace.Collection)
	if err != nil {
		return errors.Wrap(err, "drop before create")
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

// handleDrop handles drop events.
func (r *Repl) handleDrop(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[DropEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	err = r.Catalog.DropCollection(ctx,
		r.Target,
		event.Namespace.Database,
		event.Namespace.Collection)

	return err
}

// handleDropDatabase handles drop database events.
func (r *Repl) handleDropDatabase(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[DropDatabaseEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	err = r.Catalog.DropDatabase(ctx,
		r.Target,
		event.Namespace.Database)

	return err
}

// handleCreateIndexes handles create indexes events.
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

// handleDropIndexes handles drop indexes events.
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

// handleModify handles modify events.
func (r *Repl) handleModify(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[ModifyEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	lg := log.Ctx(ctx)

	db := event.Namespace.Database
	coll := event.Namespace.Collection
	opts := event.OperationDescription

	switch {
	case opts.Index != nil:
		err = r.Catalog.ModifyIndex(ctx, r.Target, db, coll, opts.Index)
		if err != nil {
			lg.Error(err, "modify index: "+opts.Index.Name)

			return nil
		}

	case opts.CappedSize != nil || opts.CappedMax != nil:
		err = r.Catalog.ModifyCappedCollection(ctx, r.Target, db, coll, opts.CappedSize, opts.CappedMax)
		if err != nil {
			lg.Error(err, "resize capped collection")

			return nil
		}

	case opts.ViewOn != "":
		if strings.HasPrefix(opts.ViewOn, "system.buckets.") {
			lg.Warn("timeseries is not supported. skip")

			return nil
		}

		err = r.Catalog.ModifyView(ctx, r.Target, db, coll, opts.ViewOn, opts.Pipeline)
		if err != nil {
			lg.Error(err, "modify view")

			return nil
		}

	case opts.ExpireAfterSeconds != nil:
		lg.Warn("collection ttl modification is not supported")

	case opts.ChangeStreamPreAndPostImages != nil:
		lg.Warn("changeStreamPreAndPostImages is not supported")

	case opts.Validator != nil || opts.ValidatorLevel != nil || opts.ValidatorAction != nil:
		lg.Warn("validator, validatorLevel and validatorAction are not supported")

	case opts.Unknown == nil:
		lg.Debug("empty modify event")

	default:
		lg.Error(errors.New("unknown modify options"), "")
	}

	return nil
}

var insertDocOptions = options.Replace().SetUpsert(true) //nolint:gochecknoglobals

// handleInsert handles insert events.
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

// handleDelete handles delete events.
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

// handleUpdate handles update events.
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

// handleReplace handles replace events.
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

// txnEvent represents a transaction event.
type txnEvent struct {
	TxnNumber int64    `bson:"txnNumber"` // Transaction number
	LSID      bson.Raw `bson:"lsid"`      // Logical session ID
}

// parseTxnEvent extracts a transaction event from raw BSON data.
func parseTxnEvent(data bson.Raw) txnEvent {
	var e txnEvent
	_ = bson.Unmarshal(data, &e)

	return e
}

// IsTxn checks if the event is part of a transaction.
func (t txnEvent) IsTxn() bool {
	return t.TxnNumber != 0
}

// Equal checks if two transaction events are equal.
func (t txnEvent) Equal(o txnEvent) bool {
	return t.TxnNumber == o.TxnNumber && bytes.Equal(t.LSID, o.LSID)
}
