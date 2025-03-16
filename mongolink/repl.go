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
	"github.com/percona-lab/percona-mongolink/topo"
)

var (
	ErrInvalidateEvent  = errors.New("invalidate")
	ErrOplogHistoryLost = errors.New("oplog history is lost")
)

// Repl handles replication from a source MongoDB to a target MongoDB.
type Repl struct {
	source *mongo.Client // Source MongoDB client
	target *mongo.Client // Target MongoDB client

	nsFilter sel.NSFilter // Namespace filter
	catalog  *Catalog     // Catalog for managing collections and indexes

	lastReplicatedOpTime bson.Timestamp
	resumeToken          bson.Raw

	lock sync.Mutex
	err  error

	eventsProcessed int64

	startTime time.Time
	pauseTime time.Time

	pausing bool
	pauseC  chan struct{}
	doneSig chan struct{}
}

// ReplStatus represents the status of change replication.
type ReplStatus struct {
	StartTime time.Time
	PauseTime time.Time

	LastReplicatedOpTime bson.Timestamp // Last applied operation time
	EventsProcessed      int64          // Number of events processed

	Err error
}

//go:inline
func (rs *ReplStatus) IsStarted() bool {
	return !rs.StartTime.IsZero()
}

//go:inline
func (rs *ReplStatus) IsRunning() bool {
	return rs.IsStarted() && !rs.IsPaused()
}

//go:inline
func (rs *ReplStatus) IsPaused() bool {
	return !rs.PauseTime.IsZero()
}

func NewRepl(source, target *mongo.Client, catalog *Catalog, nsFilter sel.NSFilter) *Repl {
	return &Repl{
		source:   source,
		target:   target,
		nsFilter: nsFilter,
		catalog:  catalog,
		pauseC:   make(chan struct{}),
		doneSig:  make(chan struct{}),
	}
}

type replCheckpoint struct {
	StartTime            time.Time      `bson:"startTime,omitempty"`
	PauseTime            time.Time      `bson:"pauseTime,omitempty"`
	ResumeToken          bson.Raw       `bson:"resumeToken,omitempty"`
	EventsProcessed      int64          `bson:"events,omitempty"`
	LastReplicatedOpTime bson.Timestamp `bson:"lastOpTS,omitempty"`
	Error                string         `bson:"error,omitempty"`
}

func (r *Repl) Checkpoint() *replCheckpoint { //nolint:revive
	r.lock.Lock()
	defer r.lock.Unlock()

	cp := &replCheckpoint{
		StartTime:            r.startTime,
		PauseTime:            r.pauseTime,
		ResumeToken:          r.resumeToken,
		EventsProcessed:      r.eventsProcessed,
		LastReplicatedOpTime: r.lastReplicatedOpTime,
	}

	if r.err != nil {
		cp.Error = r.err.Error()
	}

	return cp
}

func (r *Repl) Recover(cp *replCheckpoint) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.err != nil {
		return errors.Wrap(r.err, "cannot recover due an existing error")
	}

	if !r.startTime.IsZero() {
		return errors.New("cannot recovery: already used")
	}

	pauseTime := cp.PauseTime
	if pauseTime.IsZero() {
		pauseTime = time.Now()
	}

	r.startTime = cp.StartTime
	r.pauseTime = pauseTime
	r.resumeToken = cp.ResumeToken
	r.eventsProcessed = cp.EventsProcessed
	r.lastReplicatedOpTime = cp.LastReplicatedOpTime

	if cp.Error != "" {
		r.err = errors.New(cp.Error)
	}

	return nil
}

// Status returns the current replication status.
func (r *Repl) Status() ReplStatus {
	r.lock.Lock()
	defer r.lock.Unlock()

	return ReplStatus{
		LastReplicatedOpTime: r.lastReplicatedOpTime,
		EventsProcessed:      r.eventsProcessed,

		StartTime: r.startTime,
		PauseTime: r.pauseTime,

		Err: r.err,
	}
}

func (r *Repl) Done() <-chan struct{} {
	r.lock.Lock()
	defer r.lock.Unlock()

	return r.doneSig
}

// Start begins the replication process from the specified start timestamp.
func (r *Repl) Start(_ context.Context, startAt bson.Timestamp) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.err != nil {
		return errors.Wrap(r.err, "cannot start due an existing error")
	}

	if !r.startTime.IsZero() {
		return errors.New("already started")
	}

	go r.run(options.ChangeStream().SetStartAtOperationTime(&startAt))

	r.startTime = time.Now()

	log.New("repl").With(log.OpTime(startAt.T, startAt.I)).
		Info("Change Replication started")

	return nil
}

// Pause pauses the change replication.
func (r *Repl) Pause(context.Context) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.startTime.IsZero() {
		return errors.New("not running")
	}

	if r.pausing {
		return errors.New("already pausing")
	}

	if !r.pauseTime.IsZero() {
		return errors.New("already paused")
	}

	r.pausing = true
	doneSig := r.doneSig

	go func() {
		log.New("repl").Debug("Change Replication is pausing")

		r.pauseC <- struct{}{}
		<-doneSig

		r.lock.Lock()
		r.pauseTime = time.Now()
		r.pausing = false
		optime := r.lastReplicatedOpTime
		r.lock.Unlock()

		log.New("repl").With(log.OpTime(optime.T, optime.I)).
			Info("Change Replication paused")
	}()

	return nil
}

func (r *Repl) Resume(context.Context) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	log.New("repl").Debug("Change Replication is resuming")

	if r.startTime.IsZero() {
		return errors.New("not started")
	}

	if r.pausing {
		return errors.New("pausing")
	}

	if r.pauseTime.IsZero() {
		return errors.New("not paused")
	}

	if r.resumeToken == nil {
		return errors.New("no resume token")
	}

	r.pauseTime = time.Time{}
	r.doneSig = make(chan struct{})

	go r.run(options.ChangeStream().SetResumeAfter(r.resumeToken))

	log.New("repl").With(log.OpTime(r.lastReplicatedOpTime.T, r.lastReplicatedOpTime.I)).
		Info("Change Replication resumed")

	return nil
}

func (r *Repl) run(opts *options.ChangeStreamOptionsBuilder) {
	defer close(r.doneSig)

	watchCtx, stopWatch := context.WithCancel(context.Background())
	defer stopWatch()

	go func() {
		<-r.pauseC
		stopWatch()
	}()

	eventC := make(chan bson.Raw, config.ChangeStreamBatchSize)

	go func() {
		err := r.watchChangeStream(watchCtx, opts, eventC)
		if err != nil && !errors.Is(err, context.Canceled) {
			if topo.IsChangeStreamHistoryLost(err) {
				err = ErrOplogHistoryLost
			}

			r.lock.Lock()
			r.err = errors.Wrap(err, "watch change stream")
			r.lock.Unlock()

			log.New("repl:loop").Error(err, "watch change stream")
		}
	}()

	replCtx, stopRepl := context.WithCancel(context.Background())
	defer stopRepl()

	err := r.replication(replCtx, eventC)
	if err != nil {
		r.lock.Lock()
		r.err = errors.Wrap(err, "replication")
		r.lock.Unlock()

		log.New("repl:loop").Error(err, "Change Replication")
	}
}

func (r *Repl) watchChangeStream(
	ctx context.Context,
	streamOptions *options.ChangeStreamOptionsBuilder,
	eventC chan<- bson.Raw,
) error {
	defer close(eventC)

	ticker := time.NewTicker(config.ReplTickInteral)
	defer ticker.Stop()

	go r.doTicks(ctx, ticker.C)

	cur, err := r.source.Watch(ctx, mongo.Pipeline{}, streamOptions.SetShowExpandedEvents(true))
	if err != nil {
		return errors.Wrap(err, "open")
	}

	defer func() {
		if err := cur.Close(ctx); err != nil {
			log.New("repl:watch").Error(err, "Close change stream cursor")
		}
	}()

	for cur.Next(ctx) {
		eventC <- cur.Current

		ticker.Reset(config.ReplTickInteral)
	}

	if err := cur.Err(); err != nil {
		return errors.Wrap(err, "cursor")
	}

	return nil
}

func (r *Repl) doTicks(ctx context.Context, tickC <-chan time.Time) {
	coll := r.source.Database(config.MongoLinkDatabase).Collection(config.TickCollection)

	for {
		select {
		case <-ctx.Done():
			return

		case t := <-tickC:
			_, err := coll.UpdateOne(ctx,
				bson.D{{"_id", ""}},
				bson.D{{"$set", bson.D{{"t", t}}}},
				options.UpdateOne().SetUpsert(true))
			if err != nil {
				log.New("repl:tick").Error(err, "")
			}
		}
	}
}

func (r *Repl) replication(ctx context.Context, eventC <-chan bson.Raw) error {
	lg := log.New("repl:apply")

	var txBuffer list.List[bson.Raw]

	for {
		event, ok := <-eventC
		if !ok {
			return nil
		}

		firstTxOp := parseTxnEvent(event)
		for firstTxOp.IsTxn() {
			txBuffer.Push(event)

			for {
				innerEvent, ok := <-eventC
				if !ok {
					return nil
				}

				txOp := parseTxnEvent(innerEvent)
				if !firstTxOp.Equal(txOp) {
					event = innerEvent
					firstTxOp = txOp

					break
				}

				txBuffer.Push(innerEvent)
			}

			var opTime bson.Timestamp
			// apply all transactional ops
			for event := range txBuffer.All() {
				var err error

				opTime, err = r.apply(ctx, event)
				if err != nil {
					lg.Error(err, "Apply transaction")

					return errors.Wrap(err, "apply transaction")
				}
			}

			lg.With(log.Tx(&firstTxOp.TxnNumber, firstTxOp.LSID)).Trace("transaction applied")

			r.lock.Lock()
			r.lastReplicatedOpTime = opTime
			r.lock.Unlock()

			txBuffer.Clear()
		}

		opTime, err := r.apply(ctx, event)
		if err != nil {
			lg.Error(err, "Apply change")

			return errors.Wrap(err, "apply change")
		}

		r.lock.Lock()
		r.lastReplicatedOpTime = opTime
		r.lock.Unlock()
	}
}

// apply applies a change event to the target MongoDB.
func (r *Repl) apply(ctx context.Context, data bson.Raw) (bson.Timestamp, error) {
	var baseEvent BaseEvent

	err := bson.Unmarshal(data, &baseEvent)
	if err != nil {
		return bson.Timestamp{}, errors.Wrap(
			err,
			"failed to decode BaseEvent",
		)
	}

	opTime := baseEvent.ClusterTime
	lg := log.Ctx(ctx).With(
		log.OpTime(opTime.T, opTime.I),
		log.Op(string(baseEvent.OperationType)),
		log.NS(baseEvent.Namespace.Database, baseEvent.Namespace.Collection),
		log.Tx(baseEvent.TxnNumber, baseEvent.LSID))
	ctx = lg.WithContext(ctx)

	if !r.nsFilter(baseEvent.Namespace.Database, baseEvent.Namespace.Collection) {
		lg.Debug("not selected")

		r.lock.Lock()
		r.resumeToken = baseEvent.ID
		r.lock.Unlock()

		return opTime, nil
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
	case Rename:
		err = r.handleRename(ctx, data)

	case Insert:
		err = r.handleInsert(ctx, data)
	case Delete:
		err = r.handleDelete(ctx, data)
	case Replace:
		err = r.handleReplace(ctx, data)
	case Update:
		err = r.handleUpdate(ctx, data)

	case Invalidate:
		lg.Error(ErrInvalidateEvent, "")

		return opTime, ErrInvalidateEvent

	case ShardCollection:
		fallthrough
	case ReshardCollection:
		fallthrough
	case RefineCollectionShardKey:
		fallthrough

	default:
		lg.Warn("Unsupported type: " + string(baseEvent.OperationType))

		return opTime, nil
	}

	if err != nil {
		return opTime, errors.Wrap(err, string(baseEvent.OperationType))
	}

	r.lock.Lock()
	r.resumeToken = baseEvent.ID
	r.eventsProcessed++
	r.lock.Unlock()

	return opTime, nil
}

// handleCreate handles create events.
func (r *Repl) handleCreate(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[CreateEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	if event.IsTimeseries() {
		log.Ctx(ctx).Warn("Timeseries is not supported. skipping")

		return nil
	}

	err = r.catalog.DropCollection(ctx, event.Namespace.Database, event.Namespace.Collection)
	if err != nil {
		return errors.Wrap(err, "drop before create")
	}

	return r.catalog.CreateCollection(ctx,
		event.Namespace.Database,
		event.Namespace.Collection,
		&event.OperationDescription)
}

// handleDrop handles drop events.
func (r *Repl) handleDrop(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[DropEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	err = r.catalog.DropCollection(ctx, event.Namespace.Database, event.Namespace.Collection)

	return err
}

// handleDropDatabase handles drop database events.
func (r *Repl) handleDropDatabase(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[DropDatabaseEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	err = r.catalog.DropDatabase(ctx, event.Namespace.Database)

	return err
}

// handleCreateIndexes handles create indexes events.
func (r *Repl) handleCreateIndexes(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[CreateIndexesEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	err = r.catalog.CreateIndexes(ctx,
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
		err = r.catalog.DropIndex(ctx,
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
		err = r.catalog.ModifyIndex(ctx, db, coll, opts.Index)
		if err != nil {
			lg.Error(err, "Modify index: "+opts.Index.Name)

			return nil
		}

	case opts.CappedSize != nil || opts.CappedMax != nil:
		err = r.catalog.ModifyCappedCollection(ctx, db, coll, opts.CappedSize, opts.CappedMax)
		if err != nil {
			lg.Error(err, "Resize capped collection")

			return nil
		}

	case opts.ViewOn != "":
		if strings.HasPrefix(opts.ViewOn, TimeseriesPrefix) {
			lg.Warn("Timeseries is not supported. skipping")

			return nil
		}

		err = r.catalog.ModifyView(ctx, db, coll, opts.ViewOn, opts.Pipeline)
		if err != nil {
			lg.Error(err, "Modify view")

			return nil
		}

	case opts.ExpireAfterSeconds != nil:
		lg.Warn("Collection TTL modification is not supported")

	case opts.ChangeStreamPreAndPostImages != nil:
		lg.Warn("changeStreamPreAndPostImages is not supported")

	case opts.Validator != nil || opts.ValidatorLevel != nil || opts.ValidatorAction != nil:
		lg.Warn("validator, validatorLevel, and validatorAction are not supported")

	case opts.Unknown == nil:
		lg.Debug("empty modify event")

	default:
		lg.Warn("unknown modify options")
	}

	return nil
}

// handleRename handles rename events.
func (r *Repl) handleRename(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[RenameEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	err = r.catalog.Rename(ctx,
		event.Namespace.Database,
		event.Namespace.Collection,
		event.OperationDescription.To.Database,
		event.OperationDescription.To.Collection)

	return err //nolint:wrapcheck
}

var insertDocOptions = options.Replace().SetUpsert(true) //nolint:gochecknoglobals

// handleInsert handles insert events.
func (r *Repl) handleInsert(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[InsertEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	_, err = r.target.Database(event.Namespace.Database).
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

	_, err = r.target.Database(event.Namespace.Database).
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

	_, err = r.target.Database(event.Namespace.Database).
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

	_, err = r.target.Database(event.Namespace.Database).
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
