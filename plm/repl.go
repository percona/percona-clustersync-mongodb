package plm

import (
	"context"
	"encoding/hex"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-link-mongodb/config"
	"github.com/percona/percona-link-mongodb/errors"
	"github.com/percona/percona-link-mongodb/log"
	"github.com/percona/percona-link-mongodb/metrics"
	"github.com/percona/percona-link-mongodb/sel"
	"github.com/percona/percona-link-mongodb/topo"
	"github.com/percona/percona-link-mongodb/util"
)

var (
	ErrInvalidateEvent  = errors.New("invalidate")
	ErrOplogHistoryLost = errors.New("oplog history is lost")
)

const advanceTimePseudoEvent = "@tick"

// Repl handles replication from a source MongoDB to a target MongoDB.
type Repl struct {
	source *mongo.Client // Source MongoDB client
	target *mongo.Client // Target MongoDB client

	nsFilter sel.NSFilter // Namespace filter
	catalog  *Catalog     // Catalog for managing collections and indexes

	lastReplicatedOpTime bson.Timestamp

	lock sync.Mutex
	err  error

	eventsProcessed int64

	startTime time.Time
	pauseTime time.Time

	pausing bool
	pauseC  chan struct{}
	doneSig chan struct{}

	bulkWrite      bulkWrite
	bulkToken      bson.Raw
	bulkTS         bson.Timestamp
	lastBulkDoneAt time.Time
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
	EventsProcessed      int64          `bson:"events,omitempty"`
	LastReplicatedOpTime bson.Timestamp `bson:"lastOpTS,omitempty"`
	Error                string         `bson:"error,omitempty"`
	UseClientBulkWrite   bool           `bson:"clientBulk,omitempty"`
}

func (r *Repl) Checkpoint() *replCheckpoint { //nolint:revive
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.startTime.IsZero() && r.err == nil {
		return nil
	}

	cp := &replCheckpoint{
		StartTime:            r.startTime,
		PauseTime:            r.pauseTime,
		EventsProcessed:      r.eventsProcessed,
		LastReplicatedOpTime: r.lastReplicatedOpTime,
	}

	_, ok := r.bulkWrite.(*clientBulkWrite)
	cp.UseClientBulkWrite = ok

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
	r.eventsProcessed = cp.EventsProcessed
	r.lastReplicatedOpTime = cp.LastReplicatedOpTime

	if cp.UseClientBulkWrite {
		r.bulkWrite = newClientBulkWrite(config.BulkOpsSize)
	} else {
		r.bulkWrite = newCollectionBulkWrite(config.BulkOpsSize)
	}

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

func (r *Repl) resetError() {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.err = nil
}

func (r *Repl) Done() <-chan struct{} {
	r.lock.Lock()
	defer r.lock.Unlock()

	return r.doneSig
}

// Start begins the replication process from the specified start timestamp.
func (r *Repl) Start(ctx context.Context, startAt bson.Timestamp) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.err != nil {
		return errors.Wrap(r.err, "cannot start due an existing error")
	}

	if !r.startTime.IsZero() {
		return errors.New("already started")
	}

	serverVersion, err := topo.Version(ctx, r.target)
	if err != nil {
		return errors.Wrap(err, "major version")
	}

	if topo.Support(serverVersion).ClientBulkWrite() && !config.UseCollectionBulkWrite() {
		r.bulkWrite = newClientBulkWrite(config.BulkOpsSize)
	} else {
		r.bulkWrite = newCollectionBulkWrite(config.BulkOpsSize)

		log.New("repl").Debug("Use collection-level bulk write")
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

	r.doPause()

	return nil
}

func (r *Repl) doPause() {
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

		log.New("repl").
			With(log.OpTime(optime.T, optime.I)).
			Info("Change Replication paused")
	}()
}

func (r *Repl) setFailed(err error, msg string) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.err = err

	log.New("repl").Error(err, msg)

	r.doPause()
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

	if r.lastReplicatedOpTime.IsZero() {
		return errors.New("missing optime")
	}

	r.pauseTime = time.Time{}
	r.doneSig = make(chan struct{})

	go r.run(options.ChangeStream().SetStartAtOperationTime(&r.lastReplicatedOpTime))

	log.New("repl").With(log.OpTime(r.lastReplicatedOpTime.T, r.lastReplicatedOpTime.I)).
		Info("Change Replication resumed")

	return nil
}

func (r *Repl) watchChangeEvents(
	ctx context.Context,
	streamOptions *options.ChangeStreamOptionsBuilder,
	changeC chan<- *ChangeEvent,
) error {
	cur, err := r.source.Watch(ctx, mongo.Pipeline{},
		streamOptions.SetShowExpandedEvents(true).
			SetBatchSize(config.ChangeStreamBatchSize).
			SetMaxAwaitTime(config.ChangeStreamAwaitTime))
	if err != nil {
		return errors.Wrap(err, "open")
	}

	defer func() {
		err := util.CtxWithTimeout(context.Background(), config.CloseCursorTimeout, cur.Close)
		if err != nil {
			log.New("repl:watch").Error(err, "Close change stream cursor")
		}
	}()

	// txnOps stores transaction operations during processing.
	// This buffer is reused to minimize memory allocations.
	var txnOps []*ChangeEvent

	for {
		lastEventTS := bson.Timestamp{}
		sourceTS, err := topo.AdvanceClusterTime(ctx, r.source)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}

			log.New("watch").Error(err, "Unable to advance the source cluster time")
		}

		for cur.TryNext(ctx) {
			change := &ChangeEvent{}
			err := parseChangeEvent(cur.Current, change)
			if err != nil {
				return err
			}

			if !change.IsTransaction() {
				changeC <- change
				lastEventTS = change.ClusterTime

				continue
			}

			txn0 := change // the first transaction operation
			for txn0 != nil {
				for cur.TryNext(ctx) {
					change = &ChangeEvent{}
					err := parseChangeEvent(cur.Current, change)
					if err != nil {
						return err
					}

					if txn0.IsSameTransaction(&change.EventHeader) {
						txnOps = append(txnOps, change)

						continue
					}

					// send the entire transaction for replication
					changeC <- txn0
					for _, txn := range txnOps {
						changeC <- txn
					}
					clear(txnOps)
					txnOps = txnOps[:0]

					if !change.IsTransaction() {
						changeC <- change
						txn0 = nil // no more transaction

						break // return to non-transactional processing
					}

					txn0 = change // process the new transaction
				}

				if err := cur.Err(); err != nil || cur.ID() == 0 {
					return errors.Wrap(err, "cursor")
				}

				if txn0 == nil {
					continue
				}

				// no event available. the entire transaction is received
				changeC <- txn0
				for _, txn := range txnOps {
					changeC <- txn
				}
				clear(txnOps)
				txnOps = txnOps[:0]
				txn0 = nil // return to non-transactional processing
			}
		}

		if err := cur.Err(); err != nil || cur.ID() == 0 {
			return errors.Wrap(err, "cursor")
		}

		// no event available yet. progress plm time
		if sourceTS.After(lastEventTS) {
			changeC <- &ChangeEvent{
				EventHeader: EventHeader{
					OperationType: advanceTimePseudoEvent,
					ClusterTime:   sourceTS,
				},
			}
		}
	}
}

func (r *Repl) run(opts *options.ChangeStreamOptionsBuilder) {
	defer close(r.doneSig)

	ctx := context.Background()
	changeC := make(chan *ChangeEvent, config.ReplQueueSize)

	go func() {
		defer close(changeC)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func() {
			<-r.pauseC
			cancel()
		}()

		err := r.watchChangeEvents(ctx, opts, changeC)
		if err != nil && !errors.Is(err, context.Canceled) {
			if topo.IsChangeStreamHistoryLost(err) || topo.IsCappedPositionLost(err) {
				err = ErrOplogHistoryLost
			}

			r.setFailed(err, "Watch change stream")
		}
	}()

	uuidMap := r.catalog.UUIDMap()
	r.lastBulkDoneAt = time.Now()

	lg := log.New("repl")

	for change := range changeC {
		if time.Since(r.lastBulkDoneAt) >= config.BulkOpsInterval && !r.bulkWrite.Empty() {
			if !r.doBulkOps(ctx) {
				return
			}
		}

		if change.OperationType == advanceTimePseudoEvent {
			lg.With(log.OpTime(change.ClusterTime.T, change.ClusterTime.I)).Trace("tick")

			r.lock.Lock()
			r.lastReplicatedOpTime = change.ClusterTime
			r.lock.Unlock()

			continue
		}

		if change.Namespace.Database == config.PLMDatabase {
			if r.bulkWrite.Empty() {
				r.lock.Lock()
				r.lastReplicatedOpTime = change.ClusterTime
				r.eventsProcessed++
				r.lock.Unlock()

				metrics.AddEventsProcessed(1)
			}

			continue
		}

		if !r.nsFilter(change.Namespace.Database, change.Namespace.Collection) {
			if r.bulkWrite.Empty() {
				r.lock.Lock()
				r.lastReplicatedOpTime = change.ClusterTime
				r.eventsProcessed++
				r.lock.Unlock()

				metrics.AddEventsProcessed(1)
			}

			continue
		}

		switch change.OperationType { //nolint:exhaustive
		case Insert:
			event := change.Event.(InsertEvent) //nolint:forcetypeassert
			ns := findNamespaceByUUID(uuidMap, change)
			r.bulkWrite.Insert(ns, &event)
			r.bulkToken = change.ID
			r.bulkTS = change.ClusterTime

		case Update:
			event := change.Event.(UpdateEvent) //nolint:forcetypeassert
			ns := findNamespaceByUUID(uuidMap, change)
			r.bulkWrite.Update(ns, &event)
			r.bulkToken = change.ID
			r.bulkTS = change.ClusterTime

		case Delete:
			event := change.Event.(DeleteEvent) //nolint:forcetypeassert
			ns := findNamespaceByUUID(uuidMap, change)
			r.bulkWrite.Delete(ns, &event)
			r.bulkToken = change.ID
			r.bulkTS = change.ClusterTime

		case Replace:
			event := change.Event.(ReplaceEvent) //nolint:forcetypeassert
			ns := findNamespaceByUUID(uuidMap, change)
			r.bulkWrite.Replace(ns, &event)
			r.bulkToken = change.ID
			r.bulkTS = change.ClusterTime

		default:
			if !r.bulkWrite.Empty() {
				if !r.doBulkOps(ctx) {
					return
				}
			}

			err := r.applyDDLChange(ctx, change)
			if err != nil {
				r.setFailed(err, "Apply change")

				return
			}

			r.lock.Lock()
			r.lastReplicatedOpTime = change.ClusterTime
			r.eventsProcessed++
			r.lock.Unlock()

			metrics.AddEventsProcessed(1)

			switch change.OperationType { //nolint:exhaustive
			case Create, Rename, Drop, DropDatabase:
				uuidMap = r.catalog.UUIDMap()
			}
		}

		if r.bulkWrite.Full() {
			if !r.doBulkOps(ctx) {
				return
			}
		}
	}

	if !r.bulkWrite.Empty() {
		r.doBulkOps(ctx) //nolint:errcheck
	}
}

//go:inline
func findNamespaceByUUID(uuidMap UUIDMap, change *ChangeEvent) Namespace {
	if change.CollectionUUID == nil {
		return change.Namespace
	}

	ns, ok := uuidMap[hex.EncodeToString(change.CollectionUUID.Data)]
	if !ok {
		return change.Namespace
	}

	return ns
}

func (r *Repl) doBulkOps(ctx context.Context) bool {
	size, err := r.bulkWrite.Do(ctx, r.target)
	if err != nil {
		r.setFailed(err, "Flush bulk ops")

		return false
	}

	if size == 0 {
		return true
	}

	r.lock.Lock()
	r.lastReplicatedOpTime = r.bulkTS
	r.eventsProcessed += int64(size)
	r.lock.Unlock()

	metrics.AddEventsProcessed(size)

	log.New("bulk:write").
		With(log.Int64("size", int64(size)), log.Elapsed(time.Since(r.lastBulkDoneAt))).
		Debug("BulkOps applied")

	r.lastBulkDoneAt = time.Now()

	return true
}

// applyDDLChange applies a schema change to the target MongoDB.
func (r *Repl) applyDDLChange(ctx context.Context, change *ChangeEvent) error {
	lg := loggerForEvent(change)
	ctx = lg.WithContext(ctx)

	var err error

	switch change.OperationType { //nolint:exhaustive
	case Create:
		event := change.Event.(CreateEvent) //nolint:forcetypeassert
		if event.IsTimeseries() {
			lg.Warn("Timeseries is not supported. skipping")

			return nil
		}

		err = r.catalog.DropCollection(ctx,
			change.Namespace.Database,
			change.Namespace.Collection)
		if err != nil {
			err = errors.Wrap(err, "drop before create")

			break
		}

		err = r.catalog.CreateCollection(ctx,
			change.Namespace.Database,
			change.Namespace.Collection,
			&event.OperationDescription)
		if err != nil {
			err = errors.Wrap(err, "create")

			break
		}

		r.catalog.SetCollectionUUID(ctx,
			change.Namespace.Database,
			change.Namespace.Collection,
			change.CollectionUUID)

		lg.Infof("Collection %q has been created", change.Namespace)

	case Drop:
		err = r.catalog.DropCollection(ctx,
			change.Namespace.Database,
			change.Namespace.Collection)
		if err != nil {
			break
		}

		lg.Infof("Collection %q has been dropped", change.Namespace)

	case DropDatabase:
		err = r.catalog.DropDatabase(ctx, change.Namespace.Database)
		if err != nil {
			break
		}

		lg.Infof("Database %q has been dropped", change.Namespace)

	case CreateIndexes:
		// TODO: check the indexes status
		event := change.Event.(CreateIndexesEvent) //nolint:forcetypeassert
		err = r.catalog.CreateIndexes(ctx,
			change.Namespace.Database,
			change.Namespace.Collection,
			event.OperationDescription.Indexes)

	case DropIndexes:
		event := change.Event.(DropIndexesEvent) //nolint:forcetypeassert
		for _, index := range event.OperationDescription.Indexes {
			err = r.catalog.DropIndex(ctx,
				change.Namespace.Database,
				change.Namespace.Collection,
				index.Name)
			if err != nil {
				lg.Error(err, "Drop index "+index.Name)
			}
		}

	case Modify:
		event := change.Event.(ModifyEvent) //nolint:forcetypeassert
		r.doModify(ctx, change.Namespace, &event)

	case Rename:
		event := change.Event.(RenameEvent) //nolint:forcetypeassert
		err = r.catalog.Rename(ctx,
			change.Namespace.Database,
			change.Namespace.Collection,
			event.OperationDescription.To.Database,
			event.OperationDescription.To.Collection)
		if err != nil {
			break
		}

		lg.Infof("Collection %q has been renamed to %q",
			change.Namespace, event.OperationDescription.To)

	case Invalidate:
		lg.Error(ErrInvalidateEvent, "")

		return ErrInvalidateEvent

	case ShardCollection:
		fallthrough
	case ReshardCollection:
		fallthrough
	case RefineCollectionShardKey:
		fallthrough

	default:
		lg.Warn("Unsupported type: " + string(change.OperationType))

		return nil
	}

	if err != nil {
		return errors.Wrap(err, string(change.OperationType))
	}

	return nil
}

func (r *Repl) doModify(ctx context.Context, ns Namespace, event *ModifyEvent) {
	opts := event.OperationDescription

	if len(opts.Unknown) != 0 {
		log.Ctx(ctx).Warn("Unknown modify options")
	}

	if opts.ChangeStreamPreAndPostImages != nil {
		err := r.catalog.ModifyChangeStreamPreAndPostImages(ctx,
			ns.Database, ns.Collection, opts.ChangeStreamPreAndPostImages.Enabled)
		if err != nil {
			log.Ctx(ctx).Error(err, "Modify changeStreamPreAndPostImages")
		}
	}

	if opts.Validator != nil || opts.ValidationLevel != nil || opts.ValidationAction != nil {
		err := r.catalog.ModifyValidation(ctx,
			ns.Database, ns.Collection, opts.Validator, opts.ValidationLevel, opts.ValidationAction)
		if err != nil {
			log.Ctx(ctx).Error(err, "Modify validation")
		}
	}

	switch {
	case opts.Index != nil:
		err := r.catalog.ModifyIndex(ctx, ns.Database, ns.Collection, opts.Index)
		if err != nil {
			log.Ctx(ctx).Error(err, "Modify index: "+opts.Index.Name)

			return
		}

	case opts.CappedSize != nil || opts.CappedMax != nil:
		err := r.catalog.ModifyCappedCollection(ctx,
			ns.Database, ns.Collection, opts.CappedSize, opts.CappedMax)
		if err != nil {
			log.Ctx(ctx).Error(err, "Resize capped collection")

			return
		}

	case opts.ViewOn != "":
		if strings.HasPrefix(opts.ViewOn, TimeseriesPrefix) {
			log.Ctx(ctx).Warn("Timeseries is not supported. skipping")

			return
		}

		err := r.catalog.ModifyView(ctx, ns.Database, ns.Collection, opts.ViewOn, opts.Pipeline)
		if err != nil {
			log.Ctx(ctx).Error(err, "Modify view")

			return
		}

	case opts.ExpireAfterSeconds != nil:
		log.Ctx(ctx).Warn("Collection TTL modification is not supported")

	case opts.ChangeStreamPreAndPostImages != nil:
		log.Ctx(ctx).Warn("changeStreamPreAndPostImages is not supported")
	}
}

func loggerForEvent(change *ChangeEvent) log.Logger {
	return log.New("repl").With(
		log.OpTime(change.ClusterTime.T, change.ClusterTime.I),
		log.Op(string(change.OperationType)),
		log.NS(change.Namespace.Database, change.Namespace.Collection))
}
