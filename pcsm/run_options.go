package pcsm

import (
	"time"

	"github.com/percona/percona-clustersync-mongodb/pcsm/clone"
	"github.com/percona/percona-clustersync-mongodb/pcsm/repl"
)

// runOptions is what a run was started with, persisted in the checkpoint so a
// restore keeps it: pauseOnInitialSync and the clone and repl tuning as
// requested. Zero values stay auto, so the instance that restores the run
// resolves them for its own host instead of inheriting another host's NumCPU.
// Write concern and namespace filters are stored next to it.
type runOptions struct {
	PauseOnInitialSync bool            `bson:"pauseOnInitialSync,omitempty"`
	Clone              cloneRunOptions `bson:"clone"`
	Repl               replRunOptions  `bson:"repl"`
}

type cloneRunOptions struct {
	Parallelism        int   `bson:"parallelism,omitempty"`
	ReadWorkers        int   `bson:"readWorkers,omitempty"`
	InsertWorkers      int   `bson:"insertWorkers,omitempty"`
	SegmentSizeBytes   int64 `bson:"segmentSizeBytes,omitempty"`
	ReadBatchSizeBytes int32 `bson:"readBatchSizeBytes,omitempty"`
	SkipPresplit       bool  `bson:"skipPresplit,omitempty"`
}

type replRunOptions struct {
	UseCollectionBulkWrite bool          `bson:"useCollectionBulkWrite,omitempty"`
	NumWorkers             int           `bson:"numWorkers,omitempty"`
	ChangeStreamBatchSize  int           `bson:"changeStreamBatchSize,omitempty"`
	EventQueueSize         int           `bson:"eventQueueSize,omitempty"`
	WorkerQueueSize        int           `bson:"workerQueueSize,omitempty"`
	BulkOpsSize            int           `bson:"bulkOpsSize,omitempty"`
	WorkerFlushInterval    time.Duration `bson:"workerFlushInterval,omitempty"`
	WorkerBulkQueueSize    int           `bson:"workerBulkQueueSize,omitempty"`
}

// newRunOptions captures the requested options. The caller passes them before
// the components resolve their defaults in place.
func newRunOptions(pauseOnInitialSync bool, c *clone.Options, r *repl.Options) *runOptions {
	return &runOptions{
		PauseOnInitialSync: pauseOnInitialSync,
		Clone: cloneRunOptions{
			Parallelism:        c.Parallelism,
			ReadWorkers:        c.ReadWorkers,
			InsertWorkers:      c.InsertWorkers,
			SegmentSizeBytes:   c.SegmentSizeBytes,
			ReadBatchSizeBytes: c.ReadBatchSizeBytes,
			SkipPresplit:       c.SkipPresplit,
		},
		Repl: replRunOptions{
			UseCollectionBulkWrite: r.UseCollectionBulkWrite,
			NumWorkers:             r.NumWorkers,
			ChangeStreamBatchSize:  r.ChangeStreamBatchSize,
			EventQueueSize:         r.EventQueueSize,
			WorkerQueueSize:        r.WorkerQueueSize,
			BulkOpsSize:            r.BulkOpsSize,
			WorkerFlushInterval:    r.WorkerFlushInterval,
			WorkerBulkQueueSize:    r.WorkerBulkQueueSize,
		},
	}
}

// cloneOptions returns the requested clone tuning, without a write concern.
func (o *runOptions) cloneOptions() clone.Options {
	return clone.Options{
		Parallelism:        o.Clone.Parallelism,
		ReadWorkers:        o.Clone.ReadWorkers,
		InsertWorkers:      o.Clone.InsertWorkers,
		SegmentSizeBytes:   o.Clone.SegmentSizeBytes,
		ReadBatchSizeBytes: o.Clone.ReadBatchSizeBytes,
		SkipPresplit:       o.Clone.SkipPresplit,
	}
}

// replOptions returns the requested repl tuning, without a write concern.
func (o *runOptions) replOptions() repl.Options {
	return repl.Options{
		UseCollectionBulkWrite: o.Repl.UseCollectionBulkWrite,
		NumWorkers:             o.Repl.NumWorkers,
		ChangeStreamBatchSize:  o.Repl.ChangeStreamBatchSize,
		EventQueueSize:         o.Repl.EventQueueSize,
		WorkerQueueSize:        o.Repl.WorkerQueueSize,
		BulkOpsSize:            o.Repl.BulkOpsSize,
		WorkerFlushInterval:    o.Repl.WorkerFlushInterval,
		WorkerBulkQueueSize:    o.Repl.WorkerBulkQueueSize,
	}
}
