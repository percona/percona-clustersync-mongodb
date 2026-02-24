package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

const metricNamespace = "percona_clustersync_mongodb"

// Counters.
var (
	//nolint:gochecknoglobals
	eventsReadTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name:      "events_read_total",
		Help:      "Total number of events read from the source.",
		Namespace: metricNamespace,
	})

	//nolint:gochecknoglobals
	eventsAppliedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name:      "events_applied_total",
		Help:      "Total number of events applied.",
		Namespace: metricNamespace,
	})

	//nolint:gochecknoglobals
	copyReadSizeBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name:      "copy_read_size_bytes_total",
		Help:      "Total size of the read data in bytes.",
		Namespace: metricNamespace,
	})

	//nolint:gochecknoglobals
	copyInsertSizeBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name:      "copy_insert_size_bytes_total",
		Help:      "Total size of the inserted data in bytes.",
		Namespace: metricNamespace,
	})
)

// Replication pipeline metrics.
var (
	//nolint:gochecknoglobals
	replEventQueueSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:      "repl_event_queue_size",
		Help:      "Number of events in the reader-to-dispatcher queue.",
		Namespace: metricNamespace,
	})

	//nolint:gochecknoglobals
	replWorkerEventQueueSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:      "repl_worker_event_queue_size",
		Help:      "Number of events in a worker's inbound queue.",
		Namespace: metricNamespace,
	}, []string{"worker"})

	//nolint:gochecknoglobals
	replWorkerEventsAppliedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:      "repl_worker_events_applied_total",
		Help:      "Total events applied by each worker.",
		Namespace: metricNamespace,
	}, []string{"worker"})

	//nolint:gochecknoglobals
	replWorkerFlushBatchSize = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:      "repl_worker_flush_batch_size",
		Help:      "Number of operations per bulk write flush.",
		Namespace: metricNamespace,
		Buckets:   []float64{10, 50, 100, 250, 500, 1000, 2500, 5000},
	}, []string{"worker"})

	//nolint:gochecknoglobals
	replWorkerFlushDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:      "repl_worker_flush_duration_seconds",
		Help:      "Duration of bulk write flushes in seconds.",
		Namespace: metricNamespace,
		Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"worker"})
)

// Gauges.
var (
	//nolint:gochecknoglobals
	lagTimeSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:      "lag_time_seconds",
		Help:      "Lag time in logical seconds between source and target clusters.",
		Namespace: metricNamespace,
	})

	//nolint:gochecknoglobals
	intialSyncLagTimeSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:      "initial_sync_lag_time_seconds",
		Help:      "Lag time during the initial sync in seconds.",
		Namespace: metricNamespace,
	})

	//nolint:gochecknoglobals
	estimatedTotalSizeBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:      "estimated_total_size_bytes",
		Help:      "Estimated total size of the data to be replicated in bytes.",
		Namespace: metricNamespace,
	})

	//nolint:gochecknoglobals
	copyReadDocumentTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name:      "copy_read_document_total",
		Help:      "Total count of the read documents.",
		Namespace: metricNamespace,
	})

	//nolint:gochecknoglobals
	copyInsertDocumentTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name:      "copy_insert_document_total",
		Help:      "Total count of the inserted documents.",
		Namespace: metricNamespace,
	})

	//nolint:gochecknoglobals
	copyReadBatchDurationSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:      "copy_read_batch_duration_seconds",
		Help:      "Read batch duration time in seconds.",
		Namespace: metricNamespace,
	})

	//nolint:gochecknoglobals
	copyInsertBatchDurationSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:      "copy_insert_batch_duration_seconds",
		Help:      "Insert batch duration time in seconds.",
		Namespace: metricNamespace,
	})
)

// Init initializes and registers the metrics.
func Init(reg prometheus.Registerer) {
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{
		Namespace: metricNamespace,
	}))

	reg.MustRegister(
		estimatedTotalSizeBytes,
		copyReadDocumentTotal,
		copyInsertDocumentTotal,
		copyReadSizeBytesTotal,
		copyInsertSizeBytesTotal,
		copyReadBatchDurationSeconds,
		copyInsertBatchDurationSeconds,

		eventsReadTotal,
		eventsAppliedTotal,
		lagTimeSeconds,
		intialSyncLagTimeSeconds,

		replEventQueueSize,
		replWorkerEventQueueSize,
		replWorkerEventsAppliedTotal,
		replWorkerFlushBatchSize,
		replWorkerFlushDurationSeconds,
	)
}

// SetEstimatedTotalSizeBytes sets the estimated total size of the data to be replicated in bytes
// gauge.
func SetEstimatedTotalSizeBytes(v uint64) {
	estimatedTotalSizeBytes.Set(float64(v))
}

// AddCopyReadDocumentCount increments the total count of the read documents.
func AddCopyReadDocumentCount(v int) {
	copyReadDocumentTotal.Add(float64(v))
}

// AddCopyInsertDocumentCount increments the total count of the inserted documents.
func AddCopyInsertDocumentCount(v int) {
	copyInsertDocumentTotal.Add(float64(v))
}

// AddCopyReadSize increments the total size of the read data counter.
func AddCopyReadSize(v uint64) {
	copyReadSizeBytesTotal.Add(float64(v))
}

// AddCopyInsertSize increments the total size of the inserter data counter.
func AddCopyInsertSize(v uint64) {
	copyInsertSizeBytesTotal.Add(float64(v))
}

// SetCopyReadBatchDurationSeconds sets the duration in seconds for the copy read batch operation.
func SetCopyReadBatchDurationSeconds(dur time.Duration) {
	copyReadBatchDurationSeconds.Set(float64(dur.Seconds()))
}

// SetCopyInsertBatchDurationSeconds sets the duration in seconds for the copy insert batch
// operation.
func SetCopyInsertBatchDurationSeconds(dur time.Duration) {
	copyInsertBatchDurationSeconds.Set(float64(dur.Seconds()))
}

// IncEventsRead increments the total number of events read counter.
func IncEventsRead() {
	eventsReadTotal.Inc()
}

// AddEventsApplied increments the total number of events applied counter.
func AddEventsApplied(v int) {
	eventsAppliedTotal.Add(float64(v))
}

// SetLagTimeSeconds sets the lag time in seconds gauge.
func SetLagTimeSeconds(v uint32) {
	lagTimeSeconds.Set(float64(v))
}

// SetInitialSyncLagTimeSeconds sets the initial sync lag time in seconds gauge.
func SetInitialSyncLagTimeSeconds(v uint32) {
	intialSyncLagTimeSeconds.Set(float64(v))
}

// SetReplEventQueueSize sets the current size of the reader-to-dispatcher event queue.
func SetReplEventQueueSize(v int) {
	replEventQueueSize.Set(float64(v))
}

// SetReplWorkerEventQueueSize sets the current size of a worker's inbound event queue.
func SetReplWorkerEventQueueSize(worker string, v int) {
	replWorkerEventQueueSize.WithLabelValues(worker).Set(float64(v))
}

// AddReplWorkerEventsApplied increments the per-worker events applied counter.
func AddReplWorkerEventsApplied(worker string, v int) {
	replWorkerEventsAppliedTotal.WithLabelValues(worker).Add(float64(v))
}

// ObserveReplWorkerFlushBatchSize records the number of operations in a worker's bulk write flush.
func ObserveReplWorkerFlushBatchSize(worker string, v int) {
	replWorkerFlushBatchSize.WithLabelValues(worker).Observe(float64(v))
}

// ObserveReplWorkerFlushDuration records the duration of a worker's bulk write flush.
func ObserveReplWorkerFlushDuration(worker string, d time.Duration) {
	replWorkerFlushDurationSeconds.WithLabelValues(worker).Observe(d.Seconds())
}
