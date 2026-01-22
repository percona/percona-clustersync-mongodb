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
