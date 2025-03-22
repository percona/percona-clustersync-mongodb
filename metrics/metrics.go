package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

const metricNamespace = "percona_mongolink"

// Counters.
var (
	//nolint:gochecknoglobals
	eventsProcessedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name:      "events_processed_total",
		Help:      "Total number of events processed.",
		Namespace: metricNamespace,
	})

	//nolint:gochecknoglobals
	copiedTotalSizeBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name:      "copied_total_size_bytes_total",
		Help:      "Total size of the data copied in bytes.",
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
)

// Init initializes and registers the metrics.
func Init(reg prometheus.Registerer) {
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	reg.MustRegister(
		eventsProcessedTotal,
		copiedTotalSizeBytesTotal,
		lagTimeSeconds,
		intialSyncLagTimeSeconds,
		estimatedTotalSizeBytes)
}

// AddEventsProcessed increments the total number of events processed counter.
func AddEventsProcessed(v int) {
	eventsProcessedTotal.Add(float64(v))
}

// AddCopiedSize increments the total size of the data copied counter.
func AddCopiedSize(v int) {
	copiedTotalSizeBytesTotal.Add(float64(v))
}

// SetLagTimeSeconds sets the lag time in seconds gauge.
func SetLagTimeSeconds(v uint32) {
	lagTimeSeconds.Set(float64(v))
}

// SetInitialSyncLagTimeSeconds sets the initial sync lag time in seconds gauge.
func SetInitialSyncLagTimeSeconds(v int64) {
	intialSyncLagTimeSeconds.Set(float64(v))
}

// SetEstimatedTotalSize sets the estimated total size of the data to be replicated in bytes gauge.
func SetEstimatedTotalSize(v int64) {
	estimatedTotalSizeBytes.Set(float64(v))
}
