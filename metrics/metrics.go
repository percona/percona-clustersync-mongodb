package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

const metricNamespace = "percona-mongolink"

type M struct {
	lagTime            prometheus.Gauge
	initialSyncLagTime prometheus.Gauge

	eventsProcessed prometheus.Counter

	estimatedTotalSize prometheus.Gauge
	copiedSize         prometheus.Counter
}

// New creates new Metrics.
func New(reg prometheus.Registerer) *M {
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := &M{}

	m.lagTime = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:      "lag_time",
		Help:      "Current lag time in logical seconds between source and target clusters.",
		Namespace: metricNamespace,
	})
	reg.MustRegister(m.lagTime)

	m.initialSyncLagTime = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:      "initial_sync_lag_time",
		Help:      "Lag time during the initial sync.",
		Namespace: metricNamespace,
	})
	reg.MustRegister(m.initialSyncLagTime)

	m.eventsProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Name:      "events_processed_total",
		Help:      "Total number of events processed.",
		Namespace: metricNamespace,
	})
	reg.MustRegister(m.eventsProcessed)

	m.estimatedTotalSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:      "estimated_total_size",
		Help:      "Estimated total size of the data to be replicated in bytes.",
		Namespace: metricNamespace,
	})
	reg.MustRegister(m.estimatedTotalSize)

	m.copiedSize = prometheus.NewCounter(prometheus.CounterOpts{
		Name:      "copied_size_total",
		Help:      "Total size of the data copied in bytes.",
		Namespace: metricNamespace,
	})
	reg.MustRegister(m.copiedSize)

	return m
}

// CollectLagTime sets the current lag time in logical seconds between source and target clusters.
func (m *M) CollectLagTime(v float64) {
	m.lagTime.Set(v)
}

// CollectInitialSyncLagTime sets the lag time during the initial sync.
func (m *M) CollectInitialSyncLagTime(v float64) {
	m.initialSyncLagTime.Set(v)
}

// CollectEventsProcessed increments the total number of events processed.
func (m *M) CollectEventsProcessed(v float64) {
	m.eventsProcessed.Add(v)
}

// CollectEstimatedTotalSize sets the estimated total size of the data to be replicated in bytes.
func (m *M) CollectEstimatedTotalSize(v float64) {
	m.estimatedTotalSize.Set(v)
}

// CollectCopiedSize increments the total size of the data copied in bytes.
func (m *M) CollectCopiedSize(v float64) {
	m.copiedSize.Add(v)
}
