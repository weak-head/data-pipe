package metrics

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// TODO: comments
	dataFrameDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "dataframe_durations_seconds",
			Help:       "Data frame processing duration distributions.",
			Objectives: map[float64]float64{},
		},
		[]string{"engine", "processing_kind"},
	)

	// TODO: comments
	dataFrameDurationsHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "dataframe_durations_histogram_seconds",
			Help: "Data frame processing duration distributions.",
			// Buckets: prometheus.LinearBuckets(normMean-5*normDomain, .5*normDomain, 20),
		},
		[]string{"engine", "processing_kind"},
	)

	// TODO: comments
	processingDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "processing_durations_seconds",
			Help:       "Processing duration distributions.",
			Objectives: map[float64]float64{},
		},
		[]string{"engine", "processing_kind"},
	)

	// TODO: comments
	processingDurationsHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "processing_durations_histogram_seconds",
			Help: "Processing duration distributions.",
			// Buckets: prometheus.LinearBuckets(normMean-5*normDomain, .5*normDomain, 20),
		},
		[]string{"engine", "processing_kind"},
	)

	// TODO: comments
	dataFramesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataframes_total",
			Help: "Number of processed data frames.",
		},
		[]string{"engine", "processing_kind"},
	)

	// TODO: comments
	pipelineFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pipeline_errors_total",
			Help: "Number of pipeline errors.",
		},
		[]string{"engine", "failure"},
	)
)

// prometheusServer
type prometheusServer struct {
	server   *http.Server
	registry *prometheus.Registry
	conf     Config
}

// NewPrometheusServer
func NewPrometheusServer(conf Config) (*prometheusServer, error) {
	p := &prometheusServer{
		registry: prometheus.NewRegistry(),
		conf:     conf,
	}

	for _, c := range []prometheus.Collector{
		dataFrameDuration,
		dataFrameDurationsHistogram,
		processingDuration,
		processingDurationsHistogram,
		dataFramesTotal,
		pipelineFailures,
		collectors.NewBuildInfoCollector(),
	} {
		if err := p.registry.Register(c); err != nil {
			return nil, err
		}
	}

	p.server = &http.Server{
		Addr: p.conf.Addr,
		Handler: promhttp.HandlerFor(
			p.registry,
			promhttp.HandlerOpts{EnableOpenMetrics: true},
		),
	}

	return p, nil
}

// Serve
func (p *prometheusServer) Serve() error {
	if err := p.server.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Stop
func (p *prometheusServer) Stop(ctx context.Context) error {
	return p.server.Shutdown(ctx)
}

// reporter
type reporter struct {
	info ServiceInfo
}

// NewReporter
func NewReporter(info ServiceInfo) (*reporter, error) {
	return &reporter{info: info}, nil
}

// DataFrameProcessed
func (r *reporter) DataFrameProcessed(processingKind string, milliseconds float64) {
	dataFrameDuration.WithLabelValues(r.info.Engine, processingKind).Observe(milliseconds)
	dataFrameDurationsHistogram.WithLabelValues(r.info.Engine, processingKind).Observe(milliseconds)
	dataFramesTotal.WithLabelValues(r.info.Engine, processingKind).Inc()
}

// ProcessingFinished
func (r *reporter) ProcessingFinished(processingKind string, milliseconds float64) {
	processingDuration.WithLabelValues(r.info.Engine, processingKind).Observe(milliseconds)
	processingDurationsHistogram.WithLabelValues(r.info.Engine, processingKind).Observe(milliseconds)
}

// PipelineFailed
func (r *reporter) PipelineFailed(failure string) {
	pipelineFailures.WithLabelValues(r.info.Engine, failure).Inc()
}
