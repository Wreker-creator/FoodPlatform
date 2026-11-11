package telemetry

import (
	"context"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	httpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "route", "status"},
	)

	httpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Duration of HTTP requests in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "route"},
	)

	httpRequestsInFlight = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "http_requests_in_flight",
			Help: "Current number of HTTP requests being served",
		},
	)

	OutboxUnpublishedCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "outbox_unpublished_events_total",
			Help: "Number of events waiting to be published",
		},
		[]string{"topic"},
	)

	OutboxOldestAge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "outbox_oldest_unpublished_age_seconds",
			Help: "Age of the oldest unpublished event in seconds",
		},
		[]string{"topic"},
	)

	OutboxPublishFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "outbox_publish_failures_total",
			Help: "Total number of failed publish attempts",
		},
		[]string{"topic"},
	)

	EventsProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_events_processed_total",
			Help: "Total number of Kafka events processed by outcome",
		},
		[]string{"topic", "event_type", "status"}, // status: ok, duplicate, dlq, failed
	)

	EventProcessingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kafka_event_processing_duration_seconds",
			Help:    "Time taken to process a Kafka event",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"topic"},
	)

	ConsumerLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumer_lag",
			Help: "Current lag of the Kafka consumer group",
		},
		[]string{"topic", "group"},
	)

	DLQMessagesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_dlq_messages_total",
			Help: "Total messages routed to the Dead Letter Queue",
		},
		[]string{"topic"},
	)

	DBConnections = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "db_connections",
			Help: "Current state of database connections",
		},
		[]string{"state"}, // "idle" or "acquired"
	)

	DBMaxConnections = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "db_connections_max",
			Help: "Maximum allowed database connections in the pool",
		},
	)
)

func PromMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		httpRequestsInFlight.Inc()

		c.Next()

		httpRequestsInFlight.Dec()
		duration := time.Since(start).Seconds()

		route := c.FullPath()
		if route == "" {
			route = "UNKNOWN"
		}

		status := strconv.Itoa(c.Writer.Status())
		method := c.Request.Method

		httpRequestsTotal.WithLabelValues(method, route, status).Inc()
		httpRequestDuration.WithLabelValues(method, route).Observe(duration)
	}
}

func TrackDBPoolStats(ctx context.Context, pool *pgxpool.Pool) {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				stats := pool.Stat()

				DBConnections.WithLabelValues("idle").Set(float64(stats.IdleConns()))
				DBConnections.WithLabelValues("acquired").Set(float64(stats.AcquiredConns()))
				DBMaxConnections.Set(float64(stats.MaxConns()))
			}
		}
	}()
}
