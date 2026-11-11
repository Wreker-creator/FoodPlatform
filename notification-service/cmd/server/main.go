package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"notification-service/internal/handler"
	"notification-service/internal/kafka"
	"notification-service/internal/store"
	"notification-service/internal/telemetry"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	kafkago "github.com/segmentio/kafka-go"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// --- CHANGED: Swapped gin.Default() for gin.New() and gin.Recovery() ---
	// WHY: gin.Default() automatically attaches a plain-text logger middleware that outputs
	// unformatted logs between your clean JSON lines, breaking structured log parsing.
	// gin.New() gives us a blank router without the noisy logger, while gin.Recovery()
	// ensures that if a handler panics, it returns a 500 error instead of crashing the container.

	connString := os.Getenv("DATABASE_URL")
	if connString == "" {
		log.Fatal("DATABASE_URL environment variable is not set")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		slog.Error("Failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	// --- NEW: Start tracking DB pool stats ---
	// WHY: Even though this service acts primarily as a consumer, it still executes
	// DB transactions (Idempotency checks). If Kafka floods this service with messages,
	// we need Prometheus to monitor if we are exhausting our max Postgres connections.
	telemetry.TrackDBPoolStats(ctx, pool)

	dlqWriter := &kafkago.Writer{
		// kafka doesn't use http, kafka.TCP tells the net package to open a socket at this port
		Addr: kafkago.TCP("kafka:9094"),
		// this is the shared topic where all broken messages are going to be read from
		Topic: "dead-letter-events",
		// in case of multiple paritions, leastBytes ensures messages are distributed evenly based on payload size
		Balancer: &kafkago.LeastBytes{},
	}

	defer func() {
		if err := dlqWriter.Close(); err != nil {
			slog.Error("Failed to close DLQ writer", "error", err)
		}
	}()

	queries := store.New(pool)
	notificationsHandler := handler.NewNotificationsHandler(queries)

	consumer := kafka.NewConsumer("kafka:9094", "order-events", "notification-order-group", queries, dlqWriter)
	go consumer.Start(ctx)

	router := gin.New()
	router.Use(gin.Recovery())

	router.Use(telemetry.PromMiddleware())
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	router.GET("/notifications/:id", notificationsHandler.GetNotificationsById)

	srv := &http.Server{
		Addr:    ":8084",
		Handler: router,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("failed to listen: %v\n", err)
		}
	}()

	<-ctx.Done()
	slog.Info("Shutdown signal received, draining...")

	shutdotwnCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdotwnCtx); err != nil {
		slog.Error("Server forced to shutdown", "error:", err)
	}

	slog.Info("All Background workers and HTTP server stooped cleanly")
}
