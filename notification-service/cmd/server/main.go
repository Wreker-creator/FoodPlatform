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
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	router := gin.Default()

	connString := os.Getenv("DATABASE_URL")
	if connString == "" {
		log.Fatal("DATABASE_URL environment variable is not set")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}

	defer pool.Close()

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

	// router.Run(":8084")

}
