package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"payment-service/internal/handler"
	"payment-service/internal/kafka"
	"payment-service/internal/store"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	connString := os.Getenv("DATABASE_URL")
	if connString == "" {
		log.Fatal("DATABASE_URL environment variable is not set")
	}

	// 1. Trap SIGINT and SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	queries := store.New(pool)
	paymentHandler := handler.NewPaymentHandler(queries)

	// 2. Start Kafka background workers with cancellable ctx
	consumer := kafka.NewConsumer("kafka:9094", "inventory-events", "payment-inventory-group", queries, pool)
	publisher := kafka.NewPublisher(queries, "payment-events", "kafka:9094")

	go consumer.Start(ctx)
	go publisher.Start(ctx)

	// 3. Register routes on router FIRST
	router := gin.Default()
	router.GET("/payments/:id", paymentHandler.GetPaymentByID)
	router.GET("/payments", paymentHandler.GetPaymentsByOrder)

	// 4. Wrap router in net/http Server and serve in goroutine
	srv := &http.Server{
		Addr:    ":8082",
		Handler: router,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("failed to listen: %v\n", err)
		}
	}()

	// 5. Block main until kill signal arrives
	<-ctx.Done()
	slog.Info("shutdown signal received, draining...")

	// 6. Graceful HTTP shutdown with cutoff timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("server forced to shutdown", "error", err)
	}

	slog.Info("all background workers and HTTP server stopped cleanly")
}
