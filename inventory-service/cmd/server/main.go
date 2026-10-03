package main

import (
	"context"
	"errors"
	"inventory-service/internal/handler"
	"inventory-service/internal/kafka"
	"inventory-service/internal/store"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
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
		log.Fatalf("failed to connect to database : %v", err)
	}

	defer pool.Close()

	queries := store.New(pool)
	inventoryHandler := handler.NewInventoryHandler(queries)

	consumer := kafka.NewConsumer("kafka:9094", "order-events", "inventory-service-group", queries, pool)
	publisher := kafka.NewPublisher(queries, "inventory-events", "kafka:9094")

	go consumer.Start(ctx)
	go publisher.Start(ctx)

	// public api endpoints
	router.POST("/inventory", inventoryHandler.CreateInventory)
	router.GET("/inventory/:id", inventoryHandler.GetInventoryByProductId)

	srv := &http.Server{
		Addr:    ":8081",
		Handler: router,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Failed to listen %v\n", err)
		}
	}()

	<-ctx.Done()
	slog.Info("Shutdown signal received, draining...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("Server forced to shutdown", "error:", err)
	}

	slog.Info("all background workers and HTTP server stopped cleanly")

	// router.Run(":8081")

}
