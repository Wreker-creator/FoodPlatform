package main

import (
	"context"
	"inventory-service/internal/handler"
	"inventory-service/internal/kafka"
	"inventory-service/internal/store"
	"log"
	"log/slog"
	"os"

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

	ctx := context.Background()
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

	router.Run(":8081")

}
