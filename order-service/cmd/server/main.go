package main

import (
	"context"
	"log"
	"log/slog"
	"order-service/internal/handler"
	"order-service/internal/kafka"
	"order-service/internal/store"
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

	// need to give values param
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		slog.Error("Failed to connect - ", "error: ", err)
	}

	defer pool.Close()

	queries := store.New(pool)
	producer := kafka.NewProducer("kafka:9094", "order-events")
	orderHandler := handler.NewOrderHandler(queries, producer)

	// public api endpoints
	router.POST("/orders", orderHandler.CreateOrder)
	router.GET("/orders/:id", orderHandler.GetOrdersById)
	router.GET("/orders", orderHandler.ListOrders)

	router.Run(":8080")

}
