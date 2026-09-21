package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"payment-service/internal/handler"
	"payment-service/internal/kafka"
	"payment-service/internal/store"

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
		log.Fatalf("failed to connect to database: %v", err)
	}

	defer pool.Close()

	queries := store.New(pool)
	paymentHandler := handler.NewPaymentHandler(queries)

	producer := kafka.NewProducer("kafka:9094", "payment-events")
	consumer := kafka.NewConsumer("kafka:9094", "inventory-events", "payment-inventory-group", queries, producer, pool)

	go consumer.Start(ctx)

	router.GET("/payments/:id", paymentHandler.GetPaymentByID)
	router.GET("/payments", paymentHandler.GetPaymentsByOrder)

	router.Run(":8082")

}
