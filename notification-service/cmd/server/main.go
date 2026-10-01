package main

import (
	"context"
	"log"
	"log/slog"
	"notification-service/internal/handler"
	"notification-service/internal/kafka"
	"notification-service/internal/store"
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
		log.Fatalf("failed to connect to database: %v", err)
	}

	defer pool.Close()

	queries := store.New(pool)
	notificationsHandler := handler.NewNotificationsHandler(queries)

	consumer := kafka.NewConsumer("kafka:9094", "order-events", "notification-order-group", queries)
	go consumer.Start(ctx)

	router.GET("/notifications/:id", notificationsHandler.GetNotificationsById)

	router.Run(":8084")

}
