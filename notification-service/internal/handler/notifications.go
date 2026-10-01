package handler

import (
	"errors"
	"log/slog"
	"net/http"
	"notification-service/internal/store"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
)

type NotificationsHandler struct {
	Queries *store.Queries
}

func NewNotificationsHandler(Queries *store.Queries) *NotificationsHandler {
	return &NotificationsHandler{
		Queries: Queries,
	}
}

func (h *NotificationsHandler) GetNotificationsById(c *gin.Context) {

	var id = c.Param("id")
	idInt, err := strconv.Atoi(id)
	if err != nil {
		slog.Error("Unable to convert integer to string for order_id", "error:", err)
		c.JSON(http.StatusBadRequest, gin.H{"Error": "Unable to convert integer to string for order_id"})
		return
	}

	ctx := c.Request.Context()

	notifications, err := h.Queries.GetNotificationsByOrderId(ctx, int32(idInt))
	if err != nil {

		if errors.Is(err, pgx.ErrNoRows) {
			slog.Error("No notifications for order id ", idInt)
			c.JSON(http.StatusNotFound, gin.H{"Error": "No such notification found"})
			return
		}

		slog.Error("Error occurred while fetching notifications", "error:", err)
		c.JSON(http.StatusInternalServerError, gin.H{"Error": "Error occurred while fetching notifications"})
		return

	}

	c.JSON(http.StatusOK, gin.H{
		"OrderId":       idInt,
		"notifications": notifications,
	})

}
