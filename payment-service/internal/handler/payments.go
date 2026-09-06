package handler

import (
	"errors"
	"log/slog"
	"net/http"
	"payment-service/internal/store"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
)

// Handler — GetPaymentByID, ListPaymentsByOrder — two read-only endpoints
type PaymentHandler struct {
	Queries *store.Queries
}

func NewPaymentHandler(queries *store.Queries) *PaymentHandler {
	return &PaymentHandler{
		Queries: queries,
	}
}

// GetPaymentByID
func (h *PaymentHandler) GetPaymentByID(c *gin.Context) {

	var id = c.Param("id")
	idInt, err := strconv.Atoi(id)
	if err != nil {
		slog.Error("Unable to convert integer to string for id", "error:", err)
		c.JSON(http.StatusBadRequest, gin.H{"Error": "Unable to convert integer to string for id"})
		return
	}

	ctx := c.Request.Context()

	payment, err := h.Queries.GetPaymentByID(ctx, int32(idInt))
	if err != nil {

		if errors.Is(err, pgx.ErrNoRows) {
			slog.Error("no Such payment found", "error:", pgx.ErrNoRows)
			c.JSON(http.StatusNotFound, gin.H{"Error": "no such payment found"})
			return
		}

		slog.Error("Error occurred while fetching payment", "error:", err)
		c.JSON(http.StatusInternalServerError, gin.H{"Error": "Error occurred while fetching payment"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"PaymentId": idInt,
		"Payment":   payment,
	})

}

// ListPaymentsByOrder
func (h *PaymentHandler) GetPaymentsByOrder(c *gin.Context) {

	var orderId = c.Query("order_id")
	orderIdInt, err := strconv.Atoi(orderId)
	if err != nil {
		slog.Error("Unable to convert integer to string for order_id", "error:", err)
		c.JSON(http.StatusBadRequest, gin.H{"Error": "Unable to convert integer to string for order_id"})
		return
	}

	ctx := c.Request.Context()

	payments, err := h.Queries.GetPaymentsByOrderID(ctx, int32(orderIdInt))
	if err != nil {
		slog.Error("Failed to get payments history", "Errors:", err)
		c.JSON(http.StatusInternalServerError, gin.H{"Error": "Failed to get payments history"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"OrderId":  orderIdInt,
		"Payments": payments,
	})
}
