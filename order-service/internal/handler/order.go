package handler

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"order-service/internal/kafka"
	"order-service/internal/store"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type OrderHandler struct {
	Queries  *store.Queries
	producer *kafka.Producer
}

func NewOrderHandler(queries *store.Queries, producer *kafka.Producer) *OrderHandler {
	return &OrderHandler{Queries: queries, producer: producer}
}

// the functions added here are the representation of the public api endpoints, the ones
// that are exposed to the users

type CreateOrderRequest struct {
	CustomerID int32 `json:"customer_id" binding:"required"`
	Items      []struct {
		ProductID int32   `json:"product_id" binding:"required"`
		Quantity  int32   `json:"quantity" binding:"required"`
		UnitPrice float64 `json:"unit_price" binding:"required"`
	} `json:"items" binding:"required,dive"`
}

func (h *OrderHandler) CreateOrder(c *gin.Context) {

	var req CreateOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		slog.Error("Failed to bind json : CreateOrder", "error: ", err)
		c.JSON(http.StatusBadRequest, gin.H{"Error": err.Error()})
		return
	}

	if len(req.Items) == 0 {
		slog.Error("Order Must have at least one item")
		c.JSON(http.StatusBadRequest, gin.H{"Error": "Order must have at least one item"})
		return
	}

	totalAmount := 0.0
	for _, item := range req.Items {
		totalAmount += float64(item.Quantity) * item.UnitPrice
	}

	totalAmountNumeric, err := toNumeric(totalAmount)
	if err != nil {
		slog.Error("Failed to convert total amount to numeric", "error: ", err)
		c.JSON(http.StatusInternalServerError, gin.H{"Error": "Failed to convert total amount to numeric"})
		return
	}

	ctx := c.Request.Context()

	createdOrder, err := h.Queries.CreateOrder(ctx, store.CreateOrderParams{
		CustomerID:  req.CustomerID,
		TotalAmount: totalAmountNumeric,
		Status:      "PENDING",
	})

	if err != nil {
		slog.Error("Failed to create order", "error : ", err)
		c.JSON(http.StatusInternalServerError, gin.H{"Error": "Failed to create order"})
		return
	}

	createdItems := make([]store.OrderItem, 0)
	for _, item := range req.Items {

		numericItemPrice, err := toNumeric(item.UnitPrice)
		if err != nil {
			slog.Error("Faile to convert unit price", "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"Error": "Failed to convert unit price"})
			return
		}

		createdItem, err := h.Queries.CreateOrderItem(ctx, store.CreateOrderItemParams{
			OrderID:   createdOrder.ID,
			ProductID: item.ProductID,
			Quantity:  item.Quantity,
			UnitPrice: numericItemPrice,
		})

		if err != nil {
			slog.Error("Failed to create order item", "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"Error": "Failed to create order item"})
			return
		}

		createdItems = append(createdItems, createdItem)

	}

	createdItemEvents := make([]kafka.OrderItemEvent, 0)
	for _, item := range createdItems {
		createdItemEvents = append(createdItemEvents, kafka.OrderItemEvent{
			ProductId: item.ProductID,
			Quantity:  item.Quantity,
		})
	}

	event := kafka.OrderCreatedEvent{
		OrderId:    createdOrder.ID,
		CustomerId: createdOrder.CustomerID,
		Items:      createdItemEvents,
	}

	if err := h.producer.Publish(ctx, strconv.Itoa(int(createdOrder.ID)), event); err != nil {
		slog.Error("Failed to publish OrderCreated", "error", err, "order_id", createdOrder.ID)
	}

	c.JSON(http.StatusCreated, gin.H{
		"Order": createdOrder,
		"Items": createdItems,
	})

}

func (h *OrderHandler) GetOrdersById(c *gin.Context) {

	var id = c.Param("id")
	idInt, err := strconv.ParseInt(id, 10, 32)
	if err != nil {
		slog.Error("Unable to convert integer to string for id", "error:", err)
		c.JSON(http.StatusInternalServerError, gin.H{"Error": "Unable to convert integer to string for id"})
		return
	}

	ctx := c.Request.Context()

	order, err := h.Queries.GetOrderByID(ctx, int32(idInt))
	if err != nil {

		if errors.Is(err, pgx.ErrNoRows) {
			slog.Error("no Such order found", "error:", pgx.ErrNoRows)
			c.JSON(http.StatusNotFound, gin.H{"Error": "no such order found"})
			return
		}

		slog.Error("failed to get orders by ID", "error:", err)
		c.JSON(http.StatusInternalServerError, gin.H{"Error": "Failed to execute get orders by ID"})
		return
	}

	orderItems, err := h.Queries.GetOrderItemsByOrderId(ctx, order.ID)
	if err != nil {
		slog.Error("failed to get order items", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"Error": "failed to fetch order items"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"Order":     order,
		"OrderItem": orderItems,
	})

}

func (h *OrderHandler) ListOrders(c *gin.Context) {

	var customerId = c.Query("customer_id")
	customerIdInt, err := strconv.ParseInt(customerId, 10, 32)
	if err != nil {
		slog.Error("Unable to convert integer ot customer ID string value")
		c.JSON(http.StatusInternalServerError, gin.H{"Error": "Unable to convert integer to customerId string value to integer"})
		return
	}

	ctx := c.Request.Context()
	orders, err := h.Queries.ListOrdersByCustomer(ctx, int32(customerIdInt))
	if err != nil {
		slog.Error("Failed to get orders history", "Errors:", err)
		c.JSON(http.StatusInternalServerError, gin.H{"Error": "failed to get orders history by customerId"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"Orders": orders,
	})

}

func toNumeric(value float64) (pgtype.Numeric, error) {
	var n pgtype.Numeric
	err := n.Scan(fmt.Sprintf("%.2f", value))
	return n, err
}
