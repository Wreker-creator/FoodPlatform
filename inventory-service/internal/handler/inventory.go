package handler

import (
	"errors"
	"inventory-service/internal/store"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
)

type InventoryHandler struct {
	Queries *store.Queries
}

func NewInventoryHandler(queries *store.Queries) *InventoryHandler {
	return &InventoryHandler{Queries: queries}
}

type CreateInventoryRequest struct {
	ProductId int32 `json:"product_id" binding:"required"`
	Quantity  int32 `json:"quantity" binding:"required,gt=0"`
}

func (h *InventoryHandler) CreateInventory(c *gin.Context) {

	var req CreateInventoryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		slog.Error("Failed to bind json : CreateInventory", "error: ", err)
		c.JSON(http.StatusBadRequest, gin.H{"Error": err.Error()})
		return
	}

	if req.Quantity <= 0 {
		slog.Error("inventory cannot be empty")
		c.JSON(http.StatusBadRequest, gin.H{"Error": "Inventory cannot be empty"})
		return
	}

	ctx := c.Request.Context()

	createdInventory, err := h.Queries.CreateInventory(ctx, store.CreateInventoryParams{
		ProductID:         req.ProductId,
		AvailableQuantity: req.Quantity,
	})

	if err != nil {
		slog.Error("Failed to create inventory", "error: ", err)
		c.JSON(http.StatusInternalServerError, gin.H{"Error": "Failed to create inventory"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"Inventory": createdInventory,
	})

}

func (h *InventoryHandler) GetInventoryByProductId(c *gin.Context) {

	var id = c.Param("id")
	idInt, err := strconv.ParseInt(id, 10, 32)
	if err != nil {
		slog.Error("Unable to convert integer to string for id", "error:", err)
		c.JSON(http.StatusInternalServerError, gin.H{"Error": "Unable to convert integer to string for id"})
		return
	}

	ctx := c.Request.Context()

	inventory, err := h.Queries.GetInventoryByProductId(ctx, int32(idInt))
	if err != nil {

		if errors.Is(err, pgx.ErrNoRows) {
			slog.Error("no Such inventory found", "error:", pgx.ErrNoRows)
			c.JSON(http.StatusNotFound, gin.H{"Error": "no such inventory found"})
			return
		}

		slog.Error("failed to get inventory by ID", "error:", err)
		c.JSON(http.StatusInternalServerError, gin.H{"Error": "Failed to execute get inventory by ID"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"Inventory": inventory,
	})

}

func (h *InventoryHandler) IncrementInventory(c *gin.Context) {

	var req CreateInventoryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		slog.Error("Failed to bind json : CreateInventory", "error: ", err)
		c.JSON(http.StatusBadRequest, gin.H{"Error": err.Error()})
		return
	}

	ctx := c.Request.Context()

	err := h.Queries.IncrementInventory(ctx, store.IncrementInventoryParams{
		ProductID:         req.ProductId,
		AvailableQuantity: req.Quantity,
	})

	if err != nil {
		slog.Error("Failed to increment inventory", "error: ", err)
		c.JSON(http.StatusInternalServerError, gin.H{"Error": "Failed to increment inventory"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"Inventory": req,
	})

}

func (h *InventoryHandler) DecrementInventory(c *gin.Context) {

	var req CreateInventoryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		slog.Error("Failed to bind json : CreateInventory", "error: ", err)
		c.JSON(http.StatusBadRequest, gin.H{"Error": err.Error()})
		return
	}

	ctx := c.Request.Context()

	rowsAffected, err := h.Queries.DecrementInventory(ctx, store.DecrementInventoryParams{
		ProductID:         req.ProductId,
		AvailableQuantity: req.Quantity,
	})
	if err != nil {
		slog.Error("Failed to decrement inventory", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"Error": "Failed to decrement inventory"})
		return
	}
	if rowsAffected == 0 {
		slog.Warn("insufficient stock", "product_id", req.ProductId, "requested", req.Quantity)
		c.JSON(http.StatusConflict, gin.H{"Error": "insufficient stock"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"Message": "inventory decremented"})

}
