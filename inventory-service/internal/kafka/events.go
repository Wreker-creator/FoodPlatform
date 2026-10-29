package kafka

import (
	"encoding/json"
	"time"
)

type DLQMessage struct {
	OriginalTopic string    `json:"original_topic"`
	OriginalKey   string    `json:"original_key"`
	OriginalValue string    `json:"original_value"` // Stringified raw bytes
	ErrorReason   string    `json:"error_reason"`
	FailedAt      time.Time `json:"failed_at"`
}

type EventEnvelope struct {
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
	EventID   string          `json:"event_id"`
}

// inventory reserved
type InventoryReservedEvent struct {
	OrderId int32            `json:"order_id"`
	Items   []OrderItemEvent `json:"items"`
}

type InventoryRejectedEvent struct {
	OrderId int32  `json:"order_id"`
	Reason  string `json:"reason"`
}

type OrderItemEvent struct {
	ProductId int32   `json:"product_id" required:"true"`
	Quantity  int32   `json:"quantity" required:"true"`
	UnitPrice float64 `json:"unit_price" required:"true"`
}

type OrderCreatedEvent struct {
	OrderId    int32            `json:"order_id"`
	CustomerId int32            `json:"customer_id"`
	Items      []OrderItemEvent `json:"items"`
}

type OrderCancelledEvent struct {
	OrderId          int32            `json:"order_id"`
	Reason           string           `json:"reason"`
	Items            []OrderItemEvent `json:"items,omitempty"`
	ReleaseInventory bool             `json:"release_inventory"`
}
