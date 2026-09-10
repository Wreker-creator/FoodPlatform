package kafka

import "encoding/json"

type EventEnvelope struct {
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
}

type OrderCancelledEvent struct {
	OrderId          int32            `json:"order_id"`
	Reason           string           `json:"reason"`
	Items            []OrderItemEvent `json:"items,omitempty"`
	ReleaseInventory bool             `json:"release_inventory"`
}

type OrderConfirmedEvent struct {
	OrderId    int32 `json:"order_id"`
	CustomerId int32 `json:"customer_id"`
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

type PaymentSucceededEvent struct {
	OrderId   int32   `json:"order_id"`
	PaymentId int32   `json:"payment_id"`
	Amount    float64 `json:"amount"`
}

type PaymentFailedEvent struct {
	OrderId   int32   `json:"order_id"`
	PaymentId int32   `json:"payment_id"`
	Amount    float64 `json:"amount"`
	Reason    string  `json:"reason"`
}
