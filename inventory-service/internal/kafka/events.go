package kafka

import "encoding/json"

type EventEnvelope struct {
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
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
	ProductId int32 `json:"product_id"`
	Quantity  int32 `json:"quantity"`
}

type OrderCreatedEvent struct {
	OrderId    int32            `json:"order_id"`
	CustomerId int32            `json:"customer_id"`
	Items      []OrderItemEvent `json:"items"`
}
