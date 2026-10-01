package kafka

import "encoding/json"

type EventEnvelope struct {
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
}

type OrderCancelledEvent struct {
	OrderId          int32  `json:"order_id"`
	Reason           string `json:"reason"`
	ReleaseInventory bool   `json:"release_inventory"`
}

type OrderConfirmedEvent struct {
	OrderId    int32 `json:"order_id"`
	CustomerId int32 `json:"customer_id"`
}
