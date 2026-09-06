package kafka

import "encoding/json"

type EventEnvelope struct {
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
}

type InventoryReservedEvent struct {
	OrderId int32            `json:"order_id"`
	Items   []OrderItemEvent `json:"items"`
}

type OrderItemEvent struct {
	ProductId int32   `json:"product_id" required:"true"`
	Quantity  int32   `json:"quantity" required:"true"`
	UnitPrice float64 `json:"unit_price" required:"true"`
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
