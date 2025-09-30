package http

import "encoding/json"

type EnqueueRequest struct {
	Message json.RawMessage `json:"message" binding:"required"`
	Delay   string          `json:"delay"` // e.g., "10s", "1m"
}

type EnqueueResponse struct {
	Status  string          `json:"status"`
	Message json.RawMessage `json:"message"`
}

type EnqueueBatchRequest struct {
	Mode     string            `json:"mode" binding:"required,oneof=partialSuccess stopOnFailure"`
	Delay    string            `json:"delay"` // e.g., "10s", "1m"
	Messages []json.RawMessage `json:"messages" binding:"required"`
}

type EnqueueBatchResponse struct {
	Status         string          `json:"status"`
	SuccessCount   int64           `json:"success_count"`
	FailureCount   int64           `json:"failure_count"`
	FailedMessages []FailedMessage `json:"failed_messages"`
}

type FailedMessage struct {
	Index   int64  `json:"index"`
	Message string `json:"message"`
	Error   string `json:"error"`
}

type DequeueRequest struct {
	Group      string `json:"group" binding:"required"`
	ConsumerID string `json:"consumer_id" binding:"required"`
}

type DequeueMessage struct {
	Payload interface{} `json:"payload"`
	Receipt string      `json:"receipt"`
	ID      int64       `json:"id"`
}

type DequeueResponse struct {
	Status  string         `json:"status"`
	Message DequeueMessage `json:"message"`
}

type AckRequest struct {
	Group     string `json:"group" binding:"required"`
	MessageID int64  `json:"message_id" binding:"required"`
	Receipt   string `json:"receipt" binding:"required"`
}

type NackRequest struct {
	Group     string `json:"group" binding:"required"`
	MessageID int64  `json:"message_id" binding:"required"`
	Receipt   string `json:"receipt" binding:"required"`
}

type QueueStatus struct {
	QueueType        string `json:"queue_type"`
	TotalMessages    int64  `json:"total_messages"`
	AckedMessages    int64  `json:"acked_messages"`
	InflightMessages int64  `json:"inflight_messages"`
	DLQMessages      int64  `json:"dlq_messages"`
}
type StatusResponse struct {
	Status      string      `json:"status"`
	QueueStatus QueueStatus `json:"queue_status"`
}

type PeekRequest struct {
	Group string `json:"group" binding:"required"`
}

type PeekResponse struct {
	Status  string         `json:"status"`
	Message DequeueMessage `json:"message"`
}

type RenewRequest struct {
	Group     string `json:"group" binding:"required"`
	MessageID int64  `json:"message_id" binding:"required"`
	Receipt   string `json:"receipt" binding:"required"`
	ExtendSec int    `json:"extend_sec" binding:"required"`
}
