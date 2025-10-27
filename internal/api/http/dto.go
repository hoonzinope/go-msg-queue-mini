package http

import (
	"encoding/json"
	"time"
)

type EnqueueRequest struct {
	Message         json.RawMessage `json:"message" binding:"required"`
	Delay           string          `json:"delay"`            // optional, e.g., "10s", "1m"
	DeduplicationID string          `json:"deduplication_id"` // optional, for FIFO queues
}

type EnqueueResponse struct {
	Status  string          `json:"status"`
	Message json.RawMessage `json:"message"`
}

type EnqueueBatchRequest struct {
	Mode     string           `json:"mode" binding:"required,oneof=partialSuccess stopOnFailure"`
	Messages []EnqueueMessage `json:"messages" binding:"required,dive,required"`
}

type EnqueueMessage struct {
	Message         json.RawMessage `json:"message"`
	Delay           string          `json:"delay"`
	DeduplicationID string          `json:"deduplication_id"`
}

type EnqueueBatchResponse struct {
	Status         string          `json:"status"`
	SuccessCount   int64           `json:"success_count"`
	FailureCount   int64           `json:"failure_count"`
	FailedMessages []FailedMessage `json:"failed_messages"`
}

type FailedMessage struct {
	Index   int64           `json:"index"`
	Message json.RawMessage `json:"message"`
	Error   string          `json:"error"`
}

type DequeueRequest struct {
	Group      string `json:"group" binding:"required"`
	ConsumerID string `json:"consumer_id" binding:"required"`
}

type DequeueMessage struct {
	Payload json.RawMessage `json:"payload"`
	Receipt string          `json:"receipt"`
	ID      int64           `json:"id"`
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
type StatusAllResponse struct {
	Status      string                 `json:"status"`
	AllQueueMap map[string]QueueStatus `json:"all_queue_map"`
}

type PeekRequest struct {
	Group   string      `json:"group" binding:"required"`
	Options PeekOptions `json:"options"`
}

type PeekOptions struct {
	Limit   int    `json:"limit"`   // number of messages to peek
	Cursor  int64  `json:"cursor"`  // for pagination
	Order   string `json:"order"`   // "asc" or "desc"
	Preview bool   `json:"preview"` // whether to return full message or just metadata
}

type PeekResponse struct {
	Status   string        `json:"status"`
	Messages []PeekMessage `json:"messages"`
}

type PeekMessage struct {
	Payload    json.RawMessage `json:"payload"`
	Receipt    string          `json:"receipt"`
	ID         int64           `json:"id"`
	InsertedAt time.Time       `json:"inserted_at"`
}

type RenewRequest struct {
	Group     string `json:"group" binding:"required"`
	MessageID int64  `json:"message_id" binding:"required"`
	Receipt   string `json:"receipt" binding:"required"`
	ExtendSec int    `json:"extend_sec" binding:"required"`
}
