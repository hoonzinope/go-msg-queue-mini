package dto

import (
	"encoding/json"
	"time"
)

const PeekMaxLimit = 100
const PeekMsgPreviewLength = 50

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
	ErrorMsg   string          `json:"error_msg,omitempty"`
}

type DetailResponse struct {
	Status  string      `json:"status"`
	Message PeekMessage `json:"message"`
}
