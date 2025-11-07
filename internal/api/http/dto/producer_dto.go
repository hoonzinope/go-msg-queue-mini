package dto

import "encoding/json"

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
