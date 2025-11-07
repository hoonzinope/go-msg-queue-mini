package dto

import "encoding/json"

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

type RenewRequest struct {
	Group     string `json:"group" binding:"required"`
	MessageID int64  `json:"message_id" binding:"required"`
	Receipt   string `json:"receipt" binding:"required"`
	ExtendSec int    `json:"extend_sec" binding:"required"`
}
