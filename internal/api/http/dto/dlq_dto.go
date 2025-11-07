package dto

import "go-msg-queue-mini/internal"

var MAX_DLQ_PEEK_LIMIT = 100

type DLQListRequest struct {
	Options internal.PeekOptions `json:"options"`
}

type DLQListResponse struct {
	Status   string       `json:"status"`
	Messages []DLQMessage `json:"messages"`
}

type DLQDetailResponse struct {
	Status  string     `json:"status"`
	Message DLQMessage `json:"message"`
}

type DLQMessage struct {
	Payload     []byte
	ID          int64
	Reason      string
	FailedGroup string
	InsertedAt  string
}
