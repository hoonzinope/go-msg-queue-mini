package queue_error

import "errors"

var (
	ErrEmpty           = errors.New("queue empty")
	ErrContended       = errors.New("contention: message not claimed")
	ErrNoMessage       = errors.New("no message available")
	ErrLeaseExpired    = errors.New("lease expired")
	ErrDuplicate       = errors.New("duplicate message")
	ErrMessageNotFound = errors.New("message not found")
)
