package queue_error

import "errors"

var (
	ErrEmpty        = errors.New("queue empty")
	ErrContended    = errors.New("contention: message not claimed")
	ErrLeaseExpired = errors.New("lease expired")
)
