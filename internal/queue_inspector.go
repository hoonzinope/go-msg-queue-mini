package internal

import "time"

type QueueStatus struct {
	QueueType        string
	QueueName        string
	TotalMessages    int64
	AckedMessages    int64
	InflightMessages int64
	DLQMessages      int64
}

type PeekMessage struct {
	Payload    []byte
	ID         int64
	Receipt    string
	InsertedAt time.Time
}

type PeekOptions struct {
	Limit   int    // number of messages to peek
	Cursor  int64  // for pagination
	Order   string // "asc" or "desc"
	Preview bool   // whether to return full message or just metadata
}

type QueueInspector interface {
	Status(queueName string) (QueueStatus, error)
	StatusAll() (map[string]QueueStatus, error)
	Peek(queueName, groupName string, options PeekOptions) ([]PeekMessage, error)
}
