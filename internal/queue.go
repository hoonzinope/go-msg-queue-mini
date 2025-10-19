package internal

type QueueMessage struct {
	Payload interface{}
	ID      int64
	Receipt string
}

type BatchResult struct {
	SuccessCount   int64
	FailedCount    int64
	FailedMessages []FailedMessage
}

type FailedMessage struct {
	Index   int64
	Message interface{}
	Reason  string
}

type EnqueueMessage struct {
	Item            interface{}
	Delay           string
	DeduplicationID string
}

// add queue_name
type Queue interface {
	CreateQueue(queue_name string) error
	DeleteQueue(queue_name string) error
	Enqueue(queue_name string, msg EnqueueMessage) error
	EnqueueBatch(queue_name, mode string, msgs []EnqueueMessage) (BatchResult, error)
	Dequeue(queue_name string, group_name string, consumer_id string) (QueueMessage, error)
	Ack(queue_name string, group_name string, messageID int64, receipt string) error
	Nack(queue_name string, group_name string, messageID int64, receipt string) error
	Shutdown() error
	// new api
	// Peek(queue_name string, group_name string) (QueueMessage, error)
	Renew(queue_name string, group_name string, messageID int64, receipt string, extendSec int) error
}
