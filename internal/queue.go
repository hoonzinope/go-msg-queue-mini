package internal

type QueueStatus struct {
	QueueType        string
	QueueName        string
	TotalMessages    int64
	AckedMessages    int64
	InflightMessages int64
	DLQMessages      int64
}

type QueueMessage struct {
	Payload interface{}
	ID      int64
	Receipt string
}

// add queue_name
type Queue interface {
	CreateQueue(queue_name string) error
	DeleteQueue(queue_name string) error
	Enqueue(queue_name string, item interface{}) error
	Dequeue(queue_name string, group_name string, consumer_id string) (QueueMessage, error)
	Ack(queue_name string, group_name string, messageID int64, receipt string) error
	Nack(queue_name string, group_name string, messageID int64, receipt string) error
	Status(queue_name string) (QueueStatus, error)
	Shutdown() error
	// new api
	Peek(queue_name string, group_name string) (QueueMessage, error)
	Renew(queue_name string, group_name string, messageID int64, receipt string, extendSec int) error
}
