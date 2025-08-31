package internal

type QueueStatus struct {
	QueueType        string
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

type Queue interface {
	Enqueue(item interface{}) error
	Dequeue(group_name string, consumer_id string) (QueueMessage, error)
	Ack(group_name string, messageID int64, receipt string) error
	Nack(group_name string, messageID int64, receipt string) error
	Status() (QueueStatus, error)
	Shutdown() error
	// new api
	Peek(group_name string) (QueueMessage, error)
	Renew(group_name string, messageID int64, receipt string, extendSec int) error
}
