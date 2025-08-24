package internal

type QueueStatus struct {
	QueueType        string
	TotalMessages    int64
	AckedMessages    int64
	InflightMessages int64
	DLQMessages      int64
}

type Queue interface {
	Enqueue(group_name string, item interface{}) error
	Dequeue(group_name string, consumer_id string) (interface{}, int64, error)
	Ack(group_name string, messageID int64) error
	Nack(group_name string, messageID int64) error
	Status() (QueueStatus, error)
	Shutdown() error
}
