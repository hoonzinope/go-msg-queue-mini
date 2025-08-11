package internal

type ConsumerStatus struct {
	ConsumerID string
	LastOffset int64
	Lag        int64
}

type QueueStatus struct {
	QueueType        string
	ActiveConsumers  int
	ExtraInfo        map[string]interface{}
	ConsumerStatuses map[string]ConsumerStatus
}

type Msg struct {
	Id   int64
	Item interface{}
}

type Queue interface {
	Enqueue(item interface{}) error
	Dequeue(consumerID string, maxCount int) ([]Msg, error)
	Ack(consumerID string, messageID int64) error
	Status() (QueueStatus, error)
	Shutdown() error
}
