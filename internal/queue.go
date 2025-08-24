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
	Enqueue(group_name string, item interface{}) error
	Dequeue(group_name string, consumer_id string) (interface{}, int64, error)
	Ack(group_name string, messageID int64) error
	Nack(group_name string, messageID int64) error
	Status() (QueueStatus, error)
	Shutdown() error
}
