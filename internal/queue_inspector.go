package internal

type QueueStatus struct {
	QueueType        string
	QueueName        string
	TotalMessages    int64
	AckedMessages    int64
	InflightMessages int64
	DLQMessages      int64
}

type QueueInspector interface {
	Status(queueName string) (QueueStatus, error)
	StatusAll() (map[string]QueueStatus, error)
}
