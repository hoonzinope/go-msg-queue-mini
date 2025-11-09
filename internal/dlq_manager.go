package internal

type DLQManager interface {
	RedriveDLQ(queueName string, messageIDs []int64) error
}
