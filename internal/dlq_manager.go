package internal

type DLQManager interface {
	RedriveDLQMessages(queueName string, messageIDs []int64) error
}
