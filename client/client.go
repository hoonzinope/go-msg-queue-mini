package client

import (
	"go-msg-queue-mini/internal"
	"log/slog"
)

type QueueClient struct {
	QueueName string
	Queue     internal.Queue
	Logger    *slog.Logger
}

func NewQueue(queueName string, queue internal.Queue, logger *slog.Logger) *QueueClient {
	// TODO : implement NewQueue logic
	return &QueueClient{
		QueueName: queueName,
		Queue:     queue,
		Logger:    logger,
	}
}

func (qc *QueueClient) Produce(item interface{}, delay string) error {
	// TODO : implement Enqueue logic
	// If delay is not provided, use default value "0s"
	if err := qc.Queue.Enqueue(qc.QueueName, item, delay); err != nil {
		return err
	}
	return nil
}

func (qc *QueueClient) Consume(groupName string, consumerID string, processFunc func(item interface{}) error) error {
	// TODO : implement Dequeue logic
	message, err := qc.Queue.Dequeue(qc.QueueName, groupName, consumerID)
	if err != nil {
		return err
	}
	// Process the message. On error, NACK and do NOT ACK.
	if err := processFunc(message.Payload); err != nil {
		if nackErr := qc.Queue.Nack(qc.QueueName, groupName, message.ID, message.Receipt); nackErr != nil {
			return nackErr
		}
		qc.Logger.Error("Processing error", slog.String("consumer", consumerID), slog.Any("message", message.Payload))
		return err
	}
	// Only ACK when processing succeeds.
	if ackErr := qc.Queue.Ack(qc.QueueName, groupName, message.ID, message.Receipt); ackErr != nil {
		return ackErr
	}
	return nil
}

func (qc *QueueClient) Shutdown() error {
	return qc.Queue.Shutdown()
}
