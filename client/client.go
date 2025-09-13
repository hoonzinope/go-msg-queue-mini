package client

import (
	"fmt"
	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/util"
)

type QueueClient struct {
	QueueName string
	Queue     internal.Queue
}

func NewQueue(queueName string, queue internal.Queue) *QueueClient {
	// TODO : implement NewQueue logic
	return &QueueClient{
		QueueName: queueName,
		Queue:     queue,
	}
}

func (qc *QueueClient) Produce(item interface{}) error {
	// TODO : implement Enqueue logic
	if err := qc.Queue.Enqueue(qc.QueueName, item); err != nil {
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
	if err := processFunc(message.Payload); err != nil {
		err := qc.Queue.Nack(qc.QueueName, groupName, message.ID, message.Receipt)
		if err != nil {
			return err
		}
		util.Error(fmt.Sprintf("%s Processing error, message %s NACKed", consumerID, message.Payload))
	} else {
		err := qc.Queue.Ack(qc.QueueName, groupName, message.ID, message.Receipt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (qc *QueueClient) Shutdown() error {
	return qc.Queue.Shutdown()
}
