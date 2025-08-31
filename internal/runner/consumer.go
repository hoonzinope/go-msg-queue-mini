package internal

import (
	"context"
	"errors"
	"fmt"
	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/internal/core"
	"go-msg-queue-mini/util"
	"time"
)

type Consumer struct {
	Queue        internal.Queue
	GroupName    string
	ConsumerName string
}

func (c *Consumer) Consume(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			c._shutdown() // Consume remaining messages before exiting
			util.Info(fmt.Sprintf("Consumer %s stopped.", c.ConsumerName))
			return
		default:
			c._peek()
			c._consume()
		}
	}
}

func (c *Consumer) _peek() {
	queueMessage, err := c.Queue.Peek(c.GroupName)
	if err != nil {
		if errors.Is(err, core.ErrEmpty) {
			return // No messages available, just return
		}
		util.Error(fmt.Sprintf("Error peeking message for %s: %v", c.GroupName, err))
		return
	} else if queueMessage.ID == 0 {
		return
	}
	util.Info(fmt.Sprintf("Peeked by %s message: %v ID: %d Receipt: %s", c.GroupName, queueMessage.Payload, queueMessage.ID, queueMessage.Receipt))
}

func (c *Consumer) _consume() {
	queueMessage, err := c.Queue.Dequeue(c.GroupName, c.ConsumerName)
	message := queueMessage.Payload
	messageID := queueMessage.ID
	messageReceipt := queueMessage.Receipt
	if err == nil {
		util.Info(fmt.Sprintf("Dequeued by %s message: %v ID: %d Receipt: %s", c.ConsumerName, message, messageID, messageReceipt))
		// Simulate message processing failure randomly (1 in 3 chance of failure)
		if util.GenerateNumber(1, 3) == 1 {
			util.Info(fmt.Sprintf("group :  %s NACKing message %d, data: %v", c.GroupName, messageID, message))
			if err := c.Queue.Nack(c.GroupName, messageID, messageReceipt); err != nil {
				util.Error(fmt.Sprintf("Error NACKing message %d for %s: %v", messageID, c.GroupName, err))
			}
			time.Sleep(100 * time.Millisecond) // Sleep to avoid busy loop
		} else {
			util.Info(fmt.Sprintf("group :  %s ACKing message %d, data: %v", c.GroupName, messageID, message))
			if err := c.Queue.Ack(c.GroupName, messageID, messageReceipt); err != nil {
				util.Error(fmt.Sprintf("Error ACKing message %d for %s: %v", messageID, c.GroupName, err))
			}
		}
		time.Sleep(100 * time.Millisecond) // Sleep to avoid busy loop
	} else {
		if errors.Is(err, core.ErrEmpty) || errors.Is(err, core.ErrContended) {
			// util.Info(fmt.Sprintf("No messages available for %s", c.ConsumerName))
			time.Sleep(500 * time.Millisecond) // Sleep to avoid busy loop
			return
		}
		util.Error(fmt.Sprintf("Error dequeuing message for %s: %v", c.ConsumerName, err))
		time.Sleep(500 * time.Millisecond) // Sleep to avoid busy loop
	}
}

func (c *Consumer) _shutdown() {
	if err := c.Queue.Shutdown(); err != nil {
		util.Error(fmt.Sprintf("Error shutting down queue for %s: %v", c.ConsumerName, err))
	} else {
		util.Info(fmt.Sprintf("Queue for %s - %s shut down successfully.", c.GroupName, c.ConsumerName))
	}
}
