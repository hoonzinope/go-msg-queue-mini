package internal

import (
	"context"
	"fmt"
	"go-msg-queue-mini/util"
	"time"
)

func Consume(ctx context.Context, queue Queue, group_name, consumer_name string) {
	for {
		select {
		case <-ctx.Done():
			_shutdown(queue, group_name, consumer_name) // Consume remaining messages before exiting
			fmt.Printf("Consumer %s stopped.\n", consumer_name)
			return
		default:
			_peek(queue, group_name)
			_consume(queue, group_name, consumer_name)
		}
	}
}

func _peek(queue Queue, group_name string) {
	queueMessage, err := queue.Peek(group_name)
	if err != nil {
		fmt.Printf("Error peeking message for %s: %v\n", group_name, err)
		return
	} else if queueMessage.ID == 0 {
		return
	}
	fmt.Printf("Peeked by %s message: %v ID: %d Receipt: %s\n", group_name, queueMessage.Payload, queueMessage.ID, queueMessage.Receipt)
}

func _consume(queue Queue, group_name, consumer_name string) {
	queueMessage, err := queue.Dequeue(group_name, consumer_name)
	message := queueMessage.Payload
	messageID := queueMessage.ID
	messageReceipt := queueMessage.Receipt
	if err == nil {
		fmt.Println("Dequeued by", consumer_name, "message:", message, "ID:", messageID, "Receipt:", messageReceipt)
		// Simulate message processing failure randomly (1 in 3 chance of failure)
		if util.GenerateNumber(1, 3) == 1 {
			fmt.Printf("group :  %s NACKing message %d, data: %v\n", group_name, messageID, message)
			if err := queue.Nack(group_name, messageID, messageReceipt); err != nil {
				fmt.Printf("Error NACKing message %d for %s: %v\n", messageID, group_name, err)
			}
			time.Sleep(100 * time.Millisecond) // Sleep to avoid busy loop
		} else {
			fmt.Printf("group :  %s ACKing message %d, data: %v\n", group_name, messageID, message)
			if err := queue.Ack(group_name, messageID, messageReceipt); err != nil {
				fmt.Printf("Error ACKing message %d for %s: %v\n", messageID, group_name, err)
			}
		}
		time.Sleep(100 * time.Millisecond) // Sleep to avoid busy loop
	} else if messageID == -1 {
		return // No messages available, just return
	} else {
		fmt.Printf("Error dequeuing message for %s: %v\n", consumer_name, err)
		time.Sleep(500 * time.Millisecond) // Sleep to avoid busy loop
	}
}

func _shutdown(queue Queue, group_name, consumer_name string) {
	if err := queue.Shutdown(); err != nil {
		fmt.Printf("Error shutting down queue for %s: %v\n", consumer_name, err)
	} else {
		fmt.Printf("Queue for %s - %s shut down successfully.\n", group_name, consumer_name)
	}
}
