package internal

import (
	"context"
	"fmt"
	"go-msg-queue-mini/util"
	"time"
)

func Consume(ctx context.Context, queue Queue, name string) {
	for {
		select {
		case <-ctx.Done():
			_shutdown(queue, name) // Consume remaining messages before exiting
			fmt.Printf("Consumer %s stopped.\n", name)
			return
		default:
			_consume(queue, name)
		}
	}
}

func _consume(queue Queue, name string) {
	messages, err := queue.Dequeue(name, 10) // Dequeue up to 10 messages
	if err == nil {
		for _, msg := range messages {
			// Simulate message processing failure randomly (1 in 4 chance of failure)
			if util.GenerateNumber(1, 3) == 1 {
				fmt.Printf("Consumer %s: NACKing message %d, data: %s\n", name, msg.Id, msg.Item.(string))
				if err := queue.Nack(name, msg.Id); err != nil {
					fmt.Printf("Error NACKing message %d for %s: %v\n", msg.Id, name, err)
				}
				time.Sleep(100 * time.Millisecond) // Sleep to avoid busy loop
				break                              // Simulate a processing failure
			} else {
				fmt.Printf("Consumer %s: ACKing message %d, data: %s\n", name, msg.Id, msg.Item.(string))
				if err := queue.Ack(name, msg.Id); err != nil {
					fmt.Printf("Error ACKing message %d for %s: %v\n", msg.Id, name, err)
				}
			}
			time.Sleep(100 * time.Millisecond) // Sleep to avoid busy loop
		}
	}
}

func _shutdown(queue Queue, name string) {
	if err := queue.Shutdown(); err != nil {
		fmt.Printf("Error shutting down queue for %s: %v\n", name, err)
	} else {
		fmt.Printf("Queue for %s shut down successfully.\n", name)
	}
}
