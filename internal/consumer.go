package internal

import (
	"context"
	"fmt"
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
			fmt.Printf("Consumed by %s: %s\n", name, msg.Item.(string)) // Log the consumed item
			if err := queue.Ack(name, msg.Id); err != nil {
				fmt.Printf("Error acknowledging message %d for %s: %v\n", msg.Id, name, err)
			}
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
