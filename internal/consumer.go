package internal

import (
	"context"
	"fmt"
	"time"
)

func Consume(ctx context.Context, queue *Queue, name string) {
	for {
		select {
		case <-ctx.Done():
			_shutdown(queue, name) // Consume remaining messages before exiting
			fmt.Printf("Consumer %s stopped.\n", name)
			return
		case <-queue.msgSignal: // Wait for a new message
			_consume(queue, name)
		}
	}
}

func _consume(queue *Queue, name string) {
	if !queue.IsEmpty() {
		item := queue.Dequeue()
		if item != nil {
			fmt.Printf("Consumed by %s: %s\n", name, item.(string)) // Log the consumed item
		}
		time.Sleep(1 * time.Second) // Simulate processing time
	}
}

func _shutdown(queue *Queue, name string) {
	fmt.Printf("Consumer %s shutting down...\n", name)
	for !queue.IsEmpty() {
		item := queue.Dequeue()
		if item != nil {
			fmt.Printf("Consumed item: %s\n", item.(string))
		}
	}
	fmt.Printf("Consumer %s has finished processing all items.\n", name)
}
