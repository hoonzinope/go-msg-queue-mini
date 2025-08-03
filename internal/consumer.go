package internal

import (
	"context"
	"fmt"
)

func Consume(ctx context.Context, queue *Queue, name string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			_consume(queue, name)
		}
	}
}

func _consume(queue *Queue, name string) {
	for !queue.IsEmpty() {
		item := queue.Dequeue()
		if item != nil {
			fmt.Printf("Consumed by %s: %s\n", name, item.(string)) // Log the consumed item
		}
	}
}
