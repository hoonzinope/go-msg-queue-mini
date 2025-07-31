package internal

import (
	"context"
	"fmt"
)

func Consume(ctx context.Context, queue *Queue) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			_consume(queue)
		}
	}
}

func _consume(queue *Queue) {
	for !queue.IsEmpty() {
		item := queue.Dequeue()
		if item != nil {
			fmt.Printf("Consumed: %s\n", item.(string)) // Log the consumed item
		}
	}
}
