package internal

import (
	"context"
	"fmt"
	"go-msg-queue-mini/util"
	"time"
)

func Produce(ctx context.Context, queue *Queue) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			item := util.GenerateItem() // Generate a new item
			_produce(item, queue)
			time.Sleep(1 * time.Second) // Simulate some delay in producing items
		}
	}
}

func _produce(item interface{}, queue *Queue) {
	queue.Enqueue(item)                         // Enqueue the item
	fmt.Printf("Produced: %s\n", item.(string)) // Log the produced item
}
