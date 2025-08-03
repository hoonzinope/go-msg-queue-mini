package internal

import (
	"context"
	"fmt"
	"go-msg-queue-mini/util"
	"time"
)

func Produce(ctx context.Context, queue *Queue, name string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			item := util.GenerateItem() // Generate a new item
			_produce(item, queue, name)
			second := util.GenerateNumber(100, 300)              // Generate a random number between 100 and 300
			time.Sleep(time.Duration(second) * time.Millisecond) // Simulate processing time
		}
	}
}

func _produce(item interface{}, queue *Queue, name string) {
	queue.Enqueue(item)                                     // Enqueue the item
	fmt.Printf("Produced by %s: %s\n", name, item.(string)) // Log the produced item
}
