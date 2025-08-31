package internal

import (
	"context"
	"fmt"
	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/util"
	"time"
)

type Producer struct {
	Name  string
	Queue internal.Queue
}

func (p *Producer) Produce(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			item := util.GenerateItem() // Generate a new item
			p._produce(item)
			second := util.GenerateNumber(1, 5)             // Generate a random number between 1 and 5
			time.Sleep(time.Duration(second) * time.Second) // Simulate processing time
		}
	}
}

func (p *Producer) _produce(item interface{}) {
	p.Queue.Enqueue(item)                                               // Enqueue the item
	util.Info(fmt.Sprintf("Produced by %s: %s", p.Name, item.(string))) // Log the produced item
}
