package internal

import (
	"context"
	"fmt"
	"go-msg-queue-mini/client"
	"go-msg-queue-mini/util"
	"time"
)

type Producer struct {
	Name   string
	Client *client.QueueClient
}

func (p *Producer) Produce(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			item := util.GenerateItem() // Generate a new item
			p.produce(item)
			second := util.GenerateNumber(1, 5)             // Generate a random number between 1 and 5
			time.Sleep(time.Duration(second) * time.Second) // Simulate processing time
		}
	}
}

func (p *Producer) produce(item interface{}) {
	err := p.Client.Produce(item)
	if err != nil {
		util.Error(fmt.Sprintf("Error producing item by %s: %v", p.Name, err))
		return
	}
	util.Info(fmt.Sprintf("Produced by %s: %s", p.Name, item.(string))) // Log the produced item
}
