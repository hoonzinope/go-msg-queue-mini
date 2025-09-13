package internal

import (
	"context"
	"errors"
	"fmt"
	"go-msg-queue-mini/client"
	"go-msg-queue-mini/internal/queue_error"
	"go-msg-queue-mini/util"
	"time"
)

type Consumer struct {
	Name   string
	Group  string
	Client *client.QueueClient
}

func (c *Consumer) Consume(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			c.consume()
		}
	}
}

func (c *Consumer) consume() {
	err := c.Client.Consume(c.Group, c.Name, func(item interface{}) error {
		// Simulate message processing
		processTime := util.GenerateNumber(1, 2)
		time.Sleep(time.Duration(processTime) * time.Second)
		// Randomly simulate processing failure
		if util.GenerateNumber(1, 10) > 8 {
			return errors.New("simulated processing error")
		}
		util.Info(fmt.Sprintf("Consumed by %s: %s", c.Name, item.(string)))
		return nil
	})
	if err != nil {
		switch err {
		case queue_error.ErrEmpty:
			// pass
		case queue_error.ErrContended:
			util.Error(fmt.Sprintf("Error consuming item by %s: %v", c.Name, err))
		default:
			util.Error(fmt.Sprintf("Error consuming item by %s: %v", c.Name, err))
		}
	}
}
