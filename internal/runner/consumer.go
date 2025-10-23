package internal

import (
	"context"
	"errors"
	"go-msg-queue-mini/client"
	"go-msg-queue-mini/internal/queue_error"
	"go-msg-queue-mini/util"
	"log/slog"
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
	logger := c.Client.Logger
	err := c.Client.Consume(c.Group, c.Name, func(item []byte) error {
		// Simulate message processing
		processTime := util.GenerateNumber(1, 2)
		time.Sleep(time.Duration(processTime) * time.Second)
		// Randomly simulate processing failure
		if util.GenerateNumber(1, 10) > 8 {
			return errors.New("simulated processing error")
		}
		logger.Info("Consumed message", slog.String("consumer", c.Name), slog.String("message", string(item)))
		return nil
	})
	if err != nil {
		switch err {
		case queue_error.ErrEmpty:
			// pass
		case queue_error.ErrContended:
			logger.Error("Error consuming item", slog.String("consumer", c.Name), slog.String("error", err.Error()))
		default:
			logger.Error("Error consuming item", slog.String("consumer", c.Name), slog.String("error", err.Error()))
		}
		sleepTime := util.GenerateNumber(1, 2)
		time.Sleep(time.Duration(sleepTime) * time.Second)
	}
}
