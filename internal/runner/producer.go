package internal

import (
	"context"
	"go-msg-queue-mini/client"
	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/util"
	"log/slog"
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
			enqueueMsg := internal.EnqueueMessage{ // Generate a new item
				Item:  util.GenerateItem(),
				Delay: "0s", // Default delay
			}
			p.produce(enqueueMsg)
			second := util.GenerateNumber(1, 5)             // Generate a random number between 1 and 5
			time.Sleep(time.Duration(second) * time.Second) // Simulate processing time
		}
	}
}

func (p *Producer) produce(enqueueMsg internal.EnqueueMessage) {
	logger := p.Client.Logger
	err := p.Client.Produce(enqueueMsg)
	if err != nil {
		logger.Error("Error producing item", slog.String("producer", p.Name), slog.String("error", err.Error()))
		return
	}
	logger.Info("Produced item", slog.String("producer", p.Name), slog.Any("item", enqueueMsg.Item))
}
