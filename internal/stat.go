package internal

import (
	"context"
	"fmt"
	"time"
)

func MonitoringStatus(ctx context.Context, queue Queue) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Monitoring stopped.")
			return
		case <-ticker.C:
			status, err := queue.Status()
			if err != nil {
				fmt.Printf("Error fetching queue status: %v\n", err)
			}
			fmt.Printf("--- Queue Status ---\n")
			fmt.Printf("Queue Type: %s\n", status.QueueType)
			fmt.Printf("Active Consumers: %d\n", status.ActiveConsumers)
			for key, info := range status.ExtraInfo {
				fmt.Printf("Extra Info - %s: %v\n", key, info)
			}
			fmt.Printf("Consumer Statuses:\n")
			for consumerID, consumerStatus := range status.ConsumerStatuses {
				fmt.Printf("  - Consumer ID: %s, Last Offset: %d, Lag: %d\n",
					consumerID, consumerStatus.LastOffset, consumerStatus.Lag)
			}
			fmt.Printf("---------------------\n")
		}
	}
}
