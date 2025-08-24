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
			fmt.Printf("Total Messages: %d\n", status.TotalMessages)
			fmt.Printf("Acked Messages: %d\n", status.AckedMessages)
			fmt.Printf("Inflight Messages: %d\n", status.InflightMessages)
			fmt.Printf("DLQ Messages: %d\n", status.DLQMessages)
			fmt.Printf("---------------------\n")
		}
	}
}
