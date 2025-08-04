package internal

import (
	"context"
	"fmt"
	"time"
)

func MonitoringStatus(ctx context.Context, queue *Queue) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Monitoring stopped.")
			return
		default:
			fmt.Printf("-----Queue Length: %d-----\n", queue.Length())
			time.Sleep(5 * time.Second) // Monitor every 5 seconds
		}
	}
}
