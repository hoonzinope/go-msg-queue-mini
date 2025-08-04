package internal

import (
	"context"
	"fmt"
	"time"
)

func MonitoringStatus(ctx context.Context, queue *Queue) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Monitoring stopped.")
			return
		case <-ticker.C:
			fmt.Printf("-----Queue Length: %d-----\n", queue.Length())
		}
	}
}
