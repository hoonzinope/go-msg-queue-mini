package internal

import (
	"context"
	"fmt"
	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/util"
	"time"
)

type StatMonitor struct {
	Queue internal.Queue
}

func (s *StatMonitor) MonitoringStatus(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			util.Info("Monitoring stopped.")
			return
		case <-ticker.C:
			status, err := s.Queue.Status()
			if err != nil {
				util.Error(fmt.Sprintf("Error getting queue status: %v", err))
			}
			util.Info("--- Queue Status ---")
			util.Info(fmt.Sprintf("Total Messages: %d", status.TotalMessages))
			util.Info(fmt.Sprintf("Acked Messages: %d", status.AckedMessages))
			util.Info(fmt.Sprintf("Inflight Messages: %d", status.InflightMessages))
			util.Info(fmt.Sprintf("DLQ Messages: %d", status.DLQMessages))
			util.Info("---------------------")
		}
	}
}
