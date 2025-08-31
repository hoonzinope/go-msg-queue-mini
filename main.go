package main

import (
	"context"
	"fmt"
	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/internal/api/http"
	fileDBQueue "go-msg-queue-mini/internal/core"
	runner "go-msg-queue-mini/internal/runner"
	"go-msg-queue-mini/util"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var ctx, cancel = context.WithCancel(context.Background())
var queue internal.Queue

func main() {
	util.Info("Starting message queue...")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	config, err := internal.ReadConfig("config.yml")
	if err != nil {
		util.Error(fmt.Sprintf("Error reading config: %v", err))
		return
	}
	// Create a new queue
	switch config.Persistence.Type {
	case "", "memory":
		queue, err = fileDBQueue.NewFileDBQueue(config)
		if err != nil {
			util.Error(fmt.Sprintf("Error initializing file DB queue: %v", err))
			return
		}
	case "file":
		queue, err = fileDBQueue.NewFileDBQueue(config)
		if err != nil {
			util.Error(fmt.Sprintf("Error initializing file queue: %v", err))
			return
		}
	default:
		util.Error(fmt.Sprintf("Unsupported persistence type: %s", config.Persistence.Type))
		return
	}

	var group_name string = "default"
	wg := sync.WaitGroup{}
	if config.Debug {
		util.Info("Debug mode is enabled")
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumer := runner.Consumer{
				Queue:        queue,
				GroupName:    group_name,
				ConsumerName: "consumer_1",
			}
			consumer.Consume(ctx)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			consumer := runner.Consumer{
				Queue:        queue,
				GroupName:    group_name,
				ConsumerName: "consumer_2",
			}
			consumer.Consume(ctx)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			producer := runner.Producer{
				Name:  "producer_1",
				Queue: queue,
			}
			producer.Produce(ctx)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			producer := runner.Producer{
				Name:  "producer_2",
				Queue: queue,
			}
			producer.Produce(ctx)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			statMonitor := runner.StatMonitor{
				Queue: queue,
			}
			statMonitor.MonitoringStatus(ctx) // Start monitoring the queue status
		}()
	} else {
		if config.HTTP.Enabled {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := http.StartServer(ctx, config, queue)
				if err != nil {
					util.Error(fmt.Sprintf("Error starting HTTP server: %v", err))
				}
			}()
		}
	}

	util.Info("Message queue is running. Press Ctrl+C to stop.")

	<-quit
	util.Info("Shutting down...")
	cancel()  // Cancel the context to stop all goroutines
	wg.Wait() // Wait for all goroutines to finish
	queue.Shutdown()
	util.Info("Message queue stopped.")
}
