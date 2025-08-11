package main

import (
	"context"
	"fmt"
	"go-msg-queue-mini/internal"
	filequeue "go-msg-queue-mini/internal/queueType/file"
	memoryqueue "go-msg-queue-mini/internal/queueType/memory"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var ctx, cancel = context.WithCancel(context.Background())
var queue internal.Queue

func main() {
	fmt.Println("Starting message queue...")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	config, err := internal.ReadConfig("config.yml")
	if err != nil {
		fmt.Printf("Error reading config: %v\n", err)
		return
	}
	// Create a new queue
	if config.Persistence.Type == "" || config.Persistence.Type == "memory" {
		queue = memoryqueue.NewMemoryQueue()
	} else if config.Persistence.Type == "file" {
		logdirs := config.Persistence.Options.DirsPath
		maxSize := config.Persistence.Options.MaxSize
		maxAge := config.Persistence.Options.MaxAge
		queue, err = filequeue.NewFileQueue(logdirs, maxSize, maxAge)
		if err != nil {
			fmt.Printf("Error initializing file queue: %v\n", err)
			return
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		internal.Consume(ctx, queue, "consumer1") // Start consuming messages
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		internal.Consume(ctx, queue, "consumer2") // Start consuming messages
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		internal.Produce(ctx, queue, "producer1") // Start producing messages
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		internal.Produce(ctx, queue, "producer2") // Start producing messages
	}()

	fmt.Println("Message queue is running. Press Ctrl+C to stop.")

	wg.Add(1)
	go func() {
		defer wg.Done()
		internal.MonitoringStatus(ctx, queue) // Start monitoring the queue status
	}()

	<-quit
	fmt.Println("Stopping message queue...")
	cancel()  // Cancel the context to stop all goroutines
	wg.Wait() // Wait for all goroutines to finish
	fmt.Println("Message queue stopped.")
}
