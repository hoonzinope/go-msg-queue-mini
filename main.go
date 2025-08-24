package main

import (
	"context"
	"fmt"
	"go-msg-queue-mini/internal"
	fileDBQueue "go-msg-queue-mini/internal/queueType/fileDB"
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
	switch config.Persistence.Type {
	case "", "memory":
		queue, err = fileDBQueue.NewFileDBQueue(config)
		if err != nil {
			fmt.Printf("Error initializing file DB queue: %v\n", err)
			return
		}
	case "file":
		queue, err = fileDBQueue.NewFileDBQueue(config)
		if err != nil {
			fmt.Printf("Error initializing file queue: %v\n", err)
			return
		}
	default:
		fmt.Printf("Unsupported persistence type: %s\n", config.Persistence.Type)
		return
	}

	var group_name string = "default"
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		internal.Consume(ctx, queue, group_name, "consumer_1") // Start consuming messages
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		internal.Consume(ctx, queue, group_name, "consumer_2") // Start consuming messages
	}()

	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	// 	internal.Consume(ctx, queue, "log", "consumer_1") // Start consuming messages
	// }()

	wg.Add(1)
	go func() {
		defer wg.Done()
		internal.Produce(ctx, queue, group_name) // Start producing messages
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		internal.Produce(ctx, queue, group_name) // Start producing messages
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
