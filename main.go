package main

import (
	"context"
	"fmt"
	"go-msg-queue-mini/internal"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)

func main() {
	fmt.Println("Starting message queue...")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	// Create a new queue
	queue := internal.NewQueue()

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

	<-quit
	fmt.Println("Stopping message queue...")
	cancel()  // Cancel the context to stop all goroutines
	wg.Wait() // Wait for all goroutines to finish
	fmt.Println("Message queue stopped.")
}
