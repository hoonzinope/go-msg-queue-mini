package main

import (
	"context"
	"fmt"
	"go-msg-queue-mini/internal"
	"sync"
)

var ctx = context.Background()

func main() {
	fmt.Println("Starting message queue...")
	// Create a new queue
	queue := internal.NewQueue()

	wg := sync.WaitGroup{}
	defer wg.Wait()
	wg.Add(1)
	go func() {
		defer wg.Done()
		internal.Consume(ctx, queue) // Start consuming messages
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		internal.Produce(ctx, queue) // Start producing messages
	}()

	fmt.Println("Message queue is running. Press Ctrl+C to stop.")
	// Wait for the context to be done (in a real application, you would handle this with a signal handler)
	<-ctx.Done()
	fmt.Println("Stopping message queue...")
	fmt.Println("Message queue stopped.")
	wg.Done()
}
