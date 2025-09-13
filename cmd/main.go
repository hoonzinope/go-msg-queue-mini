package main

import (
	"context"
	"fmt"
	"go-msg-queue-mini/client"
	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/internal/api/grpc"
	"go-msg-queue-mini/internal/api/http"
	fileDBQueue "go-msg-queue-mini/internal/core"
	runner "go-msg-queue-mini/internal/runner"
	"go-msg-queue-mini/util"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/joho/godotenv"
)

var ctx, cancel = context.WithCancel(context.Background())
var queue internal.Queue

func main() {
	util.Info("Starting message queue...")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	config, err := readConfig("config.yml", ".env")
	if err != nil {
		util.Error(fmt.Sprintf("Error reading config: %v", err))
		return
	}
	queue, err = buildQueue(config)
	if err != nil {
		util.Error(fmt.Sprintf("Error initializing queue: %v", err))
		return
	}

	wg := sync.WaitGroup{}
	client := &client.QueueClient{
		Queue:     queue,
		QueueName: "default",
	}
	if config.Debug {
		var group_name string = "default"
		err := client.Queue.CreateQueue(client.QueueName)
		if err != nil {
			util.Error(fmt.Sprintf("Error creating queue: %v", err))
			return
		}
		debugMode(&wg, ctx, client, group_name)
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

		if config.GRPC.Enabled {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := grpc.StartServer(ctx, config, queue)
				if err != nil {
					util.Error(fmt.Sprintf("Error starting gRPC server: %v", err))
				}
			}()
		}
	}

	util.Info("Message queue is running. Press Ctrl+C to stop.")

	<-quit
	util.Info("Shutting down...")
	cancel()  // Cancel the context to stop all goroutines
	wg.Wait() // Wait for all goroutines to finish
	if err := queue.Shutdown(); err != nil {
		util.Error(fmt.Sprintf("Error shutting down queue: %v", err))
	}
	util.Info("Message queue stopped.")
}

func readConfig(config_path, env_path string) (*internal.Config, error) {
	err := godotenv.Load(env_path)
	if err != nil {
		util.Error(fmt.Sprintf("Error loading .env file: %v", err))
		return nil, err
	}
	config, err := internal.ReadConfig(config_path)
	if err != nil {
		util.Error(fmt.Sprintf("Error reading config: %v", err))
		return nil, err
	}
	return config, nil
}

func buildQueue(config *internal.Config) (internal.Queue, error) {
	switch config.Persistence.Type {
	case "", "memory":
		return fileDBQueue.NewFileDBQueue(config)
	case "file":
		return fileDBQueue.NewFileDBQueue(config)
	default:
		return nil, fmt.Errorf("unsupported persistence type: %s", config.Persistence.Type)
	}
}

func debugMode(wg *sync.WaitGroup, ctx context.Context, client *client.QueueClient, group_name string) {
	util.Info("Debug mode is enabled")

	wg.Add(1)
	go func() {
		defer wg.Done()
		producer := runner.Producer{
			Name:   "producer_1",
			Client: client,
		}
		producer.Produce(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		producer := runner.Producer{
			Name:   "producer_2",
			Client: client,
		}
		producer.Produce(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		consumer := runner.Consumer{
			Name:   "consumer_1",
			Group:  group_name,
			Client: client,
		}
		consumer.Consume(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		consumer := runner.Consumer{
			Name:   "consumer_2",
			Group:  group_name,
			Client: client,
		}
		consumer.Consume(ctx)
	}()
}
