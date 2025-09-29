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
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/joho/godotenv"
)

var ctx, cancel = context.WithCancel(context.Background())
var logger *slog.Logger
var queue internal.Queue

func main() {

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	config, err := readConfig("config.yml", ".env")
	if err != nil {
		log.Fatalf("Error reading config: %v", err)
		return
	}
	loggerType := "json"
	if config.Debug {
		loggerType = "text"
	} else {
		loggerType = "json"
	}
	logger = util.InitLogger(loggerType)

	queue, err = buildQueue(config, logger)
	if err != nil {
		logger.Error("Error initializing queue", "error", err)
		return
	}
	logger.Info("Starting message queue")

	wg := sync.WaitGroup{}
	if config.Debug {
		client := &client.QueueClient{
			Queue:     queue,
			QueueName: "default",
			Logger:    logger,
		}
		var groupName string = "default"
		err := client.Queue.CreateQueue(client.QueueName)
		if err != nil {
			logger.Error("Error creating queue", "error", err)
			return
		}
		debugMode(&wg, ctx, client, groupName)
	} else {
		if config.HTTP.Enabled {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := http.StartServer(ctx, config, queue, logger)
				if err != nil {
					logger.Error("Error starting HTTP server", "error", err)
				}
			}()
		}

		if config.GRPC.Enabled {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := grpc.StartServer(ctx, config, queue, logger)
				if err != nil {
					logger.Error("Error starting gRPC server", "error", err)
				}
			}()
		}
	}

	logger.Info("Message queue is running. Press Ctrl+C to stop.")

	<-quit
	logger.Info("Shutting down...")
	cancel()  // Cancel the context to stop all goroutines
	wg.Wait() // Wait for all goroutines to finish
	if err := queue.Shutdown(); err != nil {
		logger.Error("Error shutting down queue", "error", err)
	}
	logger.Info("Message queue stopped.")
}

func readConfig(config_path, env_path string) (*internal.Config, error) {
	err := godotenv.Load(env_path)
	if err != nil {
		return nil, err
	}
	config, err := internal.ReadConfig(config_path)
	if err != nil {
		return nil, err
	}
	return config, nil
}

func buildQueue(config *internal.Config, logger *slog.Logger) (internal.Queue, error) {
	switch config.Persistence.Type {
	case "", "memory":
		return fileDBQueue.NewFileDBQueue(config, logger)
	case "file":
		return fileDBQueue.NewFileDBQueue(config, logger)
	default:
		logger.Error("Unsupported persistence type", "type", config.Persistence.Type)
		return nil, fmt.Errorf("unsupported persistence type: %s", config.Persistence.Type)
	}
}

func debugMode(wg *sync.WaitGroup, ctx context.Context, client *client.QueueClient, groupName string) {
	logger.Info("Debug mode is enabled")

	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			consumer := runner.Consumer{
				Name:   fmt.Sprintf("consumer_%d", i),
				Group:  groupName,
				Client: client,
			}
			consumer.Consume(ctx)
		}(i)
	}

	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			producer := runner.Producer{
				Name:   fmt.Sprintf("producer_%d", i),
				Client: client,
			}
			producer.Produce(ctx)
		}(i)
	}
}
