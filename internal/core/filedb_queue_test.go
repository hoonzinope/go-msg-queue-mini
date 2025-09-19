package core

import (
	"errors"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"testing"

	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/internal/queue_error"
)

func newTestConfig() *internal.Config {
	cfg := &internal.Config{
		MaxRetry:      3,
		RetryInterval: "1s",
		LeaseDuration: "30s",
	}
	cfg.Persistence.Type = "memory"
	return cfg
}

func TestFileDBQueueEnqueueBatchSuccess(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	queue, err := NewFileDBQueue(newTestConfig(), logger)
	if err != nil {
		t.Fatalf("failed to create filedb queue: %v", err)
	}
	defer func() {
		if shutdownErr := queue.Shutdown(); shutdownErr != nil {
			t.Fatalf("failed to shutdown queue: %v", shutdownErr)
		}
	}()

	queueName := "enqueue-batch-success"
	if err := queue.CreateQueue(queueName); err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	batch := []interface{}{"first", map[string]interface{}{"foo": "bar"}}
	successCount, err := queue.EnqueueBatch(queueName, batch)
	if err != nil {
		t.Fatalf("enqueue batch returned error: %v", err)
	}
	if successCount != len(batch) {
		t.Fatalf("enqueue batch success count = %d, want %d", successCount, len(batch))
	}

	expected := []interface{}{"first", map[string]interface{}{"foo": "bar"}}
	for idx, want := range expected {
		msg, err := queue.Dequeue(queueName, "group-A", "consumer-1")
		if err != nil {
			t.Fatalf("dequeue failed at index %d: %v", idx, err)
		}
		if !reflect.DeepEqual(msg.Payload, want) {
			t.Fatalf("dequeued payload = %#v, want %#v", msg.Payload, want)
		}
		if ackErr := queue.Ack(queueName, "group-A", msg.ID, msg.Receipt); ackErr != nil {
			t.Fatalf("ack failed for message %d: %v", idx, ackErr)
		}
	}

	if _, err := queue.Dequeue(queueName, "group-A", "consumer-1"); !errors.Is(err, queue_error.ErrEmpty) {
		t.Fatalf("expected empty queue after acking, got error: %v", err)
	}
}

func TestFileDBQueueEnqueueBatchQueueNotFound(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	queue, err := NewFileDBQueue(newTestConfig(), logger)
	if err != nil {
		t.Fatalf("failed to create filedb queue: %v", err)
	}
	defer func() {
		if shutdownErr := queue.Shutdown(); shutdownErr != nil {
			t.Fatalf("failed to shutdown queue: %v", shutdownErr)
		}
	}()

	batch := []interface{}{"no-queue"}
	successCount, err := queue.EnqueueBatch("missing-queue", batch)
	if err == nil {
		t.Fatal("expected error when enqueueing to missing queue, got nil")
	}
	if successCount != 0 {
		t.Fatalf("success count = %d, want 0", successCount)
	}
	if !strings.Contains(err.Error(), "queue not found") {
		t.Fatalf("unexpected error: %v", err)
	}
}
