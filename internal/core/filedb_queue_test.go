package core

import (
	"errors"
	"fmt"
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

func newTestQueue(t *testing.T) *fileDBQueue {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	queue, err := NewFileDBQueue(newTestConfig(), logger)
	if err != nil {
		t.Fatalf("failed to create filedb queue: %v", err)
	}
	t.Cleanup(func() {
		if shutdownErr := queue.Shutdown(); shutdownErr != nil {
			t.Fatalf("failed to shutdown queue: %v", shutdownErr)
		}
	})
	return queue
}

func createQueueOrFail(t *testing.T, queue *fileDBQueue, queueName string) {
	t.Helper()
	if err := queue.CreateQueue(queueName); err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
}

func TestFileDBQueueEnqueueBatchSuccess(t *testing.T) {
	queue := newTestQueue(t)
	queueName := "enqueue-batch-success"
	createQueueOrFail(t, queue, queueName)

	mode := "stopOnFailure"
	batchData := make([]internal.EnqueueMessage, 0)
	batchData = append(batchData, internal.EnqueueMessage{Item: "first", Delay: "0s", DeduplicationID: "dedup-1"})
	batchData = append(batchData, internal.EnqueueMessage{Item: map[string]interface{}{"foo": "bar"}, Delay: "0s", DeduplicationID: "dedup-2"})
	batchResult, err := queue.EnqueueBatch(queueName, mode, batchData)
	if err != nil {
		t.Fatalf("enqueue batch returned error: %v", err)
	}
	if batchResult.SuccessCount != int64(len(batchData)) {
		t.Fatalf("enqueue batch success count = %d, want %d", batchResult.SuccessCount, int64(len(batchData)))
	}

	expected := []internal.EnqueueMessage{
		{Item: "first", Delay: "0s", DeduplicationID: "dedup-1"},
		{Item: map[string]interface{}{"foo": "bar"}, Delay: "0s", DeduplicationID: "dedup-2"},
	}
	for idx, want := range expected {
		msg, err := queue.Dequeue(queueName, "group-A", "consumer-1")
		if err != nil {
			t.Fatalf("dequeue failed at index %d: %v", idx, err)
		}
		if !reflect.DeepEqual(msg.Payload, want.Item) {
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
	queue := newTestQueue(t)
	mode := "stopOnFailure"
	batch := []internal.EnqueueMessage{
		{Item: "no-queue", Delay: "0s", DeduplicationID: "dedup-2"},
	}
	batchResult, err := queue.EnqueueBatch("missing-queue", mode, batch)
	if err == nil {
		t.Fatal("expected error when enqueueing to missing queue, got nil")
	}
	if batchResult.SuccessCount != 0 {
		t.Fatalf("success count = %d, want 0", batchResult.SuccessCount)
	}
	if !strings.Contains(err.Error(), "queue not found") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFileDBQueueEnqueueBatchStopOnFailureMarshalError(t *testing.T) {
	queue := newTestQueue(t)
	queueName := "enqueue-batch-stop-on-failure-marshal-error"
	createQueueOrFail(t, queue, queueName)

	invalid := make(chan int)
	batch := []internal.EnqueueMessage{
		{Item: "valid-message", Delay: "0s", DeduplicationID: "dedup-1"},
		{Item: invalid, Delay: "0s", DeduplicationID: "dedup-2"},
		{Item: "another-valid-message", Delay: "0s", DeduplicationID: "dedup-3"},
	}

	result, err := queue.EnqueueBatch(queueName, "stopOnFailure", batch)
	if err == nil {
		t.Fatal("expected marshal error, got nil")
	}
	if !strings.Contains(err.Error(), "json: unsupported type: chan int") {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.SuccessCount != 0 {
		t.Fatalf("success count = %d, want 0", result.SuccessCount)
	}
	if result.FailedCount != int64(len(batch)) {
		t.Fatalf("failed count = %d, want %d", result.FailedCount, len(batch))
	}
	if len(result.FailedMessages) != 0 {
		t.Fatalf("expected no failed messages details, got %d", len(result.FailedMessages))
	}

	if _, err := queue.Dequeue(queueName, "group-stop", "consumer-stop"); !errors.Is(err, queue_error.ErrEmpty) {
		t.Fatalf("expected empty queue after failure, got %v", err)
	}
}

func TestFileDBQueueEnqueueBatchPartialSuccessChunkError(t *testing.T) {
	queue := newTestQueue(t)
	queueName := "enqueue-batch-partial-success-chunk-error"
	createQueueOrFail(t, queue, queueName)

	delay := "0s"
	items := make([]internal.EnqueueMessage, 101)
	for i := 0; i < 100; i++ {
		items[i] = internal.EnqueueMessage{
			Item:            fmt.Sprintf("msg-%03d", i),
			Delay:           delay,
			DeduplicationID: fmt.Sprintf("dedup-%03d", i),
		}
	}
	items[100] = internal.EnqueueMessage{
		Item:            make(chan int),
		Delay:           delay,
		DeduplicationID: "dedup-error",
	}
	result, err := queue.EnqueueBatch(queueName, "partialSuccess", items)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if result.SuccessCount != 100 {
		t.Fatalf("success count = %d, want 100", result.SuccessCount)
	}
	if result.FailedCount != 1 {
		t.Fatalf("failed count = %d, want 1", result.FailedCount)
	}
	if len(result.FailedMessages) != 0 {
		t.Fatalf("expected no failed message details, got %d", len(result.FailedMessages))
	}

	for i := 0; i < 100; i++ {
		msg, err := queue.Dequeue(queueName, "group-partial", "consumer-partial")
		if err != nil {
			t.Fatalf("dequeue failed at index %d: %v", i, err)
		}
		want := fmt.Sprintf("msg-%03d", i)
		payload, ok := msg.Payload.(string)
		if !ok {
			t.Fatalf("expected payload to be string, got %T", msg.Payload)
		}
		if payload != want {
			t.Fatalf("payload = %s, want %s", payload, want)
		}
		if ackErr := queue.Ack(queueName, "group-partial", msg.ID, msg.Receipt); ackErr != nil {
			t.Fatalf("ack failed for message %d: %v", i, ackErr)
		}
	}

	if _, err := queue.Dequeue(queueName, "group-partial", "consumer-partial"); !errors.Is(err, queue_error.ErrEmpty) {
		t.Fatalf("expected empty queue after draining, got %v", err)
	}
}
