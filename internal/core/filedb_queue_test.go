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

func TestFileDBQueueEnqueueBatchPartialSuccessDuplicate(t *testing.T) {
	queue := newTestQueue(t)
	queueName := "enqueue-batch-partial-success-duplicate"
	createQueueOrFail(t, queue, queueName)

	batch := []internal.EnqueueMessage{
		{Item: "first-success", Delay: "0s", DeduplicationID: "dedup-shared"},
		{Item: "duplicate-entry", Delay: "0s", DeduplicationID: "dedup-shared"},
		{Item: map[string]interface{}{"payload": "second-success"}, Delay: "0s", DeduplicationID: "dedup-unique"},
	}

	result, err := queue.EnqueueBatch(queueName, "partialSuccess", batch)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if result.SuccessCount != 2 {
		t.Fatalf("success count = %d, want 2", result.SuccessCount)
	}
	if result.FailedCount != 1 {
		t.Fatalf("failed count = %d, want 1", result.FailedCount)
	}
	if len(result.FailedMessages) != 1 {
		t.Fatalf("failed messages len = %d, want 1", len(result.FailedMessages))
	}

	failed := result.FailedMessages[0]
	if failed.Index != 1 {
		t.Fatalf("failed index = %d, want 1", failed.Index)
	}
	msgStr, ok := failed.Message.(string)
	if !ok {
		t.Fatalf("failed message type = %T, want string", failed.Message)
	}
	if msgStr != "duplicate-entry" {
		t.Fatalf("failed message = %s, want %s", msgStr, "duplicate-entry")
	}
	if !strings.Contains(failed.Reason, "duplicate message") {
		t.Fatalf("failed reason = %s, want contains %q", failed.Reason, "duplicate message")
	}

	expected := []interface{}{
		"first-success",
		map[string]interface{}{"payload": "second-success"},
	}
	for idx, want := range expected {
		msg, err := queue.Dequeue(queueName, "group-partial-dup", "consumer-partial-dup")
		if err != nil {
			t.Fatalf("dequeue failed at index %d: %v", idx, err)
		}
		if !reflect.DeepEqual(msg.Payload, want) {
			t.Fatalf("payload = %#v, want %#v", msg.Payload, want)
		}
		if ackErr := queue.Ack(queueName, "group-partial-dup", msg.ID, msg.Receipt); ackErr != nil {
			t.Fatalf("ack failed at index %d: %v", idx, ackErr)
		}
	}

	if _, err := queue.Dequeue(queueName, "group-partial-dup", "consumer-partial-dup"); !errors.Is(err, queue_error.ErrEmpty) {
		t.Fatalf("expected empty queue, got %v", err)
	}
}

func TestFileDBQueueEnqueueDuplicateDedupID(t *testing.T) {
	queue := newTestQueue(t)
	queueName := "enqueue-duplicate-dedup-id"
	createQueueOrFail(t, queue, queueName)

	first := internal.EnqueueMessage{Item: "original", Delay: "0s", DeduplicationID: "dedup-same"}
	if err := queue.Enqueue(queueName, first); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}

	dupErr := queue.Enqueue(queueName, internal.EnqueueMessage{Item: "duplicate", Delay: "0s", DeduplicationID: "dedup-same"})
	if dupErr == nil {
		t.Fatal("expected duplicate enqueue to return error, got nil")
	}
	if !errors.Is(dupErr, queue_error.ErrDuplicate) {
		t.Fatalf("expected duplicate error, got %v", dupErr)
	}

	msg, err := queue.Dequeue(queueName, "group-dup", "consumer-dup")
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	payload, ok := msg.Payload.(string)
	if !ok {
		t.Fatalf("expected payload type string, got %T", msg.Payload)
	}
	if payload != "original" {
		t.Fatalf("payload = %s, want %s", payload, "original")
	}
	if err := queue.Ack(queueName, "group-dup", msg.ID, msg.Receipt); err != nil {
		t.Fatalf("ack failed: %v", err)
	}

	if _, err := queue.Dequeue(queueName, "group-dup", "consumer-dup"); !errors.Is(err, queue_error.ErrEmpty) {
		t.Fatalf("expected queue empty, got %v", err)
	}
}

func TestFileDBQueueEnqueueBatchStopOnFailureDuplicate(t *testing.T) {
	queue := newTestQueue(t)
	queueName := "enqueue-batch-stop-duplicate"
	createQueueOrFail(t, queue, queueName)

	seed := internal.EnqueueMessage{Item: "seed", Delay: "0s", DeduplicationID: "dedup-shared"}
	if err := queue.Enqueue(queueName, seed); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}

	msg, err := queue.Dequeue(queueName, "group-seed", "consumer-seed")
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if err := queue.Ack(queueName, "group-seed", msg.ID, msg.Receipt); err != nil {
		t.Fatalf("ack failed: %v", err)
	}

	batch := []internal.EnqueueMessage{
		{Item: "duplicate", Delay: "0s", DeduplicationID: "dedup-shared"},
		{Item: "should-not-run", Delay: "0s", DeduplicationID: "dedup-new"},
	}

	result, err := queue.EnqueueBatch(queueName, "stopOnFailure", batch)
	if err == nil {
		t.Fatal("expected duplicate error from batch enqueue, got nil")
	}
	if !errors.Is(err, queue_error.ErrDuplicate) {
		t.Fatalf("expected duplicate error, got %v", err)
	}
	if result.SuccessCount != 0 {
		t.Fatalf("success count = %d, want 0", result.SuccessCount)
	}
	if result.FailedCount != int64(len(batch)) {
		t.Fatalf("failed count = %d, want %d", result.FailedCount, len(batch))
	}

	if _, err := queue.Dequeue(queueName, "group-seed", "consumer-seed-2"); !errors.Is(err, queue_error.ErrEmpty) {
		t.Fatalf("expected no new messages for same group, got %v", err)
	}
}

func TestFileDBQueueStatusAll(t *testing.T) {
	queue := newTestQueue(t)
	dataQueue := "status-all-primary"
	emptyQueue := "status-all-empty"

	createQueueOrFail(t, queue, dataQueue)
	createQueueOrFail(t, queue, emptyQueue)

	err := queue.Enqueue(dataQueue, internal.EnqueueMessage{
		Item:            map[string]interface{}{"payload": "value"},
		Delay:           "0s",
		DeduplicationID: "status-dedup-1",
	})
	if err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}

	statusMap, err := queue.StatusAll()
	if err != nil {
		t.Fatalf("statusAll returned error: %v", err)
	}

	if len(statusMap) != 2 {
		t.Fatalf("status map length = %d, want 2", len(statusMap))
	}

	dataStatus, ok := statusMap[dataQueue]
	if !ok {
		t.Fatalf("expected status for queue %s", dataQueue)
	}
	if dataStatus.QueueType != "memory" {
		t.Fatalf("queue type = %s, want memory", dataStatus.QueueType)
	}
	if dataStatus.TotalMessages != 1 {
		t.Fatalf("total messages = %d, want 1", dataStatus.TotalMessages)
	}
	if dataStatus.AckedMessages != 0 {
		t.Fatalf("acked messages = %d, want 0", dataStatus.AckedMessages)
	}
	if dataStatus.InflightMessages != 0 {
		t.Fatalf("inflight messages = %d, want 0", dataStatus.InflightMessages)
	}
	if dataStatus.DLQMessages != 0 {
		t.Fatalf("dlq messages = %d, want 0", dataStatus.DLQMessages)
	}

	emptyStatus, ok := statusMap[emptyQueue]
	if !ok {
		t.Fatalf("expected status for queue %s", emptyQueue)
	}
	if emptyStatus.TotalMessages != 0 {
		t.Fatalf("empty queue total messages = %d, want 0", emptyStatus.TotalMessages)
	}
	if emptyStatus.QueueType != "memory" {
		t.Fatalf("empty queue type = %s, want memory", emptyStatus.QueueType)
	}
}
