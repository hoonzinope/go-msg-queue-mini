package grpc

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/internal/queue_error"
)

type enqueueBatchCall struct {
	queueName string
	items     []interface{}
}

type mockQueue struct {
	enqueueBatchResult int
	enqueueBatchError  error
	enqueueBatchCalls  []enqueueBatchCall
}

func (m *mockQueue) CreateQueue(string) error { return nil }

func (m *mockQueue) DeleteQueue(string) error { return nil }

func (m *mockQueue) Enqueue(string, interface{}) error { return nil }

func (m *mockQueue) EnqueueBatch(queueName string, items []interface{}) (int, error) {
	m.enqueueBatchCalls = append(m.enqueueBatchCalls, enqueueBatchCall{queueName: queueName, items: items})
	return m.enqueueBatchResult, m.enqueueBatchError
}

func (m *mockQueue) Dequeue(string, string, string) (internal.QueueMessage, error) {
	return internal.QueueMessage{}, queue_error.ErrEmpty
}

func (m *mockQueue) Ack(string, string, int64, string) error { return nil }

func (m *mockQueue) Nack(string, string, int64, string) error { return nil }

func (m *mockQueue) Status(string) (internal.QueueStatus, error) {
	return internal.QueueStatus{}, nil
}

func (m *mockQueue) Shutdown() error { return nil }

func (m *mockQueue) Peek(string, string) (internal.QueueMessage, error) {
	return internal.QueueMessage{}, queue_error.ErrEmpty
}

func (m *mockQueue) Renew(string, string, int64, string, int) error { return nil }

func TestQueueServiceEnqueueBatchSuccess(t *testing.T) {
	mq := &mockQueue{enqueueBatchResult: 2}
	server := NewQueueServiceServer(mq)

	req := &EnqueueBatchRequest{
		QueueName: "test-queue",
		Messages:  []string{"first", "second"},
	}

	resp, err := server.EnqueueBatch(context.Background(), req)
	if err != nil {
		t.Fatalf("enqueue batch returned error: %v", err)
	}
	if resp.GetStatus() != "ok" {
		t.Fatalf("response status = %s, want ok", resp.GetStatus())
	}
	if resp.GetSuccessCount() != int64(mq.enqueueBatchResult) {
		t.Fatalf("success count = %d, want %d", resp.GetSuccessCount(), mq.enqueueBatchResult)
	}

	if len(mq.enqueueBatchCalls) != 1 {
		t.Fatalf("enqueue batch call count = %d, want 1", len(mq.enqueueBatchCalls))
	}
	call := mq.enqueueBatchCalls[0]
	if call.queueName != "test-queue" {
		t.Fatalf("queue name = %s, want test-queue", call.queueName)
	}
	expectedItems := []interface{}{"first", "second"}
	if !reflect.DeepEqual(call.items, expectedItems) {
		t.Fatalf("enqueue items = %#v, want %#v", call.items, expectedItems)
	}
}

func TestQueueServiceEnqueueBatchMissingQueueName(t *testing.T) {
	mq := &mockQueue{}
	server := NewQueueServiceServer(mq)

	req := &EnqueueBatchRequest{QueueName: "", Messages: []string{"msg"}}

	resp, err := server.EnqueueBatch(context.Background(), req)
	if err == nil {
		t.Fatalf("expected error when queue name missing")
	}
	if resp != nil {
		t.Fatalf("expected nil response on error")
	}
	if len(mq.enqueueBatchCalls) != 0 {
		t.Fatalf("enqueue batch should not be called when queue name missing")
	}
}

func TestQueueServiceEnqueueBatchEmptyMessages(t *testing.T) {
	mq := &mockQueue{}
	server := NewQueueServiceServer(mq)

	req := &EnqueueBatchRequest{QueueName: "test-queue", Messages: []string{}}

	resp, err := server.EnqueueBatch(context.Background(), req)
	if err == nil {
		t.Fatalf("expected error when messages are empty")
	}
	if resp != nil {
		t.Fatalf("expected nil response on empty messages")
	}
	if len(mq.enqueueBatchCalls) != 0 {
		t.Fatalf("enqueue batch should not be called when messages empty")
	}
}

func TestQueueServiceEnqueueBatchQueueError(t *testing.T) {
	mq := &mockQueue{enqueueBatchError: errors.New("boom")}
	server := NewQueueServiceServer(mq)

	req := &EnqueueBatchRequest{QueueName: "test-queue", Messages: []string{"msg"}}

	resp, err := server.EnqueueBatch(context.Background(), req)
	if err == nil {
		t.Fatalf("expected error from queue")
	}
	if !errors.Is(err, mq.enqueueBatchError) {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != nil {
		t.Fatalf("expected nil response on queue error")
	}
	if len(mq.enqueueBatchCalls) != 1 {
		t.Fatalf("enqueue batch call count = %d, want 1", len(mq.enqueueBatchCalls))
	}
}

