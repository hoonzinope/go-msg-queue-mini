package grpc

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"testing"

	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/internal/queue_error"
)

type enqueueBatchCall struct {
	queueName string
	mode      string
	items     []internal.EnqueueMessage
}

type mockQueue struct {
	enqueueBatchResult   int64
	enqueueBatchError    error
	enqueueBatchCalls    []enqueueBatchCall
	enqueueBatchResponse *internal.BatchResult
}

func (m *mockQueue) CreateQueue(string) error { return nil }

func (m *mockQueue) DeleteQueue(string) error { return nil }

func (m *mockQueue) Enqueue(string, internal.EnqueueMessage) error { return nil }

func (m *mockQueue) EnqueueBatch(queueName, mode string, items []internal.EnqueueMessage) (internal.BatchResult, error) {
	m.enqueueBatchCalls = append(m.enqueueBatchCalls, enqueueBatchCall{queueName: queueName, mode: mode, items: items})
	if m.enqueueBatchResponse != nil {
		return *m.enqueueBatchResponse, m.enqueueBatchError
	}
	return internal.BatchResult{
		SuccessCount:   m.enqueueBatchResult,
		FailedCount:    0,
		FailedMessages: nil,
	}, m.enqueueBatchError
}

func (m *mockQueue) Dequeue(string, string, string) (internal.QueueMessage, error) {
	return internal.QueueMessage{}, queue_error.ErrEmpty
}

func (m *mockQueue) Ack(string, string, int64, string) error { return nil }

func (m *mockQueue) Nack(string, string, int64, string) error { return nil }

func (m *mockQueue) Status(string) (internal.QueueStatus, error) {
	return internal.QueueStatus{}, nil
}

func (m *mockQueue) StatusAll() (map[string]internal.QueueStatus, error) {
	return map[string]internal.QueueStatus{}, nil
}

func (m *mockQueue) Peek(string, string, internal.PeekOptions) ([]internal.QueueMessage, error) {
	return nil, queue_error.ErrEmpty
}

func (m *mockQueue) Shutdown() error { return nil }

func (m *mockQueue) Renew(string, string, int64, string, int) error { return nil }

func newTestGRPCServer(queue internal.Queue) *queueServiceServer {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewQueueServiceServer(queue, logger)
}

type mockQueueInspector struct {
	statusResult    internal.QueueStatus
	statusError     error
	statusAllResult map[string]internal.QueueStatus
	statusAllError  error
	peekMessages    []internal.QueueMessage
	peekError       error
	peekCalls       []peekCall
}

type peekCall struct {
	queueName string
	group     string
	options   internal.PeekOptions
}

func (m *mockQueueInspector) Status(string) (internal.QueueStatus, error) {
	return m.statusResult, m.statusError
}

func (m *mockQueueInspector) StatusAll() (map[string]internal.QueueStatus, error) {
	return m.statusAllResult, m.statusAllError
}

func (m *mockQueueInspector) Peek(queueName, group string, options internal.PeekOptions) ([]internal.QueueMessage, error) {
	m.peekCalls = append(m.peekCalls, peekCall{
		queueName: queueName,
		group:     group,
		options:   options,
	})
	if m.peekError != nil {
		return nil, m.peekError
	}
	return m.peekMessages, nil
}

func TestQueueServiceEnqueueBatchSuccess(t *testing.T) {
	mq := &mockQueue{enqueueBatchResult: 2}
	server := newTestGRPCServer(mq)

	req := &EnqueueBatchRequest{
		QueueName: "test-queue",
		Mode:      "stopOnFailure",
		Messages: []*EnqueueMessage{
			{Message: []byte("first"), DeduplicationId: "dedup-1"},
			{Message: []byte("second"), DeduplicationId: "dedup-2"},
		},
	}

	resp, err := server.EnqueueBatch(context.Background(), req)
	if err != nil {
		t.Fatalf("enqueue batch returned error: %v", err)
	}
	if resp.GetStatus() != "ok" {
		t.Fatalf("response status = %s, want ok", resp.GetStatus())
	}

	if resp.GetQueueName() != "test-queue" {
		t.Fatalf("queue name = %s, want test-queue", resp.GetQueueName())
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
	if call.mode != "stopOnFailure" {
		t.Fatalf("mode = %s, want stopOnFailure", call.mode)
	}
	expectedItems := []internal.EnqueueMessage{
		{Item: []byte("first"), DeduplicationID: "dedup-1"},
		{Item: []byte("second"), DeduplicationID: "dedup-2"},
	}
	if !reflect.DeepEqual(call.items, expectedItems) {
		t.Fatalf("enqueue items = %#v, want %#v", call.items, expectedItems)
	}
}

func TestQueueServiceEnqueueBatchPartialSuccess(t *testing.T) {
	mq := &mockQueue{
		enqueueBatchResponse: &internal.BatchResult{
			SuccessCount: 1,
			FailedCount:  1,
			FailedMessages: []internal.FailedMessage{
				{Index: 2, Message: []byte("bad"), Reason: "duplicate"},
			},
		},
	}
	server := newTestGRPCServer(mq)

	req := &EnqueueBatchRequest{
		QueueName: "test-queue",
		Mode:      "partialSuccess",
		Messages:  []*EnqueueMessage{{Message: []byte("first"), DeduplicationId: "dup-1"}, {Message: []byte("second"), DeduplicationId: "dup-2"}, {Message: []byte("third"), DeduplicationId: "dup-3"}},
	}

	resp, err := server.EnqueueBatch(context.Background(), req)
	if err != nil {
		t.Fatalf("enqueue batch returned error: %v", err)
	}
	if resp.GetFailureCount() != 1 {
		t.Fatalf("failure count = %d, want 1", resp.GetFailureCount())
	}
	if len(resp.GetFailedMessages()) != 1 {
		t.Fatalf("failed messages = %d, want 1", len(resp.GetFailedMessages()))
	}
	fm := resp.GetFailedMessages()[0]
	if fm.GetIndex() != 2 {
		t.Fatalf("failed message index = %d, want 2", fm.GetIndex())
	}
	if string(fm.GetMessage()) != "bad" {
		t.Fatalf("failed message payload = %s, want bad", fm.GetMessage())
	}
	if fm.GetError() != "duplicate" {
		t.Fatalf("failed message error = %s, want duplicate", fm.GetError())
	}
	if len(mq.enqueueBatchCalls) != 1 {
		t.Fatalf("enqueue batch call count = %d, want 1", len(mq.enqueueBatchCalls))
	}
	call := mq.enqueueBatchCalls[0]
	expectedItems := []internal.EnqueueMessage{
		{Item: []byte("first"), DeduplicationID: "dup-1"},
		{Item: []byte("second"), DeduplicationID: "dup-2"},
		{Item: []byte("third"), DeduplicationID: "dup-3"},
	}
	if !reflect.DeepEqual(call.items, expectedItems) {
		t.Fatalf("enqueue items = %#v, want %#v", call.items, expectedItems)
	}
}

func TestQueueServiceEnqueueBatchMissingQueueName(t *testing.T) {
	mq := &mockQueue{}
	server := newTestGRPCServer(mq)

	req := &EnqueueBatchRequest{QueueName: "", Mode: "stopOnFailure", Messages: []*EnqueueMessage{{Message: []byte("msg")}}}

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
	server := newTestGRPCServer(mq)

	req := &EnqueueBatchRequest{QueueName: "test-queue", Mode: "stopOnFailure", Messages: []*EnqueueMessage{}}

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
	server := newTestGRPCServer(mq)

	req := &EnqueueBatchRequest{QueueName: "test-queue", Mode: "stopOnFailure", Messages: []*EnqueueMessage{{Message: []byte("msg")}}}

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

func TestQueueServiceStatusAllSuccess(t *testing.T) {
	mq := &mockQueue{}
	inspector := &mockQueueInspector{
		statusAllResult: map[string]internal.QueueStatus{
			"alpha": {
				QueueType:        "memory",
				QueueName:        "alpha",
				TotalMessages:    5,
				AckedMessages:    2,
				InflightMessages: 1,
				DLQMessages:      2,
			},
			"beta": {
				QueueType:        "memory",
				QueueName:        "beta",
				TotalMessages:    0,
				AckedMessages:    0,
				InflightMessages: 0,
				DLQMessages:      0,
			},
		},
	}
	server := newTestGRPCServer(mq)
	server.QueueInspector = inspector

	resp, err := server.StatusAll(context.Background(), &EmptyRequest{})
	if err != nil {
		t.Fatalf("statusAll returned error: %v", err)
	}
	if resp.GetStatus() != "ok" {
		t.Fatalf("response status = %s, want ok", resp.GetStatus())
	}

	queueStatuses := resp.GetQueueStatuses()
	if len(queueStatuses) != 2 {
		t.Fatalf("queue statuses length = %d, want 2", len(queueStatuses))
	}
	alphaStatus, ok := queueStatuses["alpha"]
	if !ok {
		t.Fatalf("missing alpha status in response")
	}
	if alphaStatus.GetTotalMessages() != 5 || alphaStatus.GetAckedMessages() != 2 || alphaStatus.GetInflightMessages() != 1 || alphaStatus.GetDlqMessages() != 2 {
		t.Fatalf("unexpected alpha status: %#v", alphaStatus)
	}
}

func TestQueueServiceStatusAllError(t *testing.T) {
	mq := &mockQueue{}
	inspector := &mockQueueInspector{
		statusAllError: errors.New("boom"),
	}
	server := newTestGRPCServer(mq)
	server.QueueInspector = inspector

	resp, err := server.StatusAll(context.Background(), &EmptyRequest{})
	if err == nil {
		t.Fatalf("expected error from statusAll")
	}
	if resp != nil {
		t.Fatalf("expected nil response on error")
	}
	if !errors.Is(err, inspector.statusAllError) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueueServicePeekSuccess(t *testing.T) {
	mq := &mockQueue{}
	server := newTestGRPCServer(mq)
	inspector := &mockQueueInspector{
		peekMessages: []internal.QueueMessage{
			{ID: 201, Payload: []byte("alpha"), Receipt: "ra"},
			{ID: 202, Payload: []byte("beta"), Receipt: "rb"},
		},
	}
	server.QueueInspector = inspector

	req := &PeekRequest{
		QueueName: "tasks",
		Group:     "group-A",
		Options: &PeekOptions{
			Limit:  2,
			Cursor: 10,
			Order:  "desc",
		},
	}

	resp, err := server.Peek(context.Background(), req)
	if err != nil {
		t.Fatalf("peek returned error: %v", err)
	}
	if resp.GetStatus() != "ok" {
		t.Fatalf("response status = %s, want ok", resp.GetStatus())
	}
	if len(resp.GetMessage()) != 2 {
		t.Fatalf("message count = %d, want 2", len(resp.GetMessage()))
	}
	first := resp.GetMessage()[0]
	if first.GetId() != 201 || first.GetReceipt() != "ra" || string(first.GetPayload()) != "alpha" {
		t.Fatalf("unexpected first message: %#v", first)
	}
	if len(inspector.peekCalls) != 1 {
		t.Fatalf("peek call count = %d, want 1", len(inspector.peekCalls))
	}
	call := inspector.peekCalls[0]
	if call.queueName != "tasks" {
		t.Fatalf("queue name = %s, want tasks", call.queueName)
	}
	if call.group != "group-A" {
		t.Fatalf("group = %s, want group-A", call.group)
	}
	if call.options.Limit != 2 || call.options.Cursor != 10 || call.options.Order != "desc" {
		t.Fatalf("unexpected peek options: %+v", call.options)
	}
}

func TestQueueServicePeekEmptyQueue(t *testing.T) {
	mq := &mockQueue{}
	server := newTestGRPCServer(mq)
	inspector := &mockQueueInspector{
		peekError: queue_error.ErrNoMessage,
	}
	server.QueueInspector = inspector

	req := &PeekRequest{
		QueueName: "tasks",
		Group:     "group-A",
		Options:   &PeekOptions{},
	}

	resp, err := server.Peek(context.Background(), req)
	if err == nil {
		t.Fatalf("expected error for empty queue")
	}
	if !errors.Is(err, queue_error.ErrNoMessage) {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != nil {
		t.Fatalf("expected nil response on error")
	}
	if len(inspector.peekCalls) != 1 {
		t.Fatalf("peek call count = %d, want 1", len(inspector.peekCalls))
	}
}
