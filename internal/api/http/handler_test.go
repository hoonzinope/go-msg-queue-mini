package http

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/internal/queue_error"
	"log/slog"
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

func (m *mockQueue) Status(string) (internal.QueueStatus, error) { return internal.QueueStatus{}, nil }

func (m *mockQueue) Shutdown() error { return nil }

func (m *mockQueue) Peek(string, string) (internal.QueueMessage, error) {
	return internal.QueueMessage{}, queue_error.ErrEmpty
}

func (m *mockQueue) Renew(string, string, int64, string, int) error { return nil }

func newTestHTTPServer(queue internal.Queue) *httpServerInstance {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &httpServerInstance{Queue: queue, Logger: logger}
}

func TestEnqueueBatchHandlerSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mq := &mockQueue{enqueueBatchResult: 2}
	server := newTestHTTPServer(mq)

	body := EnqueueBatchRequest{
		Mode: "stopOnFailure",
		Messages: []EnqueueMessage{
			{Message: json.RawMessage(`{"foo":"bar"}`), DeduplicationID: "dedup-1"},
			{Message: json.RawMessage(`42`), DeduplicationID: "dedup-2"},
		},
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("queue_name", "test-queue")
	req := httptest.NewRequest(http.MethodPost, "/api/v1/test-queue/enqueue/batch", bytes.NewReader(encoded))
	req.Header.Set("Content-Type", "application/json")
	c.Request = req

	server.enqueueBatchHandler(c)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusAccepted)
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
	expectedItems := []EnqueueMessage{
		{Message: json.RawMessage(`{"foo":"bar"}`), DeduplicationID: "dedup-1"},
		{Message: json.RawMessage(`42`), DeduplicationID: "dedup-2"},
	}
	if len(call.items) != len(expectedItems) {
		t.Fatalf("items length = %d, want %d", len(call.items), len(expectedItems))
	}
	for i, item := range call.items {
		exp := expectedItems[i]
		itemBytes, _ := json.Marshal(item.Item)
		if !bytes.Equal(itemBytes, exp.Message) {
			t.Fatalf("item %d = %s, want %s", i, itemBytes, exp.Message)
		}
		if item.DeduplicationID != exp.DeduplicationID {
			t.Fatalf("dedup id %d = %s, want %s", i, item.DeduplicationID, exp.DeduplicationID)
		}
	}

	var resp EnqueueBatchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.Status != "enqueued" {
		t.Fatalf("response status = %s, want enqueued", resp.Status)
	}
	if resp.SuccessCount != mq.enqueueBatchResult {
		t.Fatalf("success count = %d, want %d", resp.SuccessCount, mq.enqueueBatchResult)
	}

	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.Status != "enqueued" {
		t.Fatalf("response status = %s, want enqueued", resp.Status)
	}
	if resp.SuccessCount != mq.enqueueBatchResult {
		t.Fatalf("success count = %d, want %d", resp.SuccessCount, mq.enqueueBatchResult)
	}
}

func TestEnqueueBatchHandlerPartialSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	failed := []internal.FailedMessage{{Index: 1, Message: json.RawMessage(`"bad"`), Reason: "duplicate message"}}
	mq := &mockQueue{
		enqueueBatchResponse: &internal.BatchResult{
			SuccessCount:   1,
			FailedCount:    1,
			FailedMessages: failed,
		},
	}
	server := newTestHTTPServer(mq)

	enqueueMsgs := []EnqueueMessage{
		{Message: json.RawMessage(`"good"`), DeduplicationID: "dedup-good"},
		{Message: json.RawMessage(`"bad"`), DeduplicationID: "dedup-bad"},
	}

	body := EnqueueBatchRequest{Mode: "partialSuccess", Messages: enqueueMsgs}
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("queue_name", "partial-queue")
	req := httptest.NewRequest(http.MethodPost, "/api/v1/partial-queue/enqueue/batch", bytes.NewReader(encoded))
	req.Header.Set("Content-Type", "application/json")
	c.Request = req

	server.enqueueBatchHandler(c)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusAccepted)
	}

	if len(mq.enqueueBatchCalls) != 1 {
		t.Fatalf("enqueue batch call count = %d, want 1", len(mq.enqueueBatchCalls))
	}
	call := mq.enqueueBatchCalls[0]
	if call.mode != "partialSuccess" {
		t.Fatalf("mode = %s, want partialSuccess", call.mode)
	}
	if len(call.items) != len(enqueueMsgs) {
		t.Fatalf("items length = %d, want %d", len(call.items), len(enqueueMsgs))
	}
	for i, item := range call.items {
		if item.DeduplicationID != enqueueMsgs[i].DeduplicationID {
			t.Fatalf("dedup id %d = %s, want %s", i, item.DeduplicationID, enqueueMsgs[i].DeduplicationID)
		}
	}

	var resp EnqueueBatchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.FailureCount != 1 {
		t.Fatalf("failure count = %d, want 1", resp.FailureCount)
	}
	if len(resp.FailedMessages) != 1 {
		t.Fatalf("failed messages = %d, want 1", len(resp.FailedMessages))
	}
	fm := resp.FailedMessages[0]
	if fm.Index != 1 {
		t.Fatalf("failed message index = %d, want 1", fm.Index)
	}
	if fm.Message != "\"bad\"" {
		t.Fatalf("failed message payload = %s, want \"bad\"", fm.Message)
	}
	if fm.Error != "duplicate message" {
		t.Fatalf("failed message error = %s, want duplicate message", fm.Error)
	}
}

func TestEnqueueBatchHandlerBindError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mq := &mockQueue{}
	server := newTestHTTPServer(mq)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("queue_name", "oops-queue")
	req := httptest.NewRequest(http.MethodPost, "/api/v1/oops-queue/enqueue/batch", bytes.NewBufferString(`{"invalid":true}`))
	req.Header.Set("Content-Type", "application/json")
	c.Request = req

	server.enqueueBatchHandler(c)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
	if len(mq.enqueueBatchCalls) != 0 {
		t.Fatalf("enqueue batch should not be called on bind error")
	}
}

func TestEnqueueBatchHandlerQueueError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mq := &mockQueue{enqueueBatchError: errors.New("boom")}
	server := newTestHTTPServer(mq)

	enqueueMsgs := []EnqueueMessage{
		{Message: json.RawMessage(`"msg"`)},
	}
	body := EnqueueBatchRequest{Mode: "stopOnFailure", Messages: enqueueMsgs}
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("queue_name", "err-queue")
	req := httptest.NewRequest(http.MethodPost, "/api/v1/err-queue/enqueue/batch", bytes.NewReader(encoded))
	req.Header.Set("Content-Type", "application/json")
	c.Request = req

	server.enqueueBatchHandler(c)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusInternalServerError)
	}
	if len(mq.enqueueBatchCalls) != 1 {
		t.Fatalf("enqueue batch call count = %d, want 1", len(mq.enqueueBatchCalls))
	}
}
