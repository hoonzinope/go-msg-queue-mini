package http

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/internal/queue_error"
)

type enqueueBatchCall struct {
	queueName string
	items     []interface{}
}

type mockQueue struct {
	enqueueBatchResult int64
	enqueueBatchError  error
	enqueueBatchCalls  []enqueueBatchCall
}

func (m *mockQueue) CreateQueue(string) error { return nil }

func (m *mockQueue) DeleteQueue(string) error { return nil }

func (m *mockQueue) Enqueue(string, interface{}) error { return nil }

func (m *mockQueue) EnqueueBatch(queueName string, items []interface{}) (int64, error) {
	m.enqueueBatchCalls = append(m.enqueueBatchCalls, enqueueBatchCall{queueName: queueName, items: items})
	return m.enqueueBatchResult, m.enqueueBatchError
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

func TestEnqueueBatchHandlerSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mq := &mockQueue{enqueueBatchResult: 2}
	server := &httpServerInstance{Queue: mq}

	body := EnqueueBatchRequest{
		Mode: "stopOnFailure",
		Messages: []json.RawMessage{
			json.RawMessage(`{"foo":"bar"}`),
			json.RawMessage(`42`),
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
	expectedItems := []interface{}{
		json.RawMessage(`{"foo":"bar"}`),
		json.RawMessage(`42`),
	}
	if len(call.items) != len(expectedItems) {
		t.Fatalf("items length = %d, want %d", len(call.items), len(expectedItems))
	}
	for i, item := range call.items {
		exp := expectedItems[i]
		rawItem, ok := item.(json.RawMessage)
		if !ok {
			t.Fatalf("item %d type = %T, want json.RawMessage", i, item)
		}
		rawExp, ok := exp.(json.RawMessage)
		if !ok {
			t.Fatalf("expected item %d type = %T, want json.RawMessage", i, exp)
		}
		if !bytes.Equal([]byte(rawItem), []byte(rawExp)) {
			t.Fatalf("item %d = %s, want %s", i, rawItem, rawExp)
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
}

func TestEnqueueBatchHandlerBindError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mq := &mockQueue{}
	server := &httpServerInstance{Queue: mq}

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
	server := &httpServerInstance{Queue: mq}

	body := EnqueueBatchRequest{Mode: "stopOnFailure", Messages: []json.RawMessage{json.RawMessage(`"msg"`)}}
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
