package http

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/internal/queue_error"
	"go-msg-queue-mini/util"
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

func (m *mockQueue) Renew(string, string, int64, string, int) error { return nil }

type mockQueueInspector struct {
	statusResult    internal.QueueStatus
	statusError     error
	statusAllResult map[string]internal.QueueStatus
	statusAllError  error
	callStatusAll   int
	peekMessages    []internal.PeekMessage
	peekError       error
	peekCalls       []peekCall
	peekErrors      []error
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
	m.callStatusAll++
	return m.statusAllResult, m.statusAllError
}

func (m *mockQueueInspector) Peek(queueName, group string, options internal.PeekOptions) ([]internal.PeekMessage, error) {
	m.peekCalls = append(m.peekCalls, peekCall{
		queueName: queueName,
		group:     group,
		options:   options,
	})
	m.peekErrors = append(m.peekErrors, m.peekError)
	if m.peekError != nil {
		return nil, m.peekError
	}
	return m.peekMessages, nil
}

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
		if !bytes.Equal(item.Item, exp.Message) {
			t.Fatalf("item %d = %s, want %s", i, item.Item, exp.Message)
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
	if !bytes.Equal(fm.Message, json.RawMessage(`"bad"`)) {
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

func TestStatusAllHandlerSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mq := &mockQueue{}
	inspector := &mockQueueInspector{
		statusAllResult: map[string]internal.QueueStatus{
			"alpha": {
				QueueType:        "memory",
				QueueName:        "alpha",
				TotalMessages:    3,
				AckedMessages:    1,
				InflightMessages: 1,
				DLQMessages:      1,
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
	server := newTestHTTPServer(mq)
	server.QueueInspector = inspector

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/status/all", nil)
	c.Request = req

	server.statusAllHandler(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp StatusAllResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.Status != "ok" {
		t.Fatalf("response status = %s, want ok", resp.Status)
	}
	if len(resp.AllQueueMap) != 2 {
		t.Fatalf("all queue map length = %d, want 2", len(resp.AllQueueMap))
	}

	alphaStatus, ok := resp.AllQueueMap["alpha"]
	if !ok {
		t.Fatalf("expected alpha queue status in response")
	}
	if alphaStatus.TotalMessages != 3 || alphaStatus.AckedMessages != 1 || alphaStatus.InflightMessages != 1 || alphaStatus.DLQMessages != 1 {
		t.Fatalf("unexpected alpha status: %#v", alphaStatus)
	}
}

func TestStatusAllHandlerError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mq := &mockQueue{}
	inspector := &mockQueueInspector{
		statusAllError: errors.New("boom"),
	}
	server := newTestHTTPServer(mq)
	server.QueueInspector = inspector

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/status/all", nil)
	c.Request = req

	server.statusAllHandler(c)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusInternalServerError)
	}

	var resp map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp["error"] != "failed to get all queue status" {
		t.Fatalf("error message = %s, want failed to get all queue status", resp["error"])
	}
	if inspector.callStatusAll != 1 {
		t.Fatalf("StatusAll call count = %d, want 1", inspector.callStatusAll)
	}
}

func TestPeekHandlerSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mq := &mockQueue{}
	now := time.Now()
	inspector := &mockQueueInspector{
		peekMessages: []internal.PeekMessage{
			{ID: 101, Payload: json.RawMessage(`"alpha"`), Receipt: "r-1", InsertedAt: now},
			{ID: 102, Payload: json.RawMessage(`"beta"`), Receipt: "r-2", InsertedAt: now.Add(1 * time.Minute)},
		},
	}
	server := newTestHTTPServer(mq)
	server.QueueInspector = inspector

	body := PeekRequest{
		Group: "group-A",
		Options: PeekOptions{
			Limit:  2,
			Cursor: 10,
			Order:  "desc",
		},
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("queue_name", "tasks")
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/peek", bytes.NewReader(encoded))
	req.Header.Set("Content-Type", "application/json")
	c.Request = req

	server.peekHandler(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if len(inspector.peekCalls) != 1 {
		t.Fatalf("peek call count = %d, want 1", len(inspector.peekCalls))
	}
	call := inspector.peekCalls[0]
	if call.queueName != "tasks" {
		t.Fatalf("queue name = %s, want tasks", call.queueName)
	}
	if call.group != "group-A" {
		t.Fatalf("group name = %s, want group-A", call.group)
	}
	if call.options.Limit != 2 || call.options.Cursor != 10 || call.options.Order != "desc" {
		t.Fatalf("unexpected peek options: %+v", call.options)
	}

	var resp PeekResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal peek response: %v", err)
	}
	if resp.Status != "ok" {
		t.Fatalf("response status = %s, want ok", resp.Status)
	}
	if len(resp.Messages) != 2 {
		t.Fatalf("message count = %d, want 2", len(resp.Messages))
	}
	if resp.Messages[0].ID != 101 || resp.Messages[0].Receipt != "r-1" || !bytes.Equal(resp.Messages[0].Payload, json.RawMessage(`"alpha"`)) {
		t.Fatalf("unexpected first message: %#v", resp.Messages[0])
	}
}

func TestPeekHandlerPreviewVariants(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mq := &mockQueue{}

	marshalString := func(tb testing.TB, s string) json.RawMessage {
		tb.Helper()
		b, err := json.Marshal(s)
		if err != nil {
			tb.Fatalf("failed to marshal string %q: %v", s, err)
		}
		return json.RawMessage(b)
	}

	longText := strings.Repeat("l", 80)

	shortPayload := marshalString(t, "short")
	longPayload := marshalString(t, longText)
	objectPayload := json.RawMessage(`{"foo":"bar","nested":{"key":1}}`)
	arrayPayload := json.RawMessage(`[1,2,3,{"nested":true}]`)

	inspector := &mockQueueInspector{
		peekMessages: []internal.PeekMessage{
			{ID: 301, Payload: shortPayload, Receipt: "short-receipt", InsertedAt: time.Now()},
			{ID: 302, Payload: longPayload, Receipt: "long-receipt", InsertedAt: time.Now()},
			{ID: 303, Payload: objectPayload, Receipt: "object-receipt", InsertedAt: time.Now()},
			{ID: 304, Payload: arrayPayload, Receipt: "array-receipt", InsertedAt: time.Now()},
		},
	}
	server := newTestHTTPServer(mq)
	server.QueueInspector = inspector

	body := PeekRequest{
		Group: "group-preview",
		Options: PeekOptions{
			Limit:   4,
			Order:   "asc",
			Preview: true,
		},
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("queue_name", "preview-queue")
	req := httptest.NewRequest(http.MethodPost, "/api/v1/preview-queue/peek", bytes.NewReader(encoded))
	req.Header.Set("Content-Type", "application/json")
	c.Request = req

	server.peekHandler(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if len(inspector.peekCalls) != 1 {
		t.Fatalf("peek call count = %d, want 1", len(inspector.peekCalls))
	}
	call := inspector.peekCalls[0]
	if !call.options.Preview {
		t.Fatal("expected preview option to be true")
	}
	if call.options.Limit != 4 {
		t.Fatalf("preview limit = %d, want 4", call.options.Limit)
	}

	var resp PeekResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal peek response: %v", err)
	}
	if resp.Status != "ok" {
		t.Fatalf("response status = %s, want ok", resp.Status)
	}
	if len(resp.Messages) != 4 {
		t.Fatalf("message count = %d, want 4", len(resp.Messages))
	}

	originals := []json.RawMessage{shortPayload, longPayload, objectPayload, arrayPayload}
	for i, msg := range resp.Messages {
		if msg.ID != inspector.peekMessages[i].ID {
			t.Fatalf("message[%d] id = %d, want %d", i, msg.ID, inspector.peekMessages[i].ID)
		}
		if msg.Receipt != inspector.peekMessages[i].Receipt {
			t.Fatalf("message[%d] receipt = %s, want %s", i, msg.Receipt, inspector.peekMessages[i].Receipt)
		}

		expectedPreview := util.PreviewStringRuneSafe(string(originals[i]), peekMsgPreviewLength)

		var actualPreview string
		if err := json.Unmarshal(msg.Payload, &actualPreview); err != nil {
			t.Fatalf("failed to unmarshal preview payload[%d]: %v", i, err)
		}

		if actualPreview != expectedPreview {
			t.Fatalf("preview payload[%d] = %q, want %q", i, actualPreview, expectedPreview)
		}
	}

	// ensure long payload was truncated with ellipsis
	var longPreview string
	if err := json.Unmarshal(resp.Messages[1].Payload, &longPreview); err != nil {
		t.Fatalf("failed to unmarshal long preview: %v", err)
	}
	if !strings.HasSuffix(longPreview, "...") {
		t.Fatalf("long preview = %q, want suffix ...", longPreview)
	}
	if runeCount := len([]rune(longPreview)); runeCount != peekMsgPreviewLength+3 {
		t.Fatalf("long preview rune length = %d, want %d", runeCount, peekMsgPreviewLength+3)
	}
}

func TestPeekHandlerEmptyQueue(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mq := &mockQueue{}
	inspector := &mockQueueInspector{
		peekError: queue_error.ErrNoMessage,
	}
	server := newTestHTTPServer(mq)
	server.QueueInspector = inspector

	if inspector.peekError == nil {
		t.Fatal("expected peekError to be set for empty queue scenario")
	}

	body := PeekRequest{
		Group: "group-A",
		Options: PeekOptions{
			Limit: 1,
		},
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("queue_name", "tasks")
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/peek", bytes.NewReader(encoded))
	req.Header.Set("Content-Type", "application/json")
	c.Request = req

	server.peekHandler(c)

	if len(inspector.peekCalls) != 1 {
		t.Fatalf("peek call count = %d, want 1", len(inspector.peekCalls))
	}
	if len(inspector.peekErrors) != 1 {
		t.Fatalf("peek error record count = %d, want 1", len(inspector.peekErrors))
	}
	if inspector.peekErrors[0] == nil {
		t.Fatalf("expected recorded peek error, got nil")
	}
	if !errors.Is(inspector.peekErrors[0], queue_error.ErrNoMessage) {
		t.Fatalf("unexpected recorded error: %v", inspector.peekErrors[0])
	}
	t.Logf("recorded peek error: %v", inspector.peekErrors[0])
	t.Logf("response status=%d body=%q", w.Code, w.Body.String())
	t.Logf("writer reported status=%d", c.Writer.Status())
	if c.Writer.Status() != http.StatusNoContent {
		t.Fatalf("writer status = %d, want %d", c.Writer.Status(), http.StatusNoContent)
	}
	if w.Body.Len() != 0 {
		t.Fatalf("expected empty response body, got %q", w.Body.String())
	}
}
