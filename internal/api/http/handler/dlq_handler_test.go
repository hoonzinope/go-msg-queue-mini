package handler

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go-msg-queue-mini/internal"
	"log/slog"

	"github.com/gin-gonic/gin"
)

type dlqListCall struct {
	queueName string
	options   internal.PeekOptions
}

type fakeDLQInspector struct {
	listCalls      []dlqListCall
	listResponse   []internal.DLQMessage
	listErr        error
	detailQueue    string
	detailID       int64
	detailResponse internal.DLQMessage
	detailErr      error
}

func (f *fakeDLQInspector) Status(string) (internal.QueueStatus, error) {
	return internal.QueueStatus{}, nil
}

func (f *fakeDLQInspector) StatusAll() (map[string]internal.QueueStatus, error) {
	return nil, nil
}

func (f *fakeDLQInspector) Peek(string, string, internal.PeekOptions) ([]internal.PeekMessage, error) {
	return nil, nil
}

func (f *fakeDLQInspector) Detail(string, int64) (internal.PeekMessage, error) {
	return internal.PeekMessage{}, nil
}

func (f *fakeDLQInspector) ListDLQ(queueName string, options internal.PeekOptions) ([]internal.DLQMessage, error) {
	f.listCalls = append(f.listCalls, dlqListCall{queueName: queueName, options: options})
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.listResponse, nil
}

func (f *fakeDLQInspector) DetailDLQ(queueName string, messageID int64) (internal.DLQMessage, error) {
	f.detailQueue = queueName
	f.detailID = messageID
	if f.detailErr != nil {
		return internal.DLQMessage{}, f.detailErr
	}
	return f.detailResponse, nil
}

func TestParseQueryOptionsDefaults(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/queues/example/dlq", nil)

	opts := ParseQueryOptions(c)
	if opts != defaultDLQPeekOptions {
		t.Fatalf("got %+v, want %+v", opts, defaultDLQPeekOptions)
	}
}

func TestParseQueryOptionsOverrides(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/queues/example/dlq?limit=5&cursor=9&order=desc&preview=true", nil)

	opts := ParseQueryOptions(c)
	if opts.Limit != 5 {
		t.Fatalf("limit = %d, want 5", opts.Limit)
	}
	if opts.Cursor != 9 {
		t.Fatalf("cursor = %d, want 9", opts.Cursor)
	}
	if opts.Order != "desc" {
		t.Fatalf("order = %s, want desc", opts.Order)
	}
	if !opts.Preview {
		t.Fatalf("preview = %v, want true", opts.Preview)
	}
}

func TestListDLQMessagesHandlerUsesQueryOptions(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/test-queue/dlq?limit=5&cursor=9&order=desc&preview=true", nil)
	c.Request = req
	c.Params = gin.Params{{Key: "queue_name", Value: "test-queue"}}

	mock := &fakeDLQInspector{
		listResponse: []internal.DLQMessage{
			{
				ID:          42,
				Payload:     []byte("hello"),
				Reason:      "worker failed",
				FailedGroup: "consumer-a",
				InsertedAt:  time.Date(2024, 3, 14, 9, 26, 0, 0, time.UTC),
			},
		},
	}
	handler := &DLQHandler{
		QueueInspector: mock,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	handler.ListDLQMessagesHandler(c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if len(mock.listCalls) != 1 {
		t.Fatalf("list calls = %d, want 1", len(mock.listCalls))
	}
	call := mock.listCalls[0]
	if call.queueName != "test-queue" {
		t.Fatalf("queue name = %s, want test-queue", call.queueName)
	}
	if call.options.Limit != 5 || call.options.Cursor != 9 || call.options.Order != "desc" || !call.options.Preview {
		t.Fatalf("options mismatch: %+v", call.options)
	}

	var resp DLQListResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Status != "ok" {
		t.Fatalf("status field = %s, want ok", resp.Status)
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("messages len = %d, want 1", len(resp.Messages))
	}
	msg := resp.Messages[0]
	if string(msg.Payload) != "hello" {
		t.Fatalf("payload = %s, want hello", string(msg.Payload))
	}
	if msg.Reason != "worker failed" {
		t.Fatalf("reason = %s, want worker failed", msg.Reason)
	}
	if msg.FailedGroup != "consumer-a" {
		t.Fatalf("failed_group = %s, want consumer-a", msg.FailedGroup)
	}
	wantInserted := mock.listResponse[0].InsertedAt.Format("2006-01-02 15:04:05")
	if msg.InsertedAt != wantInserted {
		t.Fatalf("inserted_at = %s, want %s", msg.InsertedAt, wantInserted)
	}
}

func TestDetailDLQMessageHandlerSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/test-queue/dlq/42", nil)
	c.Request = req
	c.Params = gin.Params{
		{Key: "queue_name", Value: "test-queue"},
		{Key: "message_id", Value: "42"},
	}

	mock := &fakeDLQInspector{
		detailResponse: internal.DLQMessage{
			ID:          42,
			Payload:     []byte("world"),
			Reason:      "timeout",
			FailedGroup: "consumer-b",
			InsertedAt:  time.Date(2024, 5, 1, 8, 30, 0, 0, time.UTC),
		},
	}
	handler := &DLQHandler{
		QueueInspector: mock,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	handler.DetailDLQMessageHandler(c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if mock.detailQueue != "test-queue" || mock.detailID != 42 {
		t.Fatalf("detail call mismatch: queue=%s id=%d", mock.detailQueue, mock.detailID)
	}

	var resp DLQDetailResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Message.Reason != "timeout" {
		t.Fatalf("reason = %s, want timeout", resp.Message.Reason)
	}
	if resp.Message.FailedGroup != "consumer-b" {
		t.Fatalf("failed_group = %s, want consumer-b", resp.Message.FailedGroup)
	}
	wantInserted := mock.detailResponse.InsertedAt.Format("2006-01-02 15:04:05")
	if resp.Message.InsertedAt != wantInserted {
		t.Fatalf("inserted_at = %s, want %s", resp.Message.InsertedAt, wantInserted)
	}
	if string(resp.Message.Payload) != "world" {
		t.Fatalf("payload = %s, want world", string(resp.Message.Payload))
	}
}
