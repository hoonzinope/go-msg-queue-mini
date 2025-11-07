package handler

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/internal/api/http/dto"
	"go-msg-queue-mini/internal/queue_error"
)

func TestInspectPeekHandlerAppliesOptions(t *testing.T) {
	inspector := &fakeInspector{
		peekResp: []internal.PeekMessage{
			{ID: 7, Payload: []byte(`{"foo":"bar"}`), Receipt: "r-1", InsertedAt: time.Unix(100, 0)},
		},
	}
	handler := &InspectHandler{QueueInspector: inspector, Logger: newTestLogger()}

	body := dto.PeekRequest{
		Group:   "workers",
		Options: dto.PeekOptions{Limit: dto.PeekMaxLimit + 10, Cursor: 9, Order: "desc", Preview: true},
	}
	encoded, _ := json.Marshal(body)

	ctx, recorder := newJSONContext(http.MethodPost, "/api/v1/demo/peek", encoded)
	ctx.Set("queue_name", "demo")

	handler.PeekHandler(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if len(inspector.peekCalls) != 1 {
		t.Fatalf("peek calls = %d, want 1", len(inspector.peekCalls))
	}
	call := inspector.peekCalls[0]
	if call.queueName != "demo" || call.group != "workers" {
		t.Fatalf("unexpected peek call metadata: %+v", call)
	}
	if call.options.Limit != dto.PeekMaxLimit {
		t.Fatalf("limit = %d, want %d", call.options.Limit, dto.PeekMaxLimit)
	}
	if call.options.Order != "desc" || call.options.Cursor != 9 || !call.options.Preview {
		t.Fatalf("unexpected options: %+v", call.options)
	}

	var resp dto.PeekResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if len(resp.Messages) != 1 || resp.Messages[0].ID != 7 {
		t.Fatalf("unexpected response: %+v", resp.Messages)
	}
	var payload string
	if err := json.Unmarshal(resp.Messages[0].Payload, &payload); err != nil {
		t.Fatalf("payload unmarshal: %v", err)
	}
	if payload != "{\"foo\":\"bar\"}" {
		t.Fatalf("payload preview mismatch: %s", payload)
	}
}

func TestInspectPeekHandlerEmpty(t *testing.T) {
	inspector := &fakeInspector{peekErr: queue_error.ErrEmpty}
	handler := &InspectHandler{QueueInspector: inspector, Logger: newTestLogger()}

	body := dto.PeekRequest{Group: "workers"}
	encoded, _ := json.Marshal(body)
	ctx, recorder := newJSONContext(http.MethodPost, "/api/v1/demo/peek", encoded)
	ctx.Set("queue_name", "demo")

	handler.PeekHandler(ctx)

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
}

func TestInspectStatusAllHandler(t *testing.T) {
	inspector := &fakeInspector{statusAllResp: map[string]internal.QueueStatus{
		"demo": {QueueType: "memory", TotalMessages: 3, AckedMessages: 1, InflightMessages: 1, DLQMessages: 1},
	}}
	handler := &InspectHandler{QueueInspector: inspector, Logger: newTestLogger()}

	ctx, recorder := newJSONContext(http.MethodGet, "/api/v1/status/all", nil)

	handler.StatusAllHandler(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if inspector.statusAllCalls != 1 {
		t.Fatalf("statusAll calls = %d, want 1", inspector.statusAllCalls)
	}
	var resp dto.StatusAllResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.AllQueueMap["demo"].TotalMessages != 3 {
		t.Fatalf("unexpected response body: %+v", resp)
	}
}
