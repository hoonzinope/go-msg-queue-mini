package handler

import (
	"encoding/json"
	"net/http"
	"testing"

	"go-msg-queue-mini/internal/api/http/dto"
)

func TestProducerEnqueueBatchHandlerSuccess(t *testing.T) {
	queue := &fakeQueue{}
	handler := &ProducerHandler{Queue: queue, Logger: newTestLogger()}

	body := dto.EnqueueBatchRequest{
		Mode: "stopOnFailure",
		Messages: []dto.EnqueueMessage{
			{Message: json.RawMessage(`{"foo":"bar"}`), DeduplicationID: "dedup-1"},
			{Message: json.RawMessage(`42`), DeduplicationID: "dedup-2"},
		},
	}
	encoded, _ := json.Marshal(body)

	ctx, recorder := newJSONContext(http.MethodPost, "/api/v1/demo/enqueue/batch", encoded)
	ctx.Set("queue_name", "demo")

	handler.EnqueueBatchHandler(ctx)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusAccepted)
	}
	if len(queue.enqueueBatchCalls) != 1 {
		t.Fatalf("batch calls = %d, want 1", len(queue.enqueueBatchCalls))
	}
	call := queue.enqueueBatchCalls[0]
	if call.queueName != "demo" || call.mode != "stopOnFailure" {
		t.Fatalf("unexpected call metadata: %+v", call)
	}
	if len(call.items) != 2 {
		t.Fatalf("items length = %d, want 2", len(call.items))
	}
	if string(call.items[0].Item) != `{"foo":"bar"}` {
		t.Fatalf("first payload mismatch: %s", string(call.items[0].Item))
	}
	if string(call.items[1].Item) != "42" {
		t.Fatalf("second payload mismatch: %s", string(call.items[1].Item))
	}
}

func TestProducerEnqueueBatchHandlerRejectsInvalidMode(t *testing.T) {
	queue := &fakeQueue{}
	handler := &ProducerHandler{Queue: queue, Logger: newTestLogger()}

	body := dto.EnqueueBatchRequest{
		Mode:     "unknown",
		Messages: []dto.EnqueueMessage{{Message: json.RawMessage(`1`)}},
	}

	encoded, _ := json.Marshal(body)

	ctx, recorder := newJSONContext(http.MethodPost, "/api/v1/demo/enqueue/batch", encoded)
	ctx.Set("queue_name", "demo")

	handler.EnqueueBatchHandler(ctx)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	if len(queue.enqueueBatchCalls) != 0 {
		t.Fatalf("enqueue batch should not be called")
	}
}

func TestProducerEnqueueHandlerCallsQueue(t *testing.T) {
	queue := &fakeQueue{}
	handler := &ProducerHandler{Queue: queue, Logger: newTestLogger()}

	body := dto.EnqueueRequest{Message: json.RawMessage(`{"hello":"world"}`)}
	encoded, _ := json.Marshal(body)

	ctx, recorder := newJSONContext(http.MethodPost, "/api/v1/demo/enqueue", encoded)
	ctx.Set("queue_name", "demo")

	handler.EnqueueHandler(ctx)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusAccepted)
	}
	if len(queue.enqueueCalls) != 1 {
		t.Fatalf("enqueue calls = %d, want 1", len(queue.enqueueCalls))
	}
	call := queue.enqueueCalls[0]
	if call.queueName != "demo" {
		t.Fatalf("queue name = %s, want demo", call.queueName)
	}
	if string(call.message.Item) != `{"hello":"world"}` {
		t.Fatalf("payload mismatch: %s", string(call.message.Item))
	}
}
