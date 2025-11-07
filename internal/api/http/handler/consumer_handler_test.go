package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/internal/api/http/dto"
	"go-msg-queue-mini/internal/queue_error"
)

func TestConsumerDequeueHandlerSuccess(t *testing.T) {
	queue := &fakeQueue{dequeueResp: internal.QueueMessage{Payload: []byte(`{"a":1}`), Receipt: "r-1", ID: 42}}
	handler := &ConsumerHandler{Queue: queue, Logger: newTestLogger()}

	body := dto.DequeueRequest{Group: "workers", ConsumerID: "con-1"}
	encoded, _ := json.Marshal(body)
	ctx, recorder := newJSONContext(http.MethodPost, "/api/v1/demo/dequeue", encoded)
	ctx.Set("queue_name", "demo")

	handler.DequeueHandler(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if len(queue.dequeueCalls) != 1 {
		t.Fatalf("dequeue calls = %d, want 1", len(queue.dequeueCalls))
	}
	call := queue.dequeueCalls[0]
	if call.group != "workers" || call.consumer != "con-1" {
		t.Fatalf("unexpected call: %+v", call)
	}
	var resp dto.DequeueResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Message.ID != 42 || resp.Message.Receipt != "r-1" {
		t.Fatalf("unexpected response: %+v", resp.Message)
	}
}

func TestConsumerDequeueHandlerErrorMapping(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{name: "empty", err: queue_error.ErrEmpty, wantStatus: http.StatusNoContent},
		{name: "contended", err: queue_error.ErrContended, wantStatus: http.StatusConflict},
		{name: "generic", err: errors.New("boom"), wantStatus: http.StatusBadRequest},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			queue := &fakeQueue{dequeueErr: tc.err}
			if queue.dequeueErr == nil {
				t.Fatalf("expected dequeue error for %s", tc.name)
			}
			handler := &ConsumerHandler{Queue: queue, Logger: newTestLogger()}

			body := dto.DequeueRequest{Group: "workers", ConsumerID: "con-1"}
			encoded, _ := json.Marshal(body)
			ctx, recorder := newJSONContext(http.MethodPost, "/api/v1/demo/dequeue", encoded)
			ctx.Set("queue_name", "demo")

			handler.DequeueHandler(ctx)

			if testing.Verbose() {
				t.Logf("code=%d result=%d", recorder.Code, recorder.Result().StatusCode)
			}

			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, tc.wantStatus)
			}
		})
	}
}

func TestConsumerAckHandler(t *testing.T) {
	queue := &fakeQueue{}
	handler := &ConsumerHandler{Queue: queue, Logger: newTestLogger()}

	body := dto.AckRequest{Group: "workers", MessageID: 12, Receipt: "rcpt"}
	encoded, _ := json.Marshal(body)
	ctx, recorder := newJSONContext(http.MethodPost, "/api/v1/demo/ack", encoded)
	ctx.Set("queue_name", "demo")

	handler.AckHandler(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if len(queue.ackCalls) != 1 {
		t.Fatalf("ack calls = %d, want 1", len(queue.ackCalls))
	}
	call := queue.ackCalls[0]
	if call.messageID != 12 || call.receipt != "rcpt" {
		t.Fatalf("unexpected ack call: %+v", call)
	}
}

func TestConsumerRenewHandlerLeaseExpired(t *testing.T) {
	queue := &fakeQueue{renewErr: queue_error.ErrLeaseExpired}
	handler := &ConsumerHandler{Queue: queue, Logger: newTestLogger()}

	body := dto.RenewRequest{Group: "workers", MessageID: 55, Receipt: "r", ExtendSec: 30}
	encoded, _ := json.Marshal(body)
	ctx, recorder := newJSONContext(http.MethodPost, "/api/v1/demo/renew", encoded)
	ctx.Set("queue_name", "demo")

	handler.RenewHandler(ctx)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusConflict)
	}
	if len(queue.renewCalls) != 1 {
		t.Fatalf("renew calls = %d, want 1", len(queue.renewCalls))
	}
	call := queue.renewCalls[0]
	if call.extendSec != 30 || call.messageID != 55 {
		t.Fatalf("unexpected renew call: %+v", call)
	}
}
