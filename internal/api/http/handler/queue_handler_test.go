package handler

import (
	"errors"
	"net/http"
	"testing"
)

func TestQueueHandlerCreateQueue(t *testing.T) {
	queue := &fakeQueue{}
	handler := &QueueHandler{Queue: queue, Logger: newTestLogger()}

	ctx, recorder := newJSONContext(http.MethodPost, "/api/v1/demo/create", nil)
	ctx.Set("queue_name", "demo")

	handler.CreateQueueHandler(ctx)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusCreated)
	}
	if len(queue.createCalls) != 1 || queue.createCalls[0] != "demo" {
		t.Fatalf("unexpected create calls: %+v", queue.createCalls)
	}
}

func TestQueueHandlerCreateQueueMissingName(t *testing.T) {
	handler := &QueueHandler{Queue: &fakeQueue{}, Logger: newTestLogger()}

	ctx, recorder := newJSONContext(http.MethodPost, "/api/v1/demo/create", nil)

	handler.CreateQueueHandler(ctx)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}

func TestQueueHandlerDeleteQueueError(t *testing.T) {
	queue := &fakeQueue{deleteErr: errors.New("boom")}
	handler := &QueueHandler{Queue: queue, Logger: newTestLogger()}

	ctx, recorder := newJSONContext(http.MethodDelete, "/api/v1/demo/delete", nil)
	ctx.Set("queue_name", "demo")

	handler.DeleteQueueHandler(ctx)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusInternalServerError)
	}
	if len(queue.deleteCalls) != 1 || queue.deleteCalls[0] != "demo" {
		t.Fatalf("unexpected delete calls: %+v", queue.deleteCalls)
	}
}
