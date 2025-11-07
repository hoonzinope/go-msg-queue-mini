package handler

import (
	"bytes"
	"io"
	"net/http/httptest"

	"go-msg-queue-mini/internal"
	"log/slog"

	"github.com/gin-gonic/gin"
)

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newJSONContext(method, target string, body []byte) (*gin.Context, *httptest.ResponseRecorder) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, target, reader)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	ctx.Request = req
	return ctx, recorder
}

type enqueueCall struct {
	queueName string
	message   internal.EnqueueMessage
}

type enqueueBatchCall struct {
	queueName string
	mode      string
	items     []internal.EnqueueMessage
}

type dequeueCall struct {
	queueName string
	group     string
	consumer  string
}

type ackCall struct {
	queueName string
	group     string
	messageID int64
	receipt   string
}

type renewCall struct {
	queueName string
	group     string
	messageID int64
	receipt   string
	extendSec int
}

type fakeQueue struct {
	createCalls       []string
	createErr         error
	deleteCalls       []string
	deleteErr         error
	enqueueCalls      []enqueueCall
	enqueueErr        error
	enqueueBatchCalls []enqueueBatchCall
	enqueueBatchResp  *internal.BatchResult
	enqueueBatchErr   error
	dequeueCalls      []dequeueCall
	dequeueResp       internal.QueueMessage
	dequeueErr        error
	ackCalls          []ackCall
	ackErr            error
	nackCalls         []ackCall
	nackErr           error
	renewCalls        []renewCall
	renewErr          error
}

func (f *fakeQueue) CreateQueue(name string) error {
	f.createCalls = append(f.createCalls, name)
	return f.createErr
}

func (f *fakeQueue) DeleteQueue(name string) error {
	f.deleteCalls = append(f.deleteCalls, name)
	return f.deleteErr
}

func (f *fakeQueue) Enqueue(queueName string, msg internal.EnqueueMessage) error {
	f.enqueueCalls = append(f.enqueueCalls, enqueueCall{queueName: queueName, message: msg})
	return f.enqueueErr
}

func (f *fakeQueue) EnqueueBatch(queueName, mode string, items []internal.EnqueueMessage) (internal.BatchResult, error) {
	f.enqueueBatchCalls = append(f.enqueueBatchCalls, enqueueBatchCall{queueName: queueName, mode: mode, items: items})
	if f.enqueueBatchResp != nil {
		return *f.enqueueBatchResp, f.enqueueBatchErr
	}
	return internal.BatchResult{SuccessCount: int64(len(items))}, f.enqueueBatchErr
}

func (f *fakeQueue) Dequeue(queueName, group, consumer string) (internal.QueueMessage, error) {
	f.dequeueCalls = append(f.dequeueCalls, dequeueCall{queueName: queueName, group: group, consumer: consumer})
	if f.dequeueErr != nil {
		return internal.QueueMessage{}, f.dequeueErr
	}
	return f.dequeueResp, nil
}

func (f *fakeQueue) Ack(queueName, group string, messageID int64, receipt string) error {
	f.ackCalls = append(f.ackCalls, ackCall{queueName: queueName, group: group, messageID: messageID, receipt: receipt})
	return f.ackErr
}

func (f *fakeQueue) Nack(queueName, group string, messageID int64, receipt string) error {
	f.nackCalls = append(f.nackCalls, ackCall{queueName: queueName, group: group, messageID: messageID, receipt: receipt})
	return f.nackErr
}

func (f *fakeQueue) Shutdown() error { return nil }

func (f *fakeQueue) Renew(queueName, group string, messageID int64, receipt string, extendSec int) error {
	f.renewCalls = append(f.renewCalls, renewCall{queueName: queueName, group: group, messageID: messageID, receipt: receipt, extendSec: extendSec})
	return f.renewErr
}

type peekCall struct {
	queueName string
	group     string
	options   internal.PeekOptions
}

type detailCall struct {
	queueName string
	messageID int64
}

type fakeInspector struct {
	statusResp     internal.QueueStatus
	statusErr      error
	statusCalls    []string
	statusAllResp  map[string]internal.QueueStatus
	statusAllErr   error
	statusAllCalls int
	peekResp       []internal.PeekMessage
	peekErr        error
	peekCalls      []peekCall
	detailResp     internal.PeekMessage
	detailErr      error
	detailCalls    []detailCall
}

func (f *fakeInspector) Status(queueName string) (internal.QueueStatus, error) {
	f.statusCalls = append(f.statusCalls, queueName)
	return f.statusResp, f.statusErr
}

func (f *fakeInspector) StatusAll() (map[string]internal.QueueStatus, error) {
	f.statusAllCalls++
	return f.statusAllResp, f.statusAllErr
}

func (f *fakeInspector) Peek(queueName, group string, options internal.PeekOptions) ([]internal.PeekMessage, error) {
	f.peekCalls = append(f.peekCalls, peekCall{queueName: queueName, group: group, options: options})
	if f.peekErr != nil {
		return nil, f.peekErr
	}
	return f.peekResp, nil
}

func (f *fakeInspector) Detail(queueName string, messageId int64) (internal.PeekMessage, error) {
	f.detailCalls = append(f.detailCalls, detailCall{queueName: queueName, messageID: messageId})
	if f.detailErr != nil {
		return internal.PeekMessage{}, f.detailErr
	}
	return f.detailResp, nil
}

func (f *fakeInspector) ListDLQ(string, internal.PeekOptions) ([]internal.DLQMessage, error) {
	return nil, nil
}

func (f *fakeInspector) DetailDLQ(string, int64) (internal.DLQMessage, error) {
	return internal.DLQMessage{}, nil
}
