package grpc

import (
	context "context"
	"encoding/json"
	"fmt"
	"go-msg-queue-mini/internal"
	"net"

	grpc "google.golang.org/grpc"
)

type queueServiceServer struct {
	UnimplementedQueueServiceServer
	internal.Queue
}

func StartServer(ctx context.Context, config *internal.Config, queue internal.Queue) error {
	addr := fmt.Sprintf(":%d", config.GRPC.Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	protectedMethods := map[string]bool{
		"/QueueService/Enqueue": true,
		"/QueueService/Dequeue": true,
		"/QueueService/Ack":     true,
		"/QueueService/Nack":    true,
		"/QueueService/Peek":    false,
		"/QueueService/Renew":   true,
		"/QueueService/Status":  false,
		"/QueueService/HealthCheck": false,
	}
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			LoggerInterceptor,
			ErrorInterceptor,
			AuthInterceptor(config.GRPC.Auth.APIKey, protectedMethods),
			RecoveryInterceptor,
		),
	)
	queueService := NewQueueServiceServer(queue)
	RegisterQueueServiceServer(grpcServer, queueService)

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	return grpcServer.Serve(lis)
}

func NewQueueServiceServer(queue internal.Queue) *queueServiceServer {
	return &queueServiceServer{
		Queue: queue,
	}
}

func (qs *queueServiceServer) HealthCheck(ctx context.Context, req *EmptyRequest) (res *HealthResponse, err error) {
	return &HealthResponse{
		Status: "ok",
	}, nil
}

func (qs *queueServiceServer) Enqueue(ctx context.Context, req *EnqueueRequest) (res *EnqueueResponse, err error) {
	if err := qs.Queue.Enqueue(req.Message); err != nil {
		return nil, err
	}
	return &EnqueueResponse{
		Status: "ok",
	}, nil
}

func (qs *queueServiceServer) Dequeue(ctx context.Context, req *DequeueRequest) (res *DequeueResponse, err error) {
	message, err := qs.Queue.Dequeue(req.Group, req.ConsumerId)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(message.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to cast payload to []byte: %v", err)
	}

	DequeueMessage := &DequeueMessage{
		Id:      message.ID,
		Payload: payload,
		Receipt: message.Receipt,
	}
	return &DequeueResponse{
		Status:  "ok",
		Message: DequeueMessage,
	}, nil
}

func (qs *queueServiceServer) Ack(ctx context.Context, req *AckRequest) (res *AckResponse, err error) {
	if err := qs.Queue.Ack(req.Group, req.MessageId, req.Receipt); err != nil {
		return nil, err
	}
	return &AckResponse{
		Status: "ok",
	}, nil
}

func (qs *queueServiceServer) Nack(ctx context.Context, req *NackRequest) (res *NackResponse, err error) {
	if err := qs.Queue.Nack(req.Group, req.MessageId, req.Receipt); err != nil {
		return nil, err
	}
	return &NackResponse{
		Status: "ok",
	}, nil
}

func (qs *queueServiceServer) Peek(ctx context.Context, req *PeekRequest) (res *PeekResponse, err error) {
	message, err := qs.Queue.Peek(req.Group)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(message.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to cast payload to []byte: %v", err)
	}
	dequeueMessage := &DequeueMessage{
		Id:      message.ID,
		Payload: payload,
		Receipt: message.Receipt,
	}
	return &PeekResponse{
		Status:  "ok",
		Message: dequeueMessage,
	}, nil
}

func (qs *queueServiceServer) Renew(ctx context.Context, req *RenewRequest) (res *RenewResponse, err error) {
	if err := qs.Queue.Renew(req.Group, req.MessageId, req.Receipt, int(req.ExtendSec)); err != nil {
		return nil, err
	}
	return &RenewResponse{
		Status: "ok",
	}, nil
}

func (qs *queueServiceServer) Status(ctx context.Context, req *EmptyRequest) (res *StatusResponse, err error) {
	// Check the status of the queue
	status, err := qs.Queue.Status()
	if err != nil {
		return nil, err
	}
	queueStatus := &QueueStatus{
		QueueType:   status.QueueType,
		TotalMessages: status.TotalMessages,
		AckedMessages: status.AckedMessages,
		InflightMessages: status.InflightMessages,
		DlqMessages: status.DLQMessages,
	}
	return &StatusResponse{
		Status: "ok",
		QueueStatus:  queueStatus,
	}, nil
}