package grpc

import (
	context "context"
	"encoding/json"
	"fmt"
	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/util"
	"log/slog"
	"net"

	grpc "google.golang.org/grpc"
)

type queueServiceServer struct {
	UnimplementedQueueServiceServer
	internal.Queue
	Logger *slog.Logger
}

func StartServer(ctx context.Context, config *internal.Config, queue internal.Queue, logger *slog.Logger) error {
	addr := fmt.Sprintf(":%d", config.GRPC.Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	protectedMethods := map[string]bool{
		"/queue.v1.QueueService/CreateQueue":  true,
		"/queue.v1.QueueService/DeleteQueue":  true,
		"/queue.v1.QueueService/Enqueue":      true,
		"/queue.v1.QueueService/EnqueueBatch": true,
		"/queue.v1.QueueService/Dequeue":      true,
		"/queue.v1.QueueService/Ack":          true,
		"/queue.v1.QueueService/Nack":         true,
		"/queue.v1.QueueService/Renew":        true,
		"/queue.v1.QueueService/Peek":         false,
		"/queue.v1.QueueService/Status":       false,
		"/queue.v1.QueueService/HealthCheck":  false,
	}
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			RecoveryInterceptor,
			LoggerInterceptor,
			ErrorInterceptor,
			AuthInterceptor(config.GRPC.Auth.APIKey, protectedMethods),
		),
	)
	queueService := NewQueueServiceServer(queue, logger)
	RegisterQueueServiceServer(grpcServer, queueService)

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	return grpcServer.Serve(lis)
}

func NewQueueServiceServer(queue internal.Queue, logger *slog.Logger) *queueServiceServer {
	return &queueServiceServer{
		Queue:  queue,
		Logger: logger,
	}
}

func (qs *queueServiceServer) HealthCheck(ctx context.Context, req *EmptyRequest) (res *HealthResponse, err error) {
	return &HealthResponse{
		Status: "ok",
	}, nil
}

func (qs *queueServiceServer) CreateQueue(ctx context.Context, req *CreateQueueRequest) (res *CreateQueueResponse, err error) {
	queue_name := req.GetQueueName()
	if queue_name == "" {
		qs.Logger.Error("Error creating queue", "error", "queue name is required")
		return nil, fmt.Errorf("queue name is required")
	}
	if err := qs.Queue.CreateQueue(queue_name); err != nil {
		qs.Logger.Error("Error creating queue", "error", err)
		return nil, err
	}
	return &CreateQueueResponse{Status: "ok"}, nil
}

func (qs *queueServiceServer) DeleteQueue(ctx context.Context, req *DeleteQueueRequest) (res *DeleteQueueResponse, err error) {
	queue_name := req.GetQueueName()
	if queue_name == "" {
		qs.Logger.Error("Error deleting queue", "error", "queue name is required")
		return nil, fmt.Errorf("queue name is required")
	}
	if err := qs.Queue.DeleteQueue(queue_name); err != nil {
		qs.Logger.Error("Error deleting queue", "error", err)
		return nil, err
	}
	return &DeleteQueueResponse{Status: "ok"}, nil
}

func (qs *queueServiceServer) Enqueue(ctx context.Context, req *EnqueueRequest) (res *EnqueueResponse, err error) {
	queue_name := req.GetQueueName()
	if queue_name == "" {
		qs.Logger.Error("Error enqueuing message", "error", "queue name is required")
		return nil, fmt.Errorf("queue name is required")
	}
	message := req.GetMessage()
	if err := qs.Queue.Enqueue(queue_name, message); err != nil {
		qs.Logger.Error("Error enqueuing message", "error", err)
		return nil, err
	}
	return &EnqueueResponse{Status: "ok", Message: message}, nil
}

func (qs *queueServiceServer) EnqueueBatch(ctx context.Context, req *EnqueueBatchRequest) (res *EnqueueBatchResponse, err error) {
	queue_name := req.GetQueueName()
	if queue_name == "" {
		qs.Logger.Error("Error enqueuing messages", "error", "queue name is required")
		return nil, fmt.Errorf("queue name is required")
	}

	mode := req.GetMode()
	if mode != "partialSuccess" && mode != "stopOnFailure" {
		qs.Logger.Error("Error enqueuing messages", "error", "invalid mode")
		return nil, fmt.Errorf("invalid mode")
	}

	messages := req.GetMessages()
	if len(messages) == 0 {
		qs.Logger.Error("Error enqueuing messages", "error", "messages are required")
		return nil, fmt.Errorf("messages are required")
	}
	msgs := make([]interface{}, len(messages))
	for i, msg := range messages {
		msgs[i] = msg
	}

	chunkedMsgs := util.ChunkSlice(msgs, 100) // Chunk size of 100
	var totalSuccess int64 = 0
	switch mode {
	case "stopOnFailure":
		for _, chunk := range chunkedMsgs {
			successCount, err := qs.Queue.EnqueueBatch(queue_name, chunk)
			if err != nil {
				qs.Logger.Error("Error enqueuing messages", "error", err)
				return &EnqueueBatchResponse{
					Status:       "enqueued",
					SuccessCount: totalSuccess,
					FailureCount: int64(len(messages)) - totalSuccess,
					// No failed messages in stopOnFailure mode
				}, nil
			}
			totalSuccess += int64(successCount)
			// If some messages in the chunk failed, stop processing further
			if successCount < len(chunk) {
				break
			}
		}
		return &EnqueueBatchResponse{
			Status:       "enqueued",
			SuccessCount: totalSuccess,
			FailureCount: int64(len(messages)) - totalSuccess,
			// No failed messages in stopOnFailure mode
		}, nil
	case "partialSuccess":
		failedMessages := make([]*FailedMessage, 0)
		for _, chunk := range chunkedMsgs {
			successCount, err := qs.Queue.EnqueueBatch(queue_name, chunk)
			if err != nil {
				qs.Logger.Error("Error enqueuing messages", "error", err)
				// Mark all messages in this chunk as failed
				for i := 0; i < len(chunk); i++ {
					failedMessages = append(failedMessages, &FailedMessage{
						Index:   totalSuccess + int64(i),
						Message: string(chunk[i].(json.RawMessage)),
						Error:   err.Error(),
					})
				}
			} else {
				totalSuccess += int64(successCount)
			}
		}
		return &EnqueueBatchResponse{
			Status:         "enqueued",
			SuccessCount:   totalSuccess,
			FailureCount:   int64(len(messages)) - totalSuccess,
			FailedMessages: failedMessages,
		}, nil
	default:
		qs.Logger.Error("Error enqueuing messages", "error", "invalid mode")
		return nil, fmt.Errorf("invalid mode")
	}
}

func (qs *queueServiceServer) Dequeue(ctx context.Context, req *DequeueRequest) (res *DequeueResponse, err error) {
	message, err := qs.Queue.Dequeue(req.QueueName, req.Group, req.ConsumerId)
	if err != nil {
		qs.Logger.Error("Error dequeuing message", "error", err)
		return nil, err
	}
	DequeueMessage := &DequeueMessage{
		Id:      message.ID,
		Payload: message.Payload.(string),
		Receipt: message.Receipt,
	}
	return &DequeueResponse{
		Status:  "ok",
		Message: DequeueMessage,
	}, nil
}

func (qs *queueServiceServer) Ack(ctx context.Context, req *AckRequest) (res *AckResponse, err error) {
	if err := qs.Queue.Ack(req.QueueName, req.Group, req.MessageId, req.Receipt); err != nil {
		qs.Logger.Error("Error ACKing message", "error", err)
		return nil, err
	}
	return &AckResponse{
		Status: "ok",
	}, nil
}

func (qs *queueServiceServer) Nack(ctx context.Context, req *NackRequest) (res *NackResponse, err error) {
	if err := qs.Queue.Nack(req.QueueName, req.Group, req.MessageId, req.Receipt); err != nil {
		qs.Logger.Error("Error NACKing message", "error", err)
		return nil, err
	}
	return &NackResponse{
		Status: "ok",
	}, nil
}

func (qs *queueServiceServer) Peek(ctx context.Context, req *PeekRequest) (res *PeekResponse, err error) {
	message, err := qs.Queue.Peek(req.QueueName, req.Group)
	if err != nil {
		qs.Logger.Error("Error peeking message", "error", err)
		return nil, err
	}
	dequeueMessage := &DequeueMessage{
		Id:      message.ID,
		Payload: message.Payload.(string),
		Receipt: message.Receipt,
	}
	return &PeekResponse{
		Status:  "ok",
		Message: dequeueMessage,
	}, nil
}

func (qs *queueServiceServer) Renew(ctx context.Context, req *RenewRequest) (res *RenewResponse, err error) {
	if err := qs.Queue.Renew(req.QueueName, req.Group, req.MessageId, req.Receipt, int(req.ExtendSec)); err != nil {
		qs.Logger.Error("Error renewing message", "error", err)
		return nil, err
	}
	return &RenewResponse{
		Status: "ok",
	}, nil
}

func (qs *queueServiceServer) Status(ctx context.Context, req *StatusRequest) (res *StatusResponse, err error) {
	// Check the status of the queue
	status, err := qs.Queue.Status(req.QueueName)
	if err != nil {
		qs.Logger.Error("Error getting queue status", "error", err)
		return nil, err
	}
	queueStatus := &QueueStatus{
		QueueName:        status.QueueName,
		TotalMessages:    status.TotalMessages,
		AckedMessages:    status.AckedMessages,
		InflightMessages: status.InflightMessages,
		DlqMessages:      status.DLQMessages,
	}
	return &StatusResponse{
		Status:      "ok",
		QueueStatus: queueStatus,
	}, nil
}
