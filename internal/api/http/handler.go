package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"go-msg-queue-mini/internal/queue_error"
	"go-msg-queue-mini/util"
	"net/http"

	"github.com/gin-gonic/gin"
)

func healthCheckHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func getQueueName(c *gin.Context) (string, error) {
	queue_name, ok := c.Get("queue_name")
	if !ok {
		return "", errors.New("queue name is required")
	}
	return queue_name.(string), nil
}

func (h *httpServerInstance) createQueueHandler(c *gin.Context) {
	queue_name, queueNameErr := getQueueName(c)
	if queueNameErr != nil {
		h.Logger.Error("Error getting queue name", "error", queueNameErr)
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	err := h.Queue.CreateQueue(queue_name)
	if err != nil {
		h.Logger.Error("Error creating queue", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create queue"})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"status": "created"})
}

func (h *httpServerInstance) deleteQueueHandler(c *gin.Context) {
	queue_name, queueNameErr := getQueueName(c)
	if queueNameErr != nil {
		h.Logger.Error("Error getting queue name", "error", queueNameErr)
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	err := h.Queue.DeleteQueue(queue_name)
	if err != nil {
		h.Logger.Error("Error deleting queue", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to delete queue"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "deleted"})
}

func (h *httpServerInstance) enqueueHandler(c *gin.Context) {
	queue_name, queueNameErr := getQueueName(c)
	if queueNameErr != nil {
		h.Logger.Error("Error getting queue name", "error", queueNameErr)
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}

	var req EnqueueRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.Logger.Error("Error binding JSON", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload"})
		return
	}

	err := h.Queue.Enqueue(queue_name, req.Message)
	if err != nil {
		h.Logger.Error("Error enqueuing message", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue message"})
		return
	}

	c.JSON(http.StatusAccepted, EnqueueResponse{
		Status:  "enqueued",
		Message: req.Message,
	})
}

func (h *httpServerInstance) enqueueBatchHandler(c *gin.Context) {
	queue_name, queueNameErr := getQueueName(c)
	if queueNameErr != nil {
		h.Logger.Error("Error getting queue name", "error", queueNameErr)
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}

	var req EnqueueBatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.Logger.Error("Error binding JSON", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload"})
		return
	}
	mode := req.Mode
	if mode != "partialSuccess" && mode != "stopOnFailure" {
		h.Logger.Error("Invalid mode", "mode", mode)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid mode"})
		return
	}

	msgs := make([]interface{}, len(req.Messages))
	for i, msg := range req.Messages {
		msgs[i] = msg
	}

	chunkedMsgs := util.ChunkSlice(msgs, 100) // Chunk size of 100
	var totalSuccess int64 = 0
	switch mode {
	case "stopOnFailure":
		for _, chunk := range chunkedMsgs {
			successCount, err := h.Queue.EnqueueBatch(queue_name, chunk)
			if err != nil {
				h.Logger.Error("Error enqueuing messages", "error", err)
				c.JSON(http.StatusAccepted, EnqueueBatchResponse{
					Status:       "enqueued",
					SuccessCount: totalSuccess + successCount,
					FailureCount: int64(len(req.Messages)) - (totalSuccess + successCount),
					// No failed messages in stopOnFailure mode
				})
				return
			}
			totalSuccess += successCount
			if successCount < int64(len(chunk)) {
				// If some messages in the chunk failed, stop processing further
				break
			}
		}
		c.JSON(http.StatusAccepted, EnqueueBatchResponse{
			Status:       "enqueued",
			SuccessCount: totalSuccess,
			FailureCount: int64(len(req.Messages)) - totalSuccess,
			// No failed messages in stopOnFailure mode
		})
		return
	case "partialSuccess":
		// partialSuccess mode
		failedMessages := []FailedMessage{}
		for _, chunk := range chunkedMsgs {
			successCount, err := h.Queue.EnqueueBatch(queue_name, chunk)
			if err != nil {
				h.Logger.Error("Error enqueuing messages in chunk", "error", err)
				// Mark all messages in this chunk as failed
				startIndex := successCount
				endIndex := int64(len(chunk))
				for i := startIndex; i < endIndex; i++ {
					if i == startIndex {
						failedMessages = append(failedMessages, FailedMessage{
							Index:   totalSuccess + i,
							Message: string(chunk[i].(json.RawMessage)),
							Error:   err.Error(),
						})
					} else {
						failedMessages = append(failedMessages, FailedMessage{
							Index:   totalSuccess + i,
							Message: string(chunk[i].(json.RawMessage)),
							Error:   fmt.Errorf("skipped due to previous error").Error(),
						})
					}
				}
			} else {
				totalSuccess += successCount
			}
		}
		c.JSON(http.StatusAccepted, EnqueueBatchResponse{
			Status:         "enqueued",
			SuccessCount:   totalSuccess,
			FailureCount:   int64(len(req.Messages)) - totalSuccess,
			FailedMessages: failedMessages,
		})
		return
	default:
		h.Logger.Error("Invalid mode", "mode", mode)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid mode"})
		return
	}
}

func (h *httpServerInstance) dequeueHandler(c *gin.Context) {
	queue_name, queueNameErr := getQueueName(c)
	if queueNameErr != nil {
		h.Logger.Error("Error getting queue name", "error", queueNameErr)
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	var req DequeueRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.Logger.Error("Error binding JSON", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload"})
		return
	}

	message, err := h.Queue.Dequeue(queue_name, req.Group, req.ConsumerID)
	if err != nil {
		if errors.Is(err, queue_error.ErrEmpty) {
			c.Status(http.StatusNoContent)
		} else if errors.Is(err, queue_error.ErrContended) {
			h.Logger.Error("Error dequeuing message", "error", err)
			c.JSON(http.StatusConflict, gin.H{"status": "message is being processed"})
		} else {
			h.Logger.Error("Error dequeuing message", "error", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": "failed to dequeue message"})
		}
		return
	}
	c.JSON(http.StatusOK, DequeueResponse{
		Status: "dequeued",
		Message: DequeueMessage{
			Payload: message.Payload,
			Receipt: message.Receipt,
			ID:      message.ID,
		},
	})
}

func (h *httpServerInstance) ackHandler(c *gin.Context) {
	queue_name, queueNameErr := getQueueName(c)
	if queueNameErr != nil {
		h.Logger.Error("Error getting queue name", "error", queueNameErr)
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	var req AckRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.Logger.Error("Error binding JSON", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload"})
		return
	}

	err := h.Queue.Ack(queue_name, req.Group, req.MessageID, req.Receipt)
	if err != nil {
		h.Logger.Error("Error ACKing message", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to ack message"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ack ok"})
}

func (h *httpServerInstance) nackHandler(c *gin.Context) {
	queue_name, queueNameErr := getQueueName(c)
	if queueNameErr != nil {
		h.Logger.Error("Error getting queue name", "error", queueNameErr)
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	var req NackRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.Logger.Error("Error binding JSON", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload"})
		return
	}

	err := h.Queue.Nack(queue_name, req.Group, req.MessageID, req.Receipt)
	if err != nil {
		h.Logger.Error("Error NACKing message", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to nack message"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "nack ok"})
}

func (h *httpServerInstance) statusHandler(c *gin.Context) {
	queue_name, queueNameErr := getQueueName(c)
	if queueNameErr != nil {
		h.Logger.Error("Error getting queue name", "error", queueNameErr)
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	status, err := h.Queue.Status(queue_name)
	if err != nil {
		h.Logger.Error("Error getting queue status", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get queue status"})
		return
	}
	queueStatus := QueueStatus{
		QueueType:        status.QueueType,
		TotalMessages:    status.TotalMessages,
		AckedMessages:    status.AckedMessages,
		InflightMessages: status.InflightMessages,
		DLQMessages:      status.DLQMessages,
	}
	c.JSON(http.StatusOK, StatusResponse{
		Status:      "ok",
		QueueStatus: queueStatus,
	})
}

func (h *httpServerInstance) peekHandler(c *gin.Context) {
	queue_name, queueNameErr := getQueueName(c)
	if queueNameErr != nil {
		h.Logger.Error("Error getting queue name", "error", queueNameErr)
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	var req PeekRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.Logger.Error("Error binding JSON", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload"})
		return
	}

	message, err := h.Queue.Peek(queue_name, req.Group)
	if err != nil {
		if errors.Is(err, queue_error.ErrEmpty) {
			c.Status(http.StatusNoContent)
			return
		}
		h.Logger.Error("Error peeking message", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to peek message"})
		return
	}

	c.JSON(http.StatusOK, PeekResponse{
		Status: "ok",
		Message: DequeueMessage{
			Payload: message.Payload,
			Receipt: message.Receipt,
			ID:      message.ID,
		},
	})
}

func (h *httpServerInstance) renewHandler(c *gin.Context) {
	queue_name, queueNameErr := getQueueName(c)
	if queueNameErr != nil {
		h.Logger.Error("Error getting queue name", "error", queueNameErr)
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	var req RenewRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.Logger.Error("Error binding JSON", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload"})
		return
	}

	err := h.Queue.Renew(queue_name, req.Group, req.MessageID, req.Receipt, req.ExtendSec)
	if err != nil {
		if errors.Is(err, queue_error.ErrLeaseExpired) {
			c.JSON(http.StatusConflict, gin.H{"status": "lease expired"})
		} else {
			h.Logger.Error("Error renewing message", "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to renew message"})
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "renewed"})
}
