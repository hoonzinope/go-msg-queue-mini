package handler

import (
	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/internal/api/http/dto"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
)

type ProducerHandler struct {
	Queue  internal.Queue
	Logger *slog.Logger
}

func (producerHandler *ProducerHandler) EnqueueHandler(c *gin.Context) {
	queue_name, queueNameErr := GetQueueName(c)
	if queueNameErr != nil {
		producerHandler.Logger.Error("Error getting queue name", "error", queueNameErr)
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}

	var req dto.EnqueueRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		producerHandler.Logger.Error("Error binding JSON", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload"})
		return
	}
	enqueueMsg := internal.EnqueueMessage{
		Item:            req.Message,
		Delay:           req.Delay,
		DeduplicationID: req.DeduplicationID,
	}
	err := producerHandler.Queue.Enqueue(queue_name, enqueueMsg)
	if err != nil {
		producerHandler.Logger.Error("Error enqueuing message", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue message"})
		return
	}

	c.JSON(http.StatusAccepted, dto.EnqueueResponse{
		Status:  "enqueued",
		Message: req.Message,
	})
}

func (producerHandler *ProducerHandler) EnqueueBatchHandler(c *gin.Context) {
	queue_name, queueNameErr := GetQueueName(c)
	if queueNameErr != nil {
		producerHandler.Logger.Error("Error getting queue name", "error", queueNameErr)
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}

	var req dto.EnqueueBatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		producerHandler.Logger.Error("Error binding JSON", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload"})
		return
	}
	mode := req.Mode
	if mode != "partialSuccess" && mode != "stopOnFailure" {
		producerHandler.Logger.Error("Invalid mode", "mode", mode)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid mode"})
		return
	}

	msgs := make([]internal.EnqueueMessage, len(req.Messages))
	for i, msg := range req.Messages {
		msgs[i] = internal.EnqueueMessage{
			Item:            msg.Message,
			Delay:           msg.Delay,
			DeduplicationID: msg.DeduplicationID,
		}
	}

	batchResult, err := producerHandler.Queue.EnqueueBatch(queue_name, mode, msgs)
	if err != nil {
		producerHandler.Logger.Error("Error enqueuing messages", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue messages"})
		return
	}

	resp := dto.EnqueueBatchResponse{
		Status:       "enqueued",
		SuccessCount: batchResult.SuccessCount,
		FailureCount: batchResult.FailedCount,
	}
	if len(batchResult.FailedMessages) > 0 {
		resp.FailedMessages = make([]dto.FailedMessage, len(batchResult.FailedMessages))
		for i, fm := range batchResult.FailedMessages {
			resp.FailedMessages[i] = dto.FailedMessage{
				Index:   fm.Index,
				Message: fm.Message,
				Error:   fm.Reason,
			}
		}
	}

	c.JSON(http.StatusAccepted, resp)
}
