package handler

import (
	"errors"
	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/internal/api/http/dto"
	"go-msg-queue-mini/internal/queue_error"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
)

type ConsumerHandler struct {
	Queue  internal.Queue
	Logger *slog.Logger
}

func (consumerHandler *ConsumerHandler) DequeueHandler(c *gin.Context) {
	queue_name, queueNameErr := GetQueueName(c)
	if queueNameErr != nil {
		consumerHandler.Logger.Error("Error getting queue name", "error", queueNameErr)
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	var req dto.DequeueRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		consumerHandler.Logger.Error("Error binding JSON", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload"})
		return
	}

	message, err := consumerHandler.Queue.Dequeue(queue_name, req.Group, req.ConsumerID)
	if err != nil {
		if errors.Is(err, queue_error.ErrEmpty) {
			c.JSON(http.StatusNoContent, gin.H{"status": "queue is empty"})
		} else if errors.Is(err, queue_error.ErrContended) {
			consumerHandler.Logger.Error("Error dequeuing message", "error", err)
			c.JSON(http.StatusConflict, gin.H{"status": "message is being processed"})
		} else {
			consumerHandler.Logger.Error("Error dequeuing message", "error", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": "failed to dequeue message"})
		}
		return
	}
	c.JSON(http.StatusOK, dto.DequeueResponse{
		Status: "dequeued",
		Message: dto.DequeueMessage{
			Payload: message.Payload,
			Receipt: message.Receipt,
			ID:      message.ID,
		},
	})
}

func (consumerHandler *ConsumerHandler) AckHandler(c *gin.Context) {
	queue_name, queueNameErr := GetQueueName(c)
	if queueNameErr != nil {
		consumerHandler.Logger.Error("Error getting queue name", "error", queueNameErr)
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	var req dto.AckRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		consumerHandler.Logger.Error("Error binding JSON", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload"})
		return
	}

	err := consumerHandler.Queue.Ack(queue_name, req.Group, req.MessageID, req.Receipt)
	if err != nil {
		consumerHandler.Logger.Error("Error ACKing message", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to ack message"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ack ok"})
}

func (consumerHandler *ConsumerHandler) NackHandler(c *gin.Context) {
	queue_name, queueNameErr := GetQueueName(c)
	if queueNameErr != nil {
		consumerHandler.Logger.Error("Error getting queue name", "error", queueNameErr)
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	var req dto.NackRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		consumerHandler.Logger.Error("Error binding JSON", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload"})
		return
	}

	err := consumerHandler.Queue.Nack(queue_name, req.Group, req.MessageID, req.Receipt)
	if err != nil {
		consumerHandler.Logger.Error("Error NACKing message", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to nack message"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "nack ok"})
}

func (consumerHandler *ConsumerHandler) RenewHandler(c *gin.Context) {
	queue_name, queueNameErr := GetQueueName(c)
	if queueNameErr != nil {
		consumerHandler.Logger.Error("Error getting queue name - renewHandler", "error", queueNameErr)
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	var req dto.RenewRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		consumerHandler.Logger.Error("Error binding JSON - renewHandler", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload"})
		return
	}

	err := consumerHandler.Queue.Renew(queue_name, req.Group, req.MessageID, req.Receipt, req.ExtendSec)
	if err != nil {
		if errors.Is(err, queue_error.ErrLeaseExpired) {
			c.JSON(http.StatusConflict, gin.H{"status": "lease expired"})
		} else {
			consumerHandler.Logger.Error("Error renewing message - renewHandler", "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to renew message"})
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "renewed"})
}
