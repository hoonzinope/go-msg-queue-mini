package http

import (
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
		util.Error(queueNameErr.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	err := h.Queue.CreateQueue(queue_name)
	if err != nil {
		util.Error(fmt.Sprintf("Error creating queue: %v", err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create queue"})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"status": "created"})
}

func (h *httpServerInstance) deleteQueueHandler(c *gin.Context) {
	queue_name, queueNameErr := getQueueName(c)
	if queueNameErr != nil {
		util.Error(queueNameErr.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	err := h.Queue.DeleteQueue(queue_name)
	if err != nil {
		util.Error(fmt.Sprintf("Error deleting queue: %v", err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to delete queue"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "deleted"})
}

func (h *httpServerInstance) enqueueHandler(c *gin.Context) {
	queue_name, queueNameErr := getQueueName(c)
	if queueNameErr != nil {
		util.Error(queueNameErr.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}

	var req EnqueueRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		util.Error(fmt.Sprintf("Error binding JSON: %v", err))
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload"})
		return
	}

	err := h.Queue.Enqueue(queue_name, req.Message)
	if err != nil {
		util.Error(fmt.Sprintf("Error enqueuing message: %v", err))
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
		util.Error(queueNameErr.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}

	var req EnqueueBatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		util.Error(fmt.Sprintf("Error binding JSON: %v", err))
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload"})
		return
	}

	msgs := make([]interface{}, len(req.Messages))
	for i, msg := range req.Messages {
		msgs[i] = msg
	}

	successCount, err := h.Queue.EnqueueBatch(queue_name, msgs)
	if err != nil {
		util.Error(fmt.Sprintf("Error enqueuing messages: %v", err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue messages"})
		return
	}

	c.JSON(http.StatusAccepted, EnqueueBatchResponse{
		Status:       "enqueued",
		SuccessCount: successCount,
	})
}

func (h *httpServerInstance) dequeueHandler(c *gin.Context) {
	queue_name, queueNameErr := getQueueName(c)
	if queueNameErr != nil {
		util.Error(queueNameErr.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	var req DequeueRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		util.Error(fmt.Sprintf("Error binding JSON: %v", err))
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload"})
		return
	}

	message, err := h.Queue.Dequeue(queue_name, req.Group, req.ConsumerID)
	if err != nil {
		if errors.Is(err, queue_error.ErrEmpty) {
			c.Status(http.StatusNoContent)
		} else if errors.Is(err, queue_error.ErrContended) {
			util.Error(fmt.Sprintf("Error dequeuing message: %v", err))
			c.JSON(http.StatusConflict, gin.H{"status": "message is being processed"})
		} else {
			util.Error(fmt.Sprintf("Error dequeuing message: %v", err))
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
		util.Error(queueNameErr.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	var req AckRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		util.Error(fmt.Sprintf("Error binding JSON: %v", err))
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload"})
		return
	}

	err := h.Queue.Ack(queue_name, req.Group, req.MessageID, req.Receipt)
	if err != nil {
		util.Error(fmt.Sprintf("Error ACKing message: %v", err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to ack message"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ack ok"})
}

func (h *httpServerInstance) nackHandler(c *gin.Context) {
	queue_name, queueNameErr := getQueueName(c)
	if queueNameErr != nil {
		util.Error(queueNameErr.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	var req NackRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		util.Error(fmt.Sprintf("Error binding JSON: %v", err))
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload"})
		return
	}

	err := h.Queue.Nack(queue_name, req.Group, req.MessageID, req.Receipt)
	if err != nil {
		util.Error(fmt.Sprintf("Error NACKing message: %v", err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to nack message"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "nack ok"})
}

func (h *httpServerInstance) statusHandler(c *gin.Context) {
	queue_name, queueNameErr := getQueueName(c)
	if queueNameErr != nil {
		util.Error(queueNameErr.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	status, err := h.Queue.Status(queue_name)
	if err != nil {
		util.Error(fmt.Sprintf("Error getting queue status: %v", err))
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
		util.Error(queueNameErr.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	var req PeekRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		util.Error(fmt.Sprintf("Error binding JSON: %v", err))
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload"})
		return
	}

	message, err := h.Queue.Peek(queue_name, req.Group)
	if err != nil {
		if errors.Is(err, queue_error.ErrEmpty) {
			c.Status(http.StatusNoContent)
			return
		}
		util.Error(fmt.Sprintf("Error peeking message: %v", err))
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
		util.Error(queueNameErr.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	var req RenewRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		util.Error(fmt.Sprintf("Error binding JSON: %v", err))
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload"})
		return
	}

	err := h.Queue.Renew(queue_name, req.Group, req.MessageID, req.Receipt, req.ExtendSec)
	if err != nil {
		if errors.Is(err, queue_error.ErrLeaseExpired) {
			c.JSON(http.StatusConflict, gin.H{"status": "lease expired"})
		} else {
			util.Error(fmt.Sprintf("Error renewing message: %v", err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to renew message"})
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "renewed"})
}
