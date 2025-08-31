package http

import (
	"errors"
	"go-msg-queue-mini/internal/core"
	"net/http"

	"github.com/gin-gonic/gin"
)

func healthCheckHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *httpServerInstance) enqueueHandler(c *gin.Context) {
	var req EnqueueRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err := h.Queue.Enqueue(req.Message)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, EnqueueResponse{
		Status:  "enqueued",
		Message: req.Message,
	})
}

func (h *httpServerInstance) dequeueHandler(c *gin.Context) {
	var req DequeueRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	message, err := h.Queue.Dequeue(req.Group, req.ConsumerID)
	if err != nil {
		if errors.Is(err, core.ErrEmpty) {
			c.JSON(http.StatusNoContent, gin.H{"status": "no messages"})
		} else if errors.Is(err, core.ErrContended) {
			c.JSON(http.StatusConflict, gin.H{"status": "message is being processed"})
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
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
	var req AckRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err := h.Queue.Ack(req.Group, req.MessageID, req.Receipt)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ack ok"})
}

func (h *httpServerInstance) nackHandler(c *gin.Context) {
	var req NackRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err := h.Queue.Nack(req.Group, req.MessageID, req.Receipt)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "nack ok"})
}

func (h *httpServerInstance) statusHandler(c *gin.Context) {
	status, err := h.Queue.Status()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	queueStatus := QueueStatus{
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
	var req PeekRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	message, err := h.Queue.Peek(req.Group)
	if err != nil {
		if errors.Is(err, core.ErrEmpty) {
			c.Status(http.StatusNoContent)
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
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
	var req RenewRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err := h.Queue.Renew(req.Group, req.MessageID, req.Receipt, req.ExtendSec)
	if err != nil {
		if errors.Is(err, core.ErrLeaseExpired) {
			c.JSON(http.StatusConflict, gin.H{"status": "lease expired"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "renewed"})
}
