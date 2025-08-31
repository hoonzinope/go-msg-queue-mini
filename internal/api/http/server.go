package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/internal/core"
	"net/http"

	"github.com/gin-gonic/gin"
)

type httpServerInstance struct {
	Addr  string
	Queue internal.Queue
}

func StartServer(ctx context.Context, config *internal.Config, queue internal.Queue) error {
	// Start the HTTP server
	addr := config.HTTP.Port
	if addr == 0 {
		addr = 8080
	}
	httpServerInstance := &httpServerInstance{
		Addr:  fmt.Sprintf(":%d", addr),
		Queue: queue,
	}

	server := &http.Server{
		Addr:    httpServerInstance.Addr,
		Handler: router(httpServerInstance),
	}

	go func() {
		<-ctx.Done()
		err := server.Shutdown(context.Background())
		if err != nil {
			fmt.Println("Error shutting down server:", err)
		}
	}()

	err := server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("error starting server: %w", err)
	}
	return nil
}

func router(httpServerInstance *httpServerInstance) *gin.Engine {
	r := gin.Default()
	r.GET("/api/v1/health", healthCheckHandler)
	r.POST("/api/v1/enqueue", httpServerInstance.enqueueHandler)
	r.POST("/api/v1/dequeue", httpServerInstance.dequeueHandler)
	r.POST("/api/v1/ack", httpServerInstance.ackHandler)
	r.POST("/api/v1/nack", httpServerInstance.nackHandler)
	r.GET("/api/v1/status", httpServerInstance.statusHandler)
	r.POST("/api/v1/peek", httpServerInstance.peekHandler)
	r.POST("/api/v1/renew", httpServerInstance.renewHandler)
	return r
}

func healthCheckHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

type EnqueueRequest struct {
	Group   string          `json:"group" binding:"required"`
	Message json.RawMessage `json:"message" binding:"required"`
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
	c.JSON(http.StatusAccepted, gin.H{"status": "enqueued", "message": req.Message})
}

type DequeueRequest struct {
	Group      string `json:"group" binding:"required"`
	ConsumerID string `json:"consumer_id" binding:"required"`
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
	c.JSON(http.StatusOK, gin.H{"status": "dequeued", "message": message})
}

type AckRequest struct {
	Group     string `json:"group" binding:"required"`
	MessageID int64  `json:"message_id" binding:"required"`
	Receipt   string `json:"receipt" binding:"required"`
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
	c.JSON(http.StatusOK, gin.H{"status": "acknowledged"})
}

type NackRequest struct {
	Group     string `json:"group" binding:"required"`
	MessageID int64  `json:"message_id" binding:"required"`
	Receipt   string `json:"receipt" binding:"required"`
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
	c.JSON(http.StatusOK, gin.H{"status": "not acknowledged"})
}

func (h *httpServerInstance) statusHandler(c *gin.Context) {
	status, err := h.Queue.Status()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "queue_status": status})
}

type PeekRequest struct {
	Group string `json:"group" binding:"required"`
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
	c.JSON(http.StatusOK, gin.H{"status": "ok", "message": message})
}

type RenewRequest struct {
	Group     string `json:"group" binding:"required"`
	MessageID int64  `json:"message_id" binding:"required"`
	Receipt   string `json:"receipt" binding:"required"`
	ExtendSec int    `json:"extend_sec" binding:"required"`
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
