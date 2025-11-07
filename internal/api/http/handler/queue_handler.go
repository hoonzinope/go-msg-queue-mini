package handler

import (
	"go-msg-queue-mini/internal"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
)

type QueueHandler struct {
	Queue  internal.Queue
	Logger *slog.Logger
}

func (queueHandler *QueueHandler) CreateQueueHandler(c *gin.Context) {
	queue_name, queueNameErr := GetQueueName(c)
	if queueNameErr != nil {
		queueHandler.Logger.Error("Error getting queue name", "error", queueNameErr)
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	err := queueHandler.Queue.CreateQueue(queue_name)
	if err != nil {
		queueHandler.Logger.Error("Error creating queue", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create queue"})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"status": "created"})
}

func (queueHandler *QueueHandler) DeleteQueueHandler(c *gin.Context) {
	queue_name, queueNameErr := GetQueueName(c)
	if queueNameErr != nil {
		queueHandler.Logger.Error("Error getting queue name", "error", queueNameErr)
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	err := queueHandler.Queue.DeleteQueue(queue_name)
	if err != nil {
		queueHandler.Logger.Error("Error deleting queue", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to delete queue"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "deleted"})
}

func (queueHandler *QueueHandler) HealthCheckHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
