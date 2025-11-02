package handler

import (
	"fmt"
	"go-msg-queue-mini/internal"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
)

type DLQHandler struct {
	QueueInspector internal.QueueInspector
	Logger         *slog.Logger
}

type PeekOptions struct {
	Limit   int    `json:"limit"`   // number of messages to peek
	Cursor  int64  `json:"cursor"`  // for pagination
	Order   string `json:"order"`   // "asc" or "desc"
	Preview bool   `json:"preview"` // whether to return full message or just metadata
}

type DLQListRequest struct {
	Options PeekOptions `json:"options"`
}

type DLQListResponse struct {
	Status   string       `json:"status"`
	Messages []DLQMessage `json:"messages"`
}

type DLQDetailResponse struct {
	Status  string     `json:"status"`
	Message DLQMessage `json:"message"`
}

type DLQMessage struct {
	Payload     []byte
	ID          int64
	Reason      string
	FailedGroup string
	InsertedAt  string
}

var defaultDLQPeekOptions = internal.PeekOptions{
	Limit:   100,
	Cursor:  0,
	Order:   "asc",
	Preview: false,
}

func (dlqHandler *DLQHandler) ListDLQMessagesHandler(c *gin.Context) {
	queueName := c.Param("queue_name")

	// if there are query parameters, override the defaults
	options := ParseQueryOptions(c)

	messages, err := dlqHandler.QueueInspector.ListDLQ(queueName, options)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(200, DLQListResponse{
		Status:   "ok",
		Messages: convertDLQMessages(messages),
	})
}

func (dlqHandler *DLQHandler) DetailDLQMessageHandler(c *gin.Context) {
	queueName := c.Param("queue_name")
	messageIDParam := c.Param("message_id")

	var messageID int64
	_, err := fmt.Sscan(messageIDParam, &messageID)
	if err != nil {
		dlqHandler.Logger.Error("Error parsing message ID", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid message ID"})
		return
	}

	message, err := dlqHandler.QueueInspector.DetailDLQ(queueName, messageID)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(200, DLQDetailResponse{
		Status:  "ok",
		Message: convertDLQMessage(message),
	})
}

func convertDLQMessages(internalMessages []internal.DLQMessage) []DLQMessage {
	dlqMessages := make([]DLQMessage, len(internalMessages))
	for i, msg := range internalMessages {
		dlqMessages[i] = convertDLQMessage(msg)
	}
	return dlqMessages
}

func convertDLQMessage(internalMessage internal.DLQMessage) DLQMessage {
	return DLQMessage{
		Payload:     internalMessage.Payload,
		ID:          internalMessage.ID,
		Reason:      internalMessage.Reason,
		FailedGroup: internalMessage.FailedGroup,
		InsertedAt:  internalMessage.InsertedAt.Format("2006-01-02 15:04:05"),
	}
}

func ParseQueryOptions(c *gin.Context) internal.PeekOptions {
	options := defaultDLQPeekOptions

	limitParam := c.Query("limit")
	if limitParam != "" {
		var limit int
		_, err := fmt.Sscan(limitParam, &limit)
		if err == nil && limit > 0 {
			options.Limit = limit
		}
	}

	cursorParam := c.Query("cursor")
	if cursorParam != "" {
		var cursor int64
		_, err := fmt.Sscan(cursorParam, &cursor)
		if err == nil && cursor >= 0 {
			options.Cursor = cursor
		}
	}

	orderParam := c.Query("order")
	if orderParam == "asc" || orderParam == "desc" {
		options.Order = orderParam
	}

	previewParam := c.Query("preview")
	if previewParam == "true" {
		options.Preview = true
	}

	return options
}
