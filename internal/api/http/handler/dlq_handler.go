package handler

import (
	"go-msg-queue-mini/internal"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

type DLQListRequest struct {
	Options internal.PeekOptions `json:"options"`
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

type DLQHandler struct {
	QueueInspector internal.QueueInspector
	Logger         *slog.Logger
}

var defaultDLQPeekOptions = internal.PeekOptions{
	Limit:   10,
	Cursor:  0,
	Order:   "asc",
	Preview: false,
}

var MAX_DLQ_PEEK_LIMIT = 100

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

	messageID, err := strconv.ParseInt(messageIDParam, 10, 64)
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

	if limitParam := c.Query("limit"); limitParam != "" {
		limit, err := strconv.Atoi(limitParam)
		if err == nil && limit > 0 {
			if limit > MAX_DLQ_PEEK_LIMIT {
				limit = MAX_DLQ_PEEK_LIMIT
			}
			options.Limit = limit
		}
	}

	if cursorParam := c.Query("cursor"); cursorParam != "" {
		cursor, err := strconv.ParseInt(cursorParam, 10, 64)
		if err == nil && cursor >= 0 {
			options.Cursor = cursor
		}
	}

	if orderParam := c.Query("order"); orderParam == "asc" || orderParam == "desc" {
		options.Order = orderParam
	}

	if previewParam := c.Query("preview"); previewParam != "" {
		if preview, err := strconv.ParseBool(previewParam); err == nil && preview {
			options.Preview = true
		}
	}

	return options
}
