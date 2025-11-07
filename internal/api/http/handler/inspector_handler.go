package handler

import (
	"encoding/json"
	"errors"
	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/internal/api/http/dto"
	"go-msg-queue-mini/internal/queue_error"
	"go-msg-queue-mini/util"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
)

type InspectHandler struct {
	QueueInspector internal.QueueInspector
	Logger         *slog.Logger
}

func (ih *InspectHandler) StatusHandler(c *gin.Context) {
	queue_name, queueNameErr := GetQueueName(c)
	if queueNameErr != nil {
		ih.Logger.Error("Error getting queue name", "error", queueNameErr)
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	status, err := ih.QueueInspector.Status(queue_name)
	if err != nil {
		ih.Logger.Error("Error getting queue status", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get queue status"})
		return
	}
	queueStatus := dto.QueueStatus{
		QueueType:        status.QueueType,
		TotalMessages:    status.TotalMessages,
		AckedMessages:    status.AckedMessages,
		InflightMessages: status.InflightMessages,
		DLQMessages:      status.DLQMessages,
	}
	c.JSON(http.StatusOK, dto.StatusResponse{
		Status:      "ok",
		QueueStatus: queueStatus,
	})
}

func (ih *InspectHandler) PeekHandler(c *gin.Context) {
	queue_name, queueNameErr := GetQueueName(c)
	if queueNameErr != nil {
		ih.Logger.Error("Error getting queue name", "error", queueNameErr)
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	var req dto.PeekRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		ih.Logger.Error("Error binding JSON", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload"})
		return
	}

	// if peekRequest.Options is not empty, convert PeekRequest.Options to internal.PeekOptions
	peekOptions := internal.PeekOptions{
		Limit:   1,
		Order:   "asc",
		Cursor:  0,
		Preview: false,
	}
	if req.Options != (dto.PeekOptions{}) {
		if req.Options.Limit > 0 {
			if req.Options.Limit > dto.PeekMaxLimit {
				peekOptions.Limit = dto.PeekMaxLimit
			} else {
				peekOptions.Limit = req.Options.Limit
			}
		}
		if req.Options.Order != "" {
			peekOptions.Order = req.Options.Order
		}
		if req.Options.Cursor > 0 {
			peekOptions.Cursor = req.Options.Cursor
		}
		if req.Options.Preview {
			peekOptions.Preview = req.Options.Preview
		}
	}

	messages, err := ih.QueueInspector.Peek(queue_name, req.Group, peekOptions)
	if err != nil {
		if errors.Is(err, queue_error.ErrEmpty) || errors.Is(err, queue_error.ErrNoMessage) {
			c.JSON(http.StatusNoContent, gin.H{"status": "queue is empty"})
			return
		}
		ih.Logger.Error("Error peeking message", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to peek message"})
		return
	}
	var peekMessages []dto.PeekMessage
	for _, msg := range messages {
		raw := msg.Payload
		var errorMsg string = ""

		// payload preview handling
		if peekOptions.Preview {
			var payloadStr string = util.PreviewStringRuneSafe(string(raw), dto.PeekMsgPreviewLength)
			raw, _ = json.Marshal(payloadStr)
		} else {
			var parseErr error
			raw, parseErr = util.ParseBytesToJsonRawMessage(msg.Payload)
			if parseErr != nil {
				ih.Logger.Error("Error parsing message payload", "error", parseErr)
				raw = json.RawMessage(`""`)
				errorMsg = "failed to parse message payload as JSON"
			}
		}

		peekMessages = append(peekMessages, dto.PeekMessage{
			ID:         msg.ID,
			Payload:    raw,
			Receipt:    msg.Receipt,
			InsertedAt: msg.InsertedAt,
			ErrorMsg:   errorMsg,
		})
	}

	c.JSON(http.StatusOK, dto.PeekResponse{
		Status:   "ok",
		Messages: peekMessages,
	})
}

func (ih *InspectHandler) StatusAllHandler(c *gin.Context) {
	statusMap, err := ih.QueueInspector.StatusAll()
	if err != nil {
		ih.Logger.Error("Error getting all queue status - statusAllHandler", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get all queue status"})
		return
	}
	responseMap := make(map[string]dto.QueueStatus)
	for queueName, status := range statusMap {
		responseMap[queueName] = dto.QueueStatus{
			QueueType:        status.QueueType,
			TotalMessages:    status.TotalMessages,
			AckedMessages:    status.AckedMessages,
			InflightMessages: status.InflightMessages,
			DLQMessages:      status.DLQMessages,
		}
	}
	c.JSON(http.StatusOK, dto.StatusAllResponse{
		Status:      "ok",
		AllQueueMap: responseMap,
	})
}

func (ih *InspectHandler) DetailHandler(c *gin.Context) {
	queue_name := c.Param("queue_name")
	message_id := c.Param("message_id")
	if queue_name == "" {
		ih.Logger.Error("Queue name is required - detailHandler")
		c.JSON(http.StatusBadRequest, gin.H{"error": "queue name is required"})
		return
	}
	if message_id == "" {
		ih.Logger.Error("Message ID is required - detailHandler")
		c.JSON(http.StatusBadRequest, gin.H{"error": "message ID is required"})
		return
	}
	message_id_num := util.ParseStringToInt64(message_id, 0)
	if message_id_num == 0 {
		ih.Logger.Error("Invalid message ID - detailHandler", "message_id", message_id)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid message ID"})
		return
	}

	message, err := ih.QueueInspector.Detail(queue_name, message_id_num)
	if err != nil {
		ih.Logger.Error("Error getting message detail - detailHandler", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get message detail"})
		return
	}

	var detailMessage dto.PeekMessage
	var errorMsg string = ""
	payload, parseErr := util.ParseBytesToJsonRawMessage(message.Payload)
	if parseErr != nil {
		ih.Logger.Error("Error parsing message payload - detailHandler", "error", parseErr)
		payload = json.RawMessage(`""`)
		errorMsg = "failed to parse message payload as JSON"
	}

	detailMessage = dto.PeekMessage{
		ID:         message.ID,
		Payload:    payload,
		Receipt:    message.Receipt,
		InsertedAt: message.InsertedAt,
		ErrorMsg:   errorMsg,
	}
	c.JSON(http.StatusOK, dto.DetailResponse{
		Status:  "ok",
		Message: detailMessage,
	})
}
