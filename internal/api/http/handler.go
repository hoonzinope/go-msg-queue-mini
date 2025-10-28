package http

import (
	"encoding/json"
	"errors"
	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/internal/queue_error"
	"go-msg-queue-mini/util"
	"net/http"

	"github.com/gin-gonic/gin"
)

const peekMaxLimit = 100
const peekMsgPreviewLength = 50

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

func getMessageID(c *gin.Context) (int64, error) {
	messageID, ok := c.Get("message_id")
	if !ok {
		return 0, errors.New("message ID is required")
	}
	return messageID.(int64), nil
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
	enqueueMsg := internal.EnqueueMessage{
		Item:            req.Message,
		Delay:           req.Delay,
		DeduplicationID: req.DeduplicationID,
	}
	err := h.Queue.Enqueue(queue_name, enqueueMsg)
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

	msgs := make([]internal.EnqueueMessage, len(req.Messages))
	for i, msg := range req.Messages {
		msgs[i] = internal.EnqueueMessage{
			Item:            msg.Message,
			Delay:           msg.Delay,
			DeduplicationID: msg.DeduplicationID,
		}
	}

	batchResult, err := h.Queue.EnqueueBatch(queue_name, mode, msgs)
	if err != nil {
		h.Logger.Error("Error enqueuing messages", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue messages"})
		return
	}

	resp := EnqueueBatchResponse{
		Status:       "enqueued",
		SuccessCount: batchResult.SuccessCount,
		FailureCount: batchResult.FailedCount,
	}
	if len(batchResult.FailedMessages) > 0 {
		resp.FailedMessages = make([]FailedMessage, len(batchResult.FailedMessages))
		for i, fm := range batchResult.FailedMessages {
			resp.FailedMessages[i] = FailedMessage{
				Index:   fm.Index,
				Message: fm.Message,
				Error:   fm.Reason,
			}
		}
	}

	c.JSON(http.StatusAccepted, resp)
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
	status, err := h.QueueInspector.Status(queue_name)
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

	// if peekRequest.Options is not empty, convert PeekRequest.Options to internal.PeekOptions
	peekOptions := internal.PeekOptions{
		Limit:   1,
		Order:   "asc",
		Cursor:  0,
		Preview: false,
	}
	if req.Options != (PeekOptions{}) {
		if req.Options.Limit > 0 {
			if req.Options.Limit > peekMaxLimit {
				peekOptions.Limit = peekMaxLimit
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

	messages, err := h.QueueInspector.Peek(queue_name, req.Group, peekOptions)
	if err != nil {
		if errors.Is(err, queue_error.ErrEmpty) || errors.Is(err, queue_error.ErrNoMessage) {
			c.Status(http.StatusNoContent)
			return
		}
		h.Logger.Error("Error peeking message", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to peek message"})
		return
	}
	var peekMessages []PeekMessage
	for _, msg := range messages {
		raw := msg.Payload
		var errorMsg string = ""

		// payload preview handling
		if peekOptions.Preview {
			var payloadStr string = util.PreviewStringRuneSafe(string(raw), peekMsgPreviewLength)
			raw, _ = json.Marshal(payloadStr)
		} else {
			var parseErr error
			raw, parseErr = util.ParseBytesToJsonRawMessage(msg.Payload)
			if parseErr != nil {
				h.Logger.Error("Error parsing message payload", "error", parseErr)
				raw = json.RawMessage(`""`)
				errorMsg = "failed to parse message payload as JSON"
			}
		}

		peekMessages = append(peekMessages, PeekMessage{
			ID:         msg.ID,
			Payload:    raw,
			Receipt:    msg.Receipt,
			InsertedAt: msg.InsertedAt,
			ErrorMsg:   errorMsg,
		})
	}

	c.JSON(http.StatusOK, PeekResponse{
		Status:   "ok",
		Messages: peekMessages,
	})
}

func (h *httpServerInstance) renewHandler(c *gin.Context) {
	queue_name, queueNameErr := getQueueName(c)
	if queueNameErr != nil {
		h.Logger.Error("Error getting queue name - renewHandler", "error", queueNameErr)
		c.JSON(http.StatusBadRequest, gin.H{"error": queueNameErr.Error()})
		return
	}
	var req RenewRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.Logger.Error("Error binding JSON - renewHandler", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload"})
		return
	}

	err := h.Queue.Renew(queue_name, req.Group, req.MessageID, req.Receipt, req.ExtendSec)
	if err != nil {
		if errors.Is(err, queue_error.ErrLeaseExpired) {
			c.JSON(http.StatusConflict, gin.H{"status": "lease expired"})
		} else {
			h.Logger.Error("Error renewing message - renewHandler", "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to renew message"})
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "renewed"})
}

func (h *httpServerInstance) statusAllHandler(c *gin.Context) {
	statusMap, err := h.QueueInspector.StatusAll()
	if err != nil {
		h.Logger.Error("Error getting all queue status - statusAllHandler", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get all queue status"})
		return
	}
	responseMap := make(map[string]QueueStatus)
	for queueName, status := range statusMap {
		responseMap[queueName] = QueueStatus{
			QueueType:        status.QueueType,
			TotalMessages:    status.TotalMessages,
			AckedMessages:    status.AckedMessages,
			InflightMessages: status.InflightMessages,
			DLQMessages:      status.DLQMessages,
		}
	}
	c.JSON(http.StatusOK, StatusAllResponse{
		Status:      "ok",
		AllQueueMap: responseMap,
	})
}

func (h *httpServerInstance) detailHandler(c *gin.Context) {
	queue_name := c.Param("queue_name")
	message_id := c.Param("message_id")
	if queue_name == "" {
		h.Logger.Error("Queue name is required - detailHandler")
		c.JSON(http.StatusBadRequest, gin.H{"error": "queue name is required"})
		return
	}
	if message_id == "" {
		h.Logger.Error("Message ID is required - detailHandler")
		c.JSON(http.StatusBadRequest, gin.H{"error": "message ID is required"})
		return
	}
	message_id_num := util.ParseStringToInt64(message_id, 0)
	if message_id_num == 0 {
		h.Logger.Error("Invalid message ID - detailHandler", "message_id", message_id)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid message ID"})
		return
	}

	message, err := h.QueueInspector.Detail(queue_name, message_id_num)
	if err != nil {
		h.Logger.Error("Error getting message detail - detailHandler", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get message detail"})
		return
	}

	var detailMessage PeekMessage
	var errorMsg string = ""
	payload, parseErr := util.ParseBytesToJsonRawMessage(message.Payload)
	if parseErr != nil {
		h.Logger.Error("Error parsing message payload - detailHandler", "error", parseErr)
		payload = json.RawMessage(`""`)
		errorMsg = "failed to parse message payload as JSON"
	}

	detailMessage = PeekMessage{
		ID:         message.ID,
		Payload:    payload,
		Receipt:    message.Receipt,
		InsertedAt: message.InsertedAt,
		ErrorMsg:   errorMsg,
	}
	c.JSON(http.StatusOK, DetailResponse{
		Status:  "ok",
		Message: detailMessage,
	})
}
