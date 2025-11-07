package handler

import (
	"errors"

	"github.com/gin-gonic/gin"
)

func GetQueueName(c *gin.Context) (string, error) {
	queue_name, ok := c.Get("queue_name")
	if !ok {
		return "", errors.New("queue name is required")
	}
	return queue_name.(string), nil
}

func GetMessageID(c *gin.Context) (int64, error) {
	messageID, ok := c.Get("message_id")
	if !ok {
		return 0, errors.New("message ID is required")
	}
	return messageID.(int64), nil
}
