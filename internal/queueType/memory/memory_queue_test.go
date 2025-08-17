package memory

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMemoryQueue_Nack_Retry(t *testing.T) {
	// Setup
	maxRetry := 3
	q := NewMemoryQueue(maxRetry)
	consumerID := "test-consumer"
	msgContent := "test-message"

	q.Enqueue(msgContent)
	msgs, err := q.Dequeue(consumerID, 1)
	assert.NoError(t, err)
	assert.Len(t, msgs, 1)
	msg := msgs[0]

	// Nack 2 times (less than maxRetry)
	err = q.Nack(consumerID, msg.Id)
	assert.NoError(t, err)
	err = q.Nack(consumerID, msg.Id)
	assert.NoError(t, err)

	// Assertions
	assert.Equal(t, 2, q.retryMap[consumerID][msg.Id], "Retry count should be 2")
	assert.Len(t, q.dlq, 0, "DLQ should be empty")

	// Dequeue again, should get the same message
	// Note: Dequeue updates offset, so we need to reset it for this test
	delete(q.offsetMap, consumerID)
	dequeuedMsgs, err := q.Dequeue(consumerID, 1)
	assert.NoError(t, err)
	assert.Len(t, dequeuedMsgs, 1)
	assert.Equal(t, msg.Id, dequeuedMsgs[0].Id, "Should dequeue the same message again")
}

func TestMemoryQueue_Nack_MoveToDLQ(t *testing.T) {
	// Setup
	maxRetry := 2
	q := NewMemoryQueue(maxRetry)
	consumerID := "test-consumer-dlq"
	msgContent := "test-message-dlq"

	q.Enqueue(msgContent)
	msgs, err := q.Dequeue(consumerID, 1)
	assert.NoError(t, err)
	assert.Len(t, msgs, 1)
	msg := msgs[0]

	// Nack until it moves to DLQ
	for i := 0; i <= maxRetry; i++ {
		err = q.Nack(consumerID, msg.Id)
		assert.NoError(t, err)
	}

	// Assertions
	assert.Len(t, q.dlq, 1, "DLQ should contain one message")
	assert.Equal(t, msg.Id, q.dlq[0].Id, "The correct message should be in the DLQ")
	_, exists := q.retryMap[consumerID][msg.Id]
	assert.False(t, exists, "Message should be removed from retry map")

	// The message should be considered "acked" by updating the offset, so it shouldn't be dequeued again
	dequeuedMsgs, err := q.Dequeue(consumerID, 1)
	assert.Error(t, err, "Queue should be empty or have no new messages")
	assert.Len(t, dequeuedMsgs, 0)
}

func TestMemoryQueue_AckAfterNack(t *testing.T) {
	// Setup
	maxRetry := 3
	q := NewMemoryQueue(maxRetry)
	consumerID := "test-consumer-ack"
	msgContent := "test-message-ack"

	q.Enqueue(msgContent)
	msgs, err := q.Dequeue(consumerID, 1)
	assert.NoError(t, err)
	msg := msgs[0]

	// Nack once
	err = q.Nack(consumerID, msg.Id)
	assert.NoError(t, err)

	// Assert retry count
	assert.Equal(t, 1, q.retryMap[consumerID][msg.Id])

	// Dequeue again and Ack
	// To dequeue the same message, we need to manipulate the offset as Dequeue doesn't guarantee it
	time.Sleep(1 * time.Millisecond) // Ensure next message has a different ID if we were to enqueue another
	q.offsetMap[consumerID] = -1
	dequeuedMsgs, err := q.Dequeue(consumerID, 1)
	assert.NoError(t, err)
	assert.Equal(t, msg.Id, dequeuedMsgs[0].Id)

	err = q.Ack(consumerID, msg.Id)
	assert.NoError(t, err)

	// Assertions
	assert.Empty(t, q.items, "Queue should be empty after Ack")
}