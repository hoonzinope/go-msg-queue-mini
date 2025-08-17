package memory

import (
	"fmt"
	"go-msg-queue-mini/internal"
	"sync"
	"time"
)

var maxRetry = 3 // Maximum number of retries for message processing

type memoryQueue struct {
	items     []internal.Msg // Use a slice to store messages
	dlq       []internal.Msg // Dead-letter queue for failed messages
	mutex     sync.Mutex
	offsetMap map[string]int           // Offset map for each consumer
	retryMap  map[string]map[int64]int // Retry map to track retries for each message
}

func NewMemoryQueue() *memoryQueue {
	return &memoryQueue{
		items:     make([]internal.Msg, 0),
		dlq:       make([]internal.Msg, 0), // Initialize the dead-letter queue
		mutex:     sync.Mutex{},
		offsetMap: make(map[string]int),           // Initialize the offset map
		retryMap:  make(map[string]map[int64]int), // Initialize the retry map
	}
}

func (q *memoryQueue) Enqueue(item interface{}) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	msg := internal.Msg{
		Id:   time.Now().UnixNano(), // Use current time as a unique ID
		Item: item,
	}
	q.items = append(q.items, msg)
	return nil
}

func (q *memoryQueue) Dequeue(consumerID string, maxCount int) ([]internal.Msg, error) {
	// Acknowledge the item (not used in this implementation, but can be extended)
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if consumerID == "" {
		return nil, fmt.Errorf("consumer ID is empty")
	}
	if maxCount <= 0 {
		return nil, fmt.Errorf("max count must be greater than 0")
	}
	lastOffset, exists := q.offsetMap[consumerID]
	if !exists {
		lastOffset = -1 // If no offset exists, start from the beginning
	}
	messages := make([]internal.Msg, 0, maxCount)
	for i := lastOffset + 1; i < len(q.items) && len(messages) < maxCount; i++ {
		msg := q.items[i]
		messages = append(messages, msg)
	}
	if len(messages) == 0 {
		return nil, fmt.Errorf("no new messages")
	}
	return messages, nil
}

func (q *memoryQueue) Ack(consumerID string, messageID int64) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if consumerID == "" {
		return fmt.Errorf("consumer ID is empty")
	}
	if messageID == 0 {
		return fmt.Errorf("message ID is zero")
	}
	for i, msg := range q.items {
		if msg.Id == messageID {
			q.offsetMap[consumerID] = int(i) // Update the offset for the consumer
			break
		}
	}

	minOffset := -1
	for _, offset := range q.offsetMap {
		if minOffset == -1 || offset < minOffset {
			minOffset = offset // Find the minimum offset across all consumers
		}
	}
	if minOffset >= 0 {
		q.items = q.items[minOffset+1:] // Remove acknowledged messages from the queue
		for consumerID := range q.offsetMap {
			q.offsetMap[consumerID] -= int(minOffset + 1) // Adjust offsets for all consumers
			if q.offsetMap[consumerID] < 0 {
				q.offsetMap[consumerID] = 0 // Ensure offsets do not go negative
			}
		}
	}
	return nil
}

func (q *memoryQueue) Nack(consumerID string, messageID int64) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if consumerID == "" {
		return fmt.Errorf("consumer ID is empty")
	}
	if messageID == 0 {
		return fmt.Errorf("message ID is zero")
	}

	if _, ok := q.retryMap[consumerID]; !ok {
		q.retryMap[consumerID] = make(map[int64]int) // Initialize retry map
		q.retryMap[consumerID][messageID] = 1        // Initialize retry count
	} else {
		q.retryMap[consumerID][messageID]++ // Increment retry count
	}
	if q.retryMap[consumerID][messageID] > maxRetry {
		// If max retries exceeded, move to dead-letter queue
		for i, msg := range q.items {
			if msg.Id == messageID {
				q.dlq = append(q.dlq, msg)                // Add to dead-letter queue
				q.offsetMap[consumerID] = i               // Update offset for the consumer
				delete(q.retryMap[consumerID], messageID) // Remove from retry map
				if len(q.retryMap[consumerID]) == 0 {
					delete(q.retryMap, consumerID) // Clean up empty retry map
				}
				fmt.Printf("Message %d moved to dead-letter queue after max retries\n", messageID)
				return nil
			}
		}
	}
	return nil
}

func (q *memoryQueue) Shutdown() error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	// Clear the queue and offsets
	q.items = nil
	q.offsetMap = make(map[string]int)
	q.retryMap = make(map[string]map[int64]int)
	q.dlq = nil
	fmt.Println("Memory queue has been shut down.")
	return nil
}

func (q *memoryQueue) Status() (internal.QueueStatus, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	status := internal.QueueStatus{
		QueueType:       "memory",
		ActiveConsumers: len(q.offsetMap),
		ExtraInfo: map[string]interface{}{
			"TotalMessages":       len(q.items),
			"DeadLetterQueueSize": len(q.dlq),
		},
		ConsumerStatuses: make(map[string]internal.ConsumerStatus),
	}

	for consumerID, offset := range q.offsetMap {
		lag := int64(len(q.items) - offset - 1) // Calculate lag
		if lag < 0 {
			lag = 0 // Ensure lag is not negative
		}
		status.ConsumerStatuses[consumerID] = internal.ConsumerStatus{
			ConsumerID: consumerID,
			LastOffset: int64(offset),
			Lag:        lag,
		}
	}

	return status, nil
}
