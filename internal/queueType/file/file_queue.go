package file

import (
	"fmt"
	"go-msg-queue-mini/internal"
	"os"
	"sync"
	"time"
)

type fileQueue struct {
	mutex sync.Mutex
}

func NewFileQueue(logdirs string, maxSizeMB int, maxAgeDays int) (*fileQueue, error) {
	if logdirs == "" {
		fmt.Println("Log directory path is empty - error initializing file queue")
		return nil, fmt.Errorf("log directory path is empty")
	}
	if _, err := os.Stat(logdirs); os.IsNotExist(err) {
		fmt.Printf("Log directory does not exist: %s\n", logdirs)
		if err := os.MkdirAll(logdirs, 0755); err != nil {
			return nil, fmt.Errorf("error creating log directory: %v", err)
		}
		fmt.Printf("Log directory created: %s\n", logdirs)
	}

	// Initialize the file queue
	if err := OpenFiles(logdirs, maxSizeMB, maxAgeDays); err != nil {
		return nil, fmt.Errorf("error opening files: %v", err)
	}

	fileQueue := &fileQueue{
		mutex: sync.Mutex{},
	}

	return fileQueue, nil
}

func (q *fileQueue) Enqueue(item interface{}) error {
	id := time.Now().UnixNano() // Use current time as a unique ID
	msg := internal.Msg{
		Id:   id,
		Item: item,
	}
	// Append the message to the append log file
	if err := WriteMsgToSegment(msg); err != nil {
		fmt.Printf("Error writing message to segment: %v\n", err)
		return err
	}
	return nil
}

func (q *fileQueue) Dequeue(consumerID string, maxCount int) ([]internal.Msg, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if consumerID == "" {
		return nil, fmt.Errorf("consumer ID is empty")
	}
	if maxCount <= 0 {
		return nil, fmt.Errorf("max count must be greater than 0")
	}

	// Read messages for the consumer
	messages, err := ReadMsgForConsumer(consumerID, maxCount)
	if err != nil {
		return nil, fmt.Errorf("error reading messages for consumer %s: %v", consumerID, err)
	}

	if len(messages) == 0 {
		return nil, fmt.Errorf("no new messages")
	}
	return messages, nil
}

func (q *fileQueue) Ack(consumerID string, messageID int64) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if consumerID == "" {
		return fmt.Errorf("consumer ID is empty")
	}
	if messageID == 0 {
		return fmt.Errorf("message ID is zero")
	}

	// Update the offset for the consumer
	if err := WriteOffset(consumerID, messageID); err != nil {
		return fmt.Errorf("error writing offset for consumer %s: %v", consumerID, err)
	}

	return nil
}

func (q *fileQueue) Shutdown() error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	// Close all segment files
	if err := CloseFiles(); err != nil {
		return fmt.Errorf("error closing segment files: %v", err)
	}

	fmt.Println("File queue shutdown successfully.")
	return nil
}

func (q *fileQueue) Status() (internal.QueueStatus, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	var consumerStatuses = make(map[string]internal.ConsumerStatus)
	for consumerID, offset := range offsetMap {
		lag := int64(0) // Calculate lag based on the latest message ID and consumer offset
		if latestMessageID > offset {
			lag = latestMessageID - offset
		}
		consumerStatuses[consumerID] = internal.ConsumerStatus{
			ConsumerID: consumerID,
			LastOffset: offset,
			Lag:        lag,
		}
	}

	totalSegmentFiles := len(segmentFileMetadataMap) + 1 // +1 for the append log file
	extraInfo := map[string]interface{}{
		"TotalSegmentFiles": totalSegmentFiles,
		"LatestMessageID":   latestMessageID,
	}

	status := internal.QueueStatus{
		QueueType:        "file",
		ActiveConsumers:  len(offsetMap),
		ExtraInfo:        extraInfo,
		ConsumerStatuses: consumerStatuses,
	}

	return status, nil
}
