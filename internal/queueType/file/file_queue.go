package file

import (
	"fmt"
	"go-msg-queue-mini/internal"
	"os"
	"sync"
	"time"
)

type fileQueue struct {
	maxRetry    int // Maximum number of retries for message processing
	mutex       sync.Mutex
	fileManager *fileManager // File manager for handling file operations
}

func NewFileQueue(logdirs string, maxSizeMB int, maxAgeDays int, maxRetry int) (*fileQueue, error) {
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
	fm, err := NewFileManager(logdirs, maxSizeMB, maxAgeDays)
	if err != nil {
		return nil, fmt.Errorf("error initializing file manager: %v", err)
	}
	if err := fm.OpenFiles(); err != nil {
		return nil, fmt.Errorf("error opening files: %v", err)
	}

	fileQueue := &fileQueue{
		mutex:       sync.Mutex{},
		maxRetry:    maxRetry,
		fileManager: fm,
	}

	return fileQueue, nil
}

func (q *fileQueue) Enqueue(item interface{}) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	id := time.Now().UnixNano() // Use current time as a unique ID
	msg := internal.Msg{
		Id:   id,
		Item: item,
	}
	// Append the message to the append log file
	if err := q.fileManager.WriteMsg(msg); err != nil {
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
	messages, err := q.fileManager.ReadMsg(consumerID, maxCount)
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
	if err := q.fileManager.WriteOffset(consumerID, messageID); err != nil {
		return fmt.Errorf("error writing offset for consumer %s: %v", consumerID, err)
	}

	return nil
}

func (q *fileQueue) Nack(consumerID string, messageID int64) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if consumerID == "" {
		return fmt.Errorf("consumer ID is empty")
	}
	if messageID == 0 {
		return fmt.Errorf("message ID is zero")
	}

	if err := q.fileManager.WriteRetry(consumerID, messageID, q.maxRetry); err != nil {
		return fmt.Errorf("error writing retry for consumer %s: %v", consumerID, err)
	}
	return nil
}

func (q *fileQueue) Shutdown() error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	// Close all segment files
	if err := q.fileManager.CloseFiles(); err != nil {
		return fmt.Errorf("error closing segment files: %v", err)
	}

	fmt.Println("File queue shutdown successfully.")
	return nil
}

func (q *fileQueue) Status() (internal.QueueStatus, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	status, err := q.fileManager.GetStatus()
	if err != nil {
		return internal.QueueStatus{}, fmt.Errorf("error getting queue status: %v", err)
	}
	return status, nil
}
