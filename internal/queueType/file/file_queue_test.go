package file

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

// setupFileQueueTest creates a temporary directory for the test and returns a new fileQueue and the directory path.
func setupFileQueueTest(t *testing.T) (*fileQueue, string) {
	tempDir, err := ioutil.TempDir("", "filequeue-test-")
	assert.NoError(t, err)

	maxRetry := 3
	q, err := NewFileQueue(tempDir, 1, 1, maxRetry)
	assert.NoError(t, err)

	return q, tempDir
}

// teardownFileQueueTest closes the queue and removes the temporary directory.
func teardownFileQueueTest(t *testing.T, q *fileQueue, tempDir string) {
	err := q.Shutdown()
	assert.NoError(t, err)
	os.RemoveAll(tempDir)
}

func TestFileQueue_Nack_Retry(t *testing.T) {
	q, tempDir := setupFileQueueTest(t)
	defer teardownFileQueueTest(t, q, tempDir)

	consumerID := "test-consumer-retry"
	msgContent := "test-message"

	// Enqueue and Dequeue
	assert.NoError(t, q.Enqueue(msgContent))
	msgs, err := q.Dequeue(consumerID, 1)
	assert.NoError(t, err)
	assert.Len(t, msgs, 1)
	msgID := msgs[0].Id

	// Nack twice
	assert.NoError(t, q.Nack(consumerID, msgID))
	assert.NoError(t, q.Nack(consumerID, msgID))
	fmt.Printf("Message %d NACKed twice\n", msgID)
	// Verify retry in event.log file
	retryFilePath := filepath.Join(tempDir, "event.log")
	data, err := os.OpenFile(retryFilePath, os.O_RDONLY, 0644)
	assert.NoError(t, err)
	defer data.Close()
	dec := json.NewDecoder(data)
	for {
		var ev event
		if err := dec.Decode(&ev); err != nil {
			if errors.Is(err, io.EOF) {
				break // End of file
			}
			assert.Fail(t, "Failed to decode event log", err)
		}
		if ev.Type == "NACK" {
			msg := ev.Data.(map[string]interface{})
			rawMessageID, ok := msg["messageID"]
			assert.True(t, ok, "messageID should be present in NACK event")
			fmt.Printf("Raw message ID: %v\n", rawMessageID)
			messageID, err := strconv.ParseInt(rawMessageID.(string), 10, 64)
			assert.NoError(t, err, "messageID should be a number")
			assert.Equal(t, consumerID, msg["consumerID"], "consumerID should match")
			assert.Equal(t, int64(msgID), messageID, "Message ID should match")
		}
	}
}
