package file

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
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

	// Verify retry.state file
	retryFilePath := filepath.Join(tempDir, "retry.state")
	data, err := ioutil.ReadFile(retryFilePath)
	assert.NoError(t, err)
	expectedRetryState := fmt.Sprintf("%s %d 2\n", consumerID, msgID)
	assert.Equal(t, expectedRetryState, string(data), "retry.state file content is incorrect")
}

func TestFileQueue_Nack_MoveToDLQ(t *testing.T) {
	q, tempDir := setupFileQueueTest(t)
	defer teardownFileQueueTest(t, q, tempDir)

	t.Logf("Starting TestFileQueue_Nack_MoveToDLQ...")

	consumerID := "test-consumer-dlq"
	msgContent := "test-message-dlq"
	q.maxRetry = 2 // Override for this test
	t.Logf("Queue created with maxRetry=%d", q.maxRetry)

	// Enqueue and Dequeue
	assert.NoError(t, q.Enqueue(msgContent))
	t.Logf("Message enqueued: %s", msgContent)

	msgs, err := q.Dequeue(consumerID, 1)
	assert.NoError(t, err)
	assert.Len(t, msgs, 1)
	msg := msgs[0]
	t.Logf("Message dequeued with ID: %d", msg.Id)

	// Nack until it moves to DLQ
	for i := 0; i <= q.maxRetry; i++ {
		t.Logf("Nacking message... Attempt %d", i+1)
		err := q.Nack(consumerID, msg.Id)
		assert.NoError(t, err)
		t.Logf("Nack attempt %d successful", i+1)
	}

	t.Logf("Finished Nacking. Verifying DLQ status...")

	// Verify DLQ file
	dlqFilePath := filepath.Join(tempDir, "dead_letter_queue.log")
	_, err = os.Stat(dlqFilePath)
	assert.NoError(t, err, "dead_letter_queue.log should exist")
	t.Logf("DLQ file found.")

	data, err := ioutil.ReadFile(dlqFilePath)
	assert.NoError(t, err)
	assert.Contains(t, string(data), msgContent, "DLQ file should contain the failed message")
	t.Logf("DLQ file content verified.")

	// Verify retry.state is empty
	retryFilePath := filepath.Join(tempDir, "retry.state")
	data, err = ioutil.ReadFile(retryFilePath)
	assert.NoError(t, err)
	assert.Empty(t, string(data), "retry.state should be empty after moving to DLQ")
	t.Logf("retry.state file verified to be empty.")

	// Verify offset is updated
	offsetFilePath := filepath.Join(tempDir, "offset.state")
	data, err = ioutil.ReadFile(offsetFilePath)
	assert.NoError(t, err)
	expectedOffsetState := fmt.Sprintf("%s %d\n", consumerID, msg.Id)
	assert.Equal(t, expectedOffsetState, string(data), "offset.state should be updated to skip the DLQ message")
	t.Logf("offset.state file verified.")
	t.Logf("TestFileQueue_Nack_MoveToDLQ finished successfully.")
}
