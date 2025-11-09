package core

import (
	"bytes"
	"database/sql"
	"testing"

	"go-msg-queue-mini/internal"
)

func TestDLQScanOrderForListAndDetail(t *testing.T) {
	queue := newTestQueue(t)
	queueName := "dlq-scan-order"
	createQueueOrFail(t, queue, queueName)

	payload := []byte("dlq-body")
	if err := queue.Enqueue(queueName, internal.EnqueueMessage{Item: payload, Delay: "", DeduplicationID: ""}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	manager := queue.manager
	queueInfoID, err := manager.getQueueInfoID(queueName)
	if err != nil {
		t.Fatalf("getQueueInfoID: %v", err)
	}

	const failedGroup = "worker-a"
	const reason = "processing-failed"

	if err := manager.inTx(func(tx *sql.Tx) error {
		var globalID string
		var partitionID int
		if err := tx.QueryRow(`SELECT global_id, partition_id FROM queue WHERE queue_info_id = ? LIMIT 1`, queueInfoID).
			Scan(&globalID, &partitionID); err != nil {
			return err
		}
		return manager.insertDLQ(tx, queueInfoID, partitionID, globalID, nil, failedGroup, reason)
	}); err != nil {
		t.Fatalf("seed DLQ: %v", err)
	}

	msgs, err := manager.ListDLQMessages(queueName, internal.PeekOptions{Limit: 5})
	if err != nil {
		t.Fatalf("ListDLQMessages: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("ListDLQMessages len = %d, want 1", len(msgs))
	}
	listMsg := msgs[0]
	if listMsg.Reason != reason {
		t.Fatalf("list reason = %s, want %s", listMsg.Reason, reason)
	}
	if listMsg.FailedGroup != failedGroup {
		t.Fatalf("list failed_group = %s, want %s", listMsg.FailedGroup, failedGroup)
	}
	if !bytes.Equal(listMsg.Msg, payload) {
		t.Fatalf("list payload = %s, want %s", string(listMsg.Msg), string(payload))
	}

	detail, err := manager.GetDLQMessageDetail(queueName, listMsg.ID)
	if err != nil {
		t.Fatalf("GetDLQMessageDetail: %v", err)
	}
	if detail.Reason != reason {
		t.Fatalf("detail reason = %s, want %s", detail.Reason, reason)
	}
	if detail.FailedGroup != failedGroup {
		t.Fatalf("detail failed_group = %s, want %s", detail.FailedGroup, failedGroup)
	}
	if !bytes.Equal(detail.Msg, payload) {
		t.Fatalf("detail payload = %s, want %s", string(detail.Msg), string(payload))
	}
}

func TestFileDBManagerRedriveDLQMessagesRequeuesWithNewGlobalID(t *testing.T) {
	queue := newTestQueue(t)
	queueName := "dlq-redrive-success"
	createQueueOrFail(t, queue, queueName)

	payload := []byte("to-redrive")
	if err := queue.Enqueue(queueName, internal.EnqueueMessage{Item: payload}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	queueInfoID, originalGlobalID, dlqID := seedSingleDLQMessage(t, queue, queueName)

	if err := queue.manager.RedriveDLQMessages(queueName, []int64{dlqID}); err != nil {
		t.Fatalf("RedriveDLQMessages: %v", err)
	}

	// ensure the queue now has the re-driven message with a fresh global_id
	var newGlobalID string
	var queueCount int
	if err := queue.manager.inTx(func(tx *sql.Tx) error {
		if err := tx.QueryRow(`SELECT COUNT(*) FROM queue WHERE queue_info_id = ?`, queueInfoID).Scan(&queueCount); err != nil {
			return err
		}
		return tx.QueryRow(`SELECT global_id FROM queue WHERE queue_info_id = ? LIMIT 1`, queueInfoID).Scan(&newGlobalID)
	}); err != nil {
		t.Fatalf("query queue table: %v", err)
	}
	if queueCount != 1 {
		t.Fatalf("queue message count = %d, want 1", queueCount)
	}
	if newGlobalID == originalGlobalID {
		t.Fatalf("global_id reused; got %s", newGlobalID)
	}

	// DLQ should be empty after redrive
	msgs, err := queue.manager.ListDLQMessages(queueName, internal.PeekOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListDLQMessages after redrive: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("DLQ length after redrive = %d, want 0", len(msgs))
	}

	// consumer should be able to read the re-driven payload
	dequeued, err := queue.Dequeue(queueName, "redrive-group", "consumer-1")
	if err != nil {
		t.Fatalf("Dequeue after redrive: %v", err)
	}
	if !bytes.Equal(dequeued.Payload, payload) {
		t.Fatalf("redriven payload = %s, want %s", string(dequeued.Payload), string(payload))
	}
	if err := queue.Ack(queueName, "redrive-group", dequeued.ID, dequeued.Receipt); err != nil {
		t.Fatalf("ack redriven message: %v", err)
	}
}

func TestFileDBManagerRedriveDLQMessagesRollsBackOnMissingMessage(t *testing.T) {
	queue := newTestQueue(t)
	queueName := "dlq-redrive-rollback"
	createQueueOrFail(t, queue, queueName)

	payload := []byte("stay-in-dlq")
	if err := queue.Enqueue(queueName, internal.EnqueueMessage{Item: payload}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	queueInfoID, _, dlqID := seedSingleDLQMessage(t, queue, queueName)

	err := queue.manager.RedriveDLQMessages(queueName, []int64{dlqID, dlqID + 9999})
	if err == nil {
		t.Fatal("RedriveDLQMessages should fail when one message id is missing")
	}

	// ensure queue remains empty because the transaction rolled back
	var queueCount int
	if err := queue.manager.inTx(func(tx *sql.Tx) error {
		return tx.QueryRow(`SELECT COUNT(*) FROM queue WHERE queue_info_id = ?`, queueInfoID).Scan(&queueCount)
	}); err != nil {
		t.Fatalf("query queue table: %v", err)
	}
	if queueCount != 0 {
		t.Fatalf("queue message count = %d, want 0", queueCount)
	}

	// DLQ should still contain the original message
	msgs, err := queue.manager.ListDLQMessages(queueName, internal.PeekOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListDLQMessages: %v", err)
	}
	if len(msgs) != 1 || msgs[0].ID != dlqID {
		t.Fatalf("DLQ entries after rollback = %+v, want single id %d", msgs, dlqID)
	}
}

func seedSingleDLQMessage(t *testing.T, queue *fileDBQueue, queueName string) (queueInfoID int64, originalGlobalID string, dlqID int64) {
	t.Helper()
	manager := queue.manager
	var err error
	queueInfoID, err = manager.getQueueInfoID(queueName)
	if err != nil {
		t.Fatalf("getQueueInfoID: %v", err)
	}

	const failedGroup = "dlq-group"
	const reason = "move-to-dlq"

	if err := manager.inTx(func(tx *sql.Tx) error {
		var queueRowID int64
		var partitionID int
		if err := tx.QueryRow(`SELECT id, global_id, partition_id FROM queue WHERE queue_info_id = ? LIMIT 1`,
			queueInfoID).Scan(&queueRowID, &originalGlobalID, &partitionID); err != nil {
			return err
		}
		if err := manager.insertDLQ(tx, queueInfoID, partitionID, originalGlobalID, nil, failedGroup, reason); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM queue WHERE id = ?`, queueRowID); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("failed to seed DLQ record: %v", err)
	}

	msgs, err := manager.ListDLQMessages(queueName, internal.PeekOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListDLQMessages: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected single DLQ message, got %d", len(msgs))
	}
	return queueInfoID, originalGlobalID, msgs[0].ID
}
