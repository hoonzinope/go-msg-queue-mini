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
