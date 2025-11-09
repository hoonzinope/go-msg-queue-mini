package core

import (
	"database/sql"
	"go-msg-queue-mini/internal"
	"time"
)

type DLQMessage struct {
	ID          int64
	QueueInfoID int64
	QueueID     int64
	PartitionID int
	GlobalID    string
	Msg         []byte
	FailedGroup string
	Reason      string
	InsertTs    time.Time
}

// create dlq table
func (m *FileDBManager) createDLQTable() error {
	createTableSQL :=
		`CREATE TABLE IF NOT EXISTS dlq (
		id INTEGER PRIMARY KEY,                          -- rowid 기반 PK
		queue_info_id INTEGER NOT NULL,                  -- 큐 정보 ID
    	q_id INTEGER NOT NULL,                           -- 원본 queue.id
		partition_id INTEGER NOT NULL DEFAULT 0,		 -- 파티션 ID
		global_id TEXT NOT NULL DEFAULT '',			 -- 글로벌 ID (복제시 사용, UUID 사용)
    	msg BLOB,                                        -- 메시지 복사본
    	failed_group TEXT,                               -- 실패한 컨슈머 그룹
    	reason TEXT,                                     -- 실패 사유
    	insert_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- 그룹별 메시지 1개만 점유 가능
		FOREIGN KEY (queue_info_id) REFERENCES queue_info(id) ON DELETE CASCADE,
		FOREIGN KEY (q_id) REFERENCES queue(id) ON DELETE CASCADE
	);`
	_, err := m.db.Exec(createTableSQL)
	return err
}

// insert dlq
func (m *FileDBManager) insertDLQ(tx *sql.Tx, queueInfoID int64, partitionID int, globalID string, msg []byte, group string, reason string) error {
	if _, err := tx.Exec(`
			INSERT INTO dlq(q_id, queue_info_id, global_id, partition_id, msg, failed_group, reason)
			SELECT q.id, q.queue_info_id, q.global_id, q.partition_id, q.msg, ?, ?
			FROM queue q WHERE q.queue_info_id = ? AND q.global_id = ? AND q.partition_id = ?
			`, group, reason, queueInfoID, globalID, partitionID); err != nil {
		return err
	}
	return nil
}

// get dlq list with peek options
func (m *FileDBManager) ListDLQ(tx *sql.Tx, queueInfoID int64, options internal.PeekOptions) ([]DLQMessage, error) {
	query := `SELECT 
		id, queue_info_id, q_id, partition_id, global_id, msg, failed_group, reason, insert_ts
	 FROM dlq WHERE queue_info_id = ? `
	var args []interface{}
	args = append(args, queueInfoID)

	// order by
	orderDirection := "ASC"
	if options.Order == "desc" {
		orderDirection = "DESC"
	}
	cursor := options.Cursor
	if cursor > 0 {
		if orderDirection == "ASC" {
			query += " AND id > ? "
		} else {
			query += " AND id < ? "
		}
		args = append(args, cursor)
	}
	query += " ORDER BY id " + orderDirection + " "

	// limit - default limit is 10
	if options.Limit > 0 {
		query += " LIMIT ? "
		args = append(args, options.Limit)
	}

	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dlqMessages []DLQMessage
	for rows.Next() {
		var dlqMsg DLQMessage
		if err := rows.Scan(
			&dlqMsg.ID,
			&dlqMsg.QueueInfoID,
			&dlqMsg.QueueID,
			&dlqMsg.PartitionID,
			&dlqMsg.GlobalID,
			&dlqMsg.Msg,
			&dlqMsg.FailedGroup,
			&dlqMsg.Reason,
			&dlqMsg.InsertTs,
		); err != nil {
			return nil, err
		}
		dlqMessages = append(dlqMessages, dlqMsg)
	}
	return dlqMessages, nil
}

// get dlq detail by message id
func (m *FileDBManager) DetailDLQ(tx *sql.Tx, queueInfoID int64, messageId int64) (DLQMessage, error) {
	var dlqMsg DLQMessage
	err := tx.QueryRow(`
		SELECT 
			id, queue_info_id, q_id, partition_id, global_id, msg, failed_group, reason, insert_ts
		FROM dlq 
		WHERE queue_info_id = ? AND id = ?`, queueInfoID, messageId).Scan(
		&dlqMsg.ID,
		&dlqMsg.QueueInfoID,
		&dlqMsg.QueueID,
		&dlqMsg.PartitionID,
		&dlqMsg.GlobalID,
		&dlqMsg.Msg,
		&dlqMsg.FailedGroup,
		&dlqMsg.Reason,
		&dlqMsg.InsertTs,
	)
	if err != nil {
		return DLQMessage{}, err
	}
	return dlqMsg, nil
}

func (m *FileDBManager) deleteDLQByID(tx *sql.Tx, queueInfoID int64, messageID int64) error {
	_, err := tx.Exec(`DELETE FROM dlq WHERE queue_info_id = ? AND id = ?`, queueInfoID, messageID)
	return err
}
