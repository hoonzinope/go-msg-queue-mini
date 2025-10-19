package core

import (
	"database/sql"
	"fmt"
	"go-msg-queue-mini/internal"
	queue_error "go-msg-queue-mini/internal/queue_error"
	"go-msg-queue-mini/util"
)

// create queue msg table
func (m *FileDBManager) createQueueTable() error {
	createTableSQL :=
		`CREATE TABLE IF NOT EXISTS queue (
		id INTEGER PRIMARY KEY,                          -- rowid 기반 고유 PK
		queue_info_id INTEGER NOT NULL,                  -- 큐 정보 ID
    	msg BLOB NOT NULL,                               -- 메시지 본문
		partition_id INTEGER NOT NULL DEFAULT 0,		 -- 파티션 ID
		global_id TEXT NOT NULL DEFAULT '',			 -- 글로벌 ID (복제시 사용, UUID 사용)
    	insert_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY (queue_info_id) REFERENCES queue_info(id) ON DELETE CASCADE
	);`
	_, err := m.db.Exec(createTableSQL)
	if err != nil {
		return err
	}

	// if not exists visible_at then add column (초기값은 insert_ts)
	rows, err := m.db.Query(`PRAGMA table_info(queue);`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var check_visible_at bool
	for rows.Next() {
		var cid int
		var name string
		var ctype string
		var notnull int
		var dflt_value *string
		var pk int

		err = rows.Scan(&cid, &name, &ctype, &notnull, &dflt_value, &pk)
		if err != nil {
			return err
		}

		if name == "visible_at" {
			check_visible_at = true
			break
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}

	if !check_visible_at {
		_, err = m.db.Exec(`ALTER TABLE queue ADD COLUMN visible_at TIMESTAMP;`)
		if err != nil {
			return err
		}

		// 기존 데이터들의 visible_at 값을 insert_ts 값으로 초기화
		_, err = m.db.Exec(`UPDATE queue SET visible_at = insert_ts WHERE visible_at IS NULL;`)
		if err != nil {
			return err
		}
	}

	createIndex := `CREATE UNIQUE INDEX IF NOT EXISTS uq_queue_global ON queue(queue_info_id, global_id);`
	_, err = m.db.Exec(createIndex)
	if err != nil {
		return err
	}
	createIndex2 := `CREATE INDEX IF NOT EXISTS idx_queue_partition ON queue(queue_info_id, partition_id, visible_at, id);`
	_, err = m.db.Exec(createIndex2)
	if err != nil {
		return err
	}
	return nil
}

// insert message to queue
func (m *FileDBManager) insertMessage(
	tx *sql.Tx, queueInfoID int64, msg []byte, globalID string, partitionID int,
	mod string) (int64, error) {
	// 메시지 삽입
	res, err := tx.Exec(`
		INSERT INTO queue 
		(queue_info_id, msg, global_id, partition_id, visible_at) 
		VALUES (?, ?, ?, ?, DATETIME('now', ?));`,
		queueInfoID, msg, globalID, partitionID, mod)
	if err != nil {
		return -1, err
	}
	queueRowId, err := res.LastInsertId()
	if err != nil {
		return -1, err
	}
	return queueRowId, nil
}

// insert messagebatch to queue (stopOnFailure mode)
func (m *FileDBManager) insertMessageBatchStopOnFailure(
	tx *sql.Tx, queueInfoID int64, msgs [][]byte, partitionID int,
	delays []string, deduplicationIDs []string) (int64, error) {
	var txCnt int64 = 0

	stmt, err := tx.Prepare(`
			INSERT INTO queue 
			(queue_info_id, msg, global_id, partition_id, visible_at) 
			VALUES (?, ?, ?, ?, DATETIME('now', ?))`)
	if err != nil {
		return txCnt, err
	}
	defer stmt.Close()

	for i, msg := range msgs {
		var delay string = "+0 seconds"
		if i < len(delays) {
			delay = delays[i]
		}
		mod, err := util.DelayToSeconds(delay) // "+0 seconds"
		if err != nil {
			return txCnt, err
		}
		var dedupID string = ""
		if i < len(deduplicationIDs) {
			dedupID = deduplicationIDs[i]
		}
		// deduplicationID가 있으면 중복 검사
		if dedupID != "" {
			logId, err := m.checkDuplicationID(tx, queueInfoID, dedupID)
			if err != nil {
				return txCnt, fmt.Errorf("error checking duplication ID: %w", err)
			}
			globalID := util.GenerateGlobalID()
			res, insertErr := stmt.Exec(queueInfoID, msg, globalID, partitionID, mod)
			if insertErr != nil {
				m.deleteDeduplicationLogs(tx, logId) // 삽입 실패시 deduplication_log 삭제
				return txCnt, insertErr
			}
			queue_row_id, err := res.LastInsertId()
			if err != nil {
				m.deleteDeduplicationLogs(tx, logId) // 삽입 실패시 deduplication_log 삭제
				return txCnt, fmt.Errorf("error getting last insert ID: %w", err)
			}
			if queue_row_id <= 0 {
				m.deleteDeduplicationLogs(tx, logId) // 삽입 실패시 deduplication_log 삭제
				return txCnt, fmt.Errorf("failed to insert message")
			}
			// log_id, queue_row_id 업데이트
			if err := m.updateDeduplicationLogWithQueueRowID(tx, logId, queue_row_id); err != nil {
				m.deleteDeduplicationLogs(tx, logId) // 삽입 실패시 deduplication_log 삭제
				return txCnt, fmt.Errorf("error updating deduplication log with queue row ID: %w", err)
			}
		} else {
			globalID := util.GenerateGlobalID()
			_, insertErr := stmt.Exec(queueInfoID, msg, globalID, partitionID, mod)
			if insertErr != nil {
				return txCnt, insertErr
			}
		}
		txCnt++
	}
	return txCnt, nil
}

// insert messagebatch to queue (partialSuccess mode)
func (m *FileDBManager) insertMessageBatchReturnFailed(
	tx *sql.Tx, queueInfoID int64, msgs [][]byte, partitionID int,
	delays []string, deduplicationIDs []string, startIndex int64) (successCnt int64, failedMessages []internal.FailedMessage, err error) {
	stmt, err := tx.Prepare(`
			INSERT INTO queue 
			(queue_info_id, msg, global_id, partition_id, visible_at) 
			VALUES (?, ?, ?, ?, DATETIME('now', ?))
			ON CONFLICT(queue_info_id, global_id) DO NOTHING
			`)
	if err != nil {
		return 0, nil, err
	}
	defer stmt.Close()

	var txCnt int64 = 0
	for i, msg := range msgs {
		globalID := util.GenerateGlobalID()
		var delay string = "+0 seconds"
		if i < len(delays) {
			delay = delays[i]
		}
		mod, err := util.DelayToSeconds(delay) // "+0 seconds"
		if err != nil {
			return txCnt, nil, err
		}
		var dedupID string = ""
		if i < len(deduplicationIDs) {
			dedupID = deduplicationIDs[i]
		}
		// deduplicationID가 있으면 중복 검사
		if dedupID != "" {
			logId, err := m.checkDuplicationID(tx, queueInfoID, dedupID)
			if err != nil {
				failedMessages = append(failedMessages, internal.FailedMessage{
					Index:   int64(i) + startIndex,
					Reason:  fmt.Sprintf("error checking duplication ID: %v", err),
					Message: msg,
				})
				continue
			}
			queue_row_id, failedMessage, insertErr := m.partialSuccess(tx, stmt, queueInfoID, msg, globalID, partitionID, mod, startIndex+int64(i))
			if insertErr != nil {
				failedMessages = append(failedMessages, failedMessage)
				m.deleteDeduplicationLogs(tx, logId) // 삽입 실패시 로그 삭제
				continue
			}

			// log_id, queue_row_id 업데이트
			if err := m.updateDeduplicationLogWithQueueRowID(tx, logId, queue_row_id); err != nil {
				failedMessages = append(failedMessages, internal.FailedMessage{
					Index:   int64(i) + startIndex,
					Reason:  fmt.Sprintf("error updating deduplication log with queue row ID: %v", err),
					Message: msg,
				})
				m.deleteDeduplicationLogs(tx, logId) // 삽입 실패시 로그 삭제
				continue
			}
			txCnt++
		} else {
			// msg 단위 savepoint
			_, failedMessage, insertErr := m.partialSuccess(tx, stmt, queueInfoID, msg, globalID, partitionID, mod, startIndex+int64(i))
			if insertErr != nil {
				failedMessages = append(failedMessages, failedMessage)
				continue
			}
			txCnt++
		}

	}
	if txCnt > 0 {
		return txCnt, failedMessages, nil
	} else {
		return 0, failedMessages, fmt.Errorf("all messages failed to insert")
	}
}

// msg 단위로 삽입 시도, 실패시 롤백 후 실패 사유 리턴
func (m *FileDBManager) partialSuccess(tx *sql.Tx, stmt *sql.Stmt,
	queueInfoID int64, msg []byte, globalID string, partitionID int, mod string, order int64) (queue_row_id int64, failedMessage internal.FailedMessage, err error) {
	// msg 단위 savepoint
	sp := fmt.Sprintf("sp_%d", order)
	if _, err := tx.Exec(fmt.Sprintf("SAVEPOINT %s;", sp)); err != nil {

	}
	res, insertErr := stmt.Exec(queueInfoID, msg, globalID, partitionID, mod)
	if insertErr != nil {
		_, _ = tx.Exec(fmt.Sprintf("ROLLBACK TO %s;", sp))
		_, _ = tx.Exec(fmt.Sprintf("RELEASE %s;", sp))
		failedMessage = internal.FailedMessage{
			Index:   int64(order),
			Reason:  insertErr.Error(),
			Message: msg,
		}
		return -1, failedMessage, insertErr
	}

	if ra, _ := res.RowsAffected(); ra == 0 {
		// 중복으로 삽입 안된 경우
		_, _ = tx.Exec(fmt.Sprintf("ROLLBACK TO %s;", sp))
		_, _ = tx.Exec(fmt.Sprintf("RELEASE %s;", sp))
		failedMessage = internal.FailedMessage{
			Index:   int64(order),
			Reason:  "duplicate message",
			Message: msg,
		}
		return -1, failedMessage, fmt.Errorf("duplicate message")
	}
	queue_row_id, err = res.LastInsertId()
	if err != nil {
		_, _ = tx.Exec(fmt.Sprintf("ROLLBACK TO %s;", sp))
		_, _ = tx.Exec(fmt.Sprintf("RELEASE %s;", sp))
		failedMessage = internal.FailedMessage{
			Index:   int64(order),
			Reason:  fmt.Sprintf("error getting last insert ID: %v", err),
			Message: msg,
		}
		return -1, failedMessage, fmt.Errorf("error getting last insert ID: %v", err)
	}

	if _, err := tx.Exec(fmt.Sprintf("RELEASE %s;", sp)); err != nil {
		_, _ = tx.Exec(fmt.Sprintf("ROLLBACK TO %s;", sp))
	}
	return queue_row_id, internal.FailedMessage{}, nil
}

// 후보 메시지 조회
func (m *FileDBManager) getCandidateQueueMsg(tx *sql.Tx, queueInfoID int64, group string, partitionID int) (candidateMsg queueMsg, err error) {
	// 1) 후보 조회
	options := internal.PeekOptions{
		Limit:   1,
		Cursor:  0,
		Order:   "asc",
		Preview: false,
	}
	msgs, err := m.peekCandidateQueueMsgsWithOptions(tx, queueInfoID, group, partitionID, options)
	if err != nil {
		return queueMsg{}, err
	}
	if len(msgs) == 0 {
		return queueMsg{}, queue_error.ErrEmpty
	}
	return msgs[0], nil
}

// 후보 메세지 조회 (option에 따라)
func (m *FileDBManager) peekCandidateQueueMsgsWithOptions(tx *sql.Tx, queueInfoID int64, group string, partitionID int, options internal.PeekOptions) ([]queueMsg, error) {
	var msgs []queueMsg
	var err error
	// build query
	query := `
			SELECT q.id, q.msg, q.insert_ts, "" as receipt, q.global_id, q.partition_id
			FROM queue q
			LEFT JOIN acked a
				ON a.queue_info_id = q.queue_info_id 
				AND a.global_id = q.global_id 
				AND a.group_name = ? 
				AND a.partition_id = ?
			LEFT JOIN inflight i 
				ON i.queue_info_id = q.queue_info_id 
				AND i.q_id = q.id 
				AND i.group_name = ? 
				AND i.partition_id = ?
			JOIN queue_info qi
				ON qi.id = q.queue_info_id
			WHERE
			q.queue_info_id = ? 
			AND q.visible_at <= CURRENT_TIMESTAMP
			AND a.global_id IS NULL
			AND (i.q_id IS NULL OR i.lease_until <= CURRENT_TIMESTAMP)
			AND q.partition_id = ?
		`
	// cursor & order
	if options.Cursor != 0 {
		cursorID := options.Cursor
		switch {
		case options.Order == "asc":
			query += fmt.Sprintf(" AND q.id > %d", cursorID)
			query += " ORDER BY q.id ASC"
		case options.Order == "desc":
			query += fmt.Sprintf(" AND q.id < %d", cursorID)
			query += " ORDER BY q.id DESC"
		default:
			query += fmt.Sprintf(" AND q.id > %d", cursorID)
			query += " ORDER BY q.id ASC"
		}
	} else {
		switch {
		case options.Order == "asc":
			query += " ORDER BY q.id ASC"
		case options.Order == "desc":
			query += " ORDER BY q.id DESC"
		default:
			query += " ORDER BY q.id ASC"
		}
	}

	// limit
	if options.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", options.Limit)
	} else {
		query += " LIMIT 1" // default limit
	}

	rows, err := tx.Query(query,
		group, partitionID,
		group, partitionID,
		queueInfoID, partitionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var msg queueMsg
		err = rows.Scan(
			&msg.ID, &msg.Msg,
			&msg.InsertTS, &msg.Receipt,
			&msg.GlobalID, &msg.PartitionID)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, queue_error.ErrEmpty
	}
	return msgs, nil
}

// 내가 점유한 메시지 반환 (consumer_id로 한정)
func (m *FileDBManager) getLeaseMsg(
	tx *sql.Tx, queueInfoID int64, group string, consumerID string, partitionID int) (queueMsg, error) {
	var msg queueMsg
	err := tx.QueryRow(`
			SELECT 
			q.id, q.msg, q.insert_ts, i.receipt, q.global_id, q.partition_id
			FROM queue q
			JOIN inflight i ON 
				i.q_id = q.id AND
				i.queue_info_id = q.queue_info_id
			JOIN queue_info qi ON
				qi.id = q.queue_info_id
			WHERE 
				i.queue_info_id = ? 
				AND i.group_name = ? 
				AND i.consumer_id = ? 
				AND i.partition_id = ?
			ORDER BY i.claimed_at DESC
			LIMIT 1
		`, queueInfoID, group, consumerID, partitionID).Scan(
		&msg.ID, &msg.Msg, &msg.InsertTS, &msg.Receipt, &msg.GlobalID, &msg.PartitionID)
	if err != nil {
		return queueMsg{}, err
	}
	return msg, nil
}

// exist check by msg_id
func (m *FileDBManager) existsMsgID(tx *sql.Tx, msgID int64) (global_id string, partition_id int, err error) {
	err = tx.QueryRow(`SELECT global_id, partition_id FROM queue WHERE id = ?`, msgID).Scan(&global_id, &partition_id)
	if err == sql.ErrNoRows {
		return "", 0, queue_error.ErrMessageNotFound
	}
	if err != nil {
		return "", 0, err
	}
	return global_id, partition_id, nil
}

// delete queue message
func (m *FileDBManager) deleteQueueMsg(tx *sql.Tx) (int64, error) {
	res, err := tx.Exec(`
			DELETE FROM queue
			WHERE id IN (
				SELECT q.id
				FROM queue q
					JOIN queue_info qi 
						ON qi.id = q.queue_info_id
					LEFT JOIN inflight i
						ON i.queue_info_id = q.queue_info_id
						AND i.q_id         = q.id
				WHERE 
					i.q_id IS NULL
					AND q.insert_ts <= DATETIME('now', printf('-%d seconds', qi.retention_s))
			);
		`)
	if err != nil {
		return 0, err
	}
	resAffected, err := res.RowsAffected()
	if err != nil || resAffected <= 0 {
		return 0, err
	}
	return resAffected, nil
}
