package core

import (
	"database/sql"
	"fmt"
	"go-msg-queue-mini/internal/queue_error"
)

var expired_deduplication_log_cleanup_interval_seconds = 1 * 60 * 60 // 1 hour

// create deduplication_log table
func (m *FileDBManager) createDeduplicationLogTable() error {
	createTableSQL := `
		-- 중복 로그
		CREATE TABLE IF NOT EXISTS deduplication_log (
		id INTEGER,
		queue_info_id INTEGER NOT NULL,
		deduplication_id TEXT NOT NULL,
		expires_at TIMESTAMP NOT NULL,
		queue_row_id INTEGER,                      -- 최초 삽입된 queue.id를 기록(선택) 
		insert_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (id),
		UNIQUE(queue_info_id, deduplication_id),
		FOREIGN KEY (queue_info_id) REFERENCES queue_info(id) ON DELETE CASCADE
		);`
	_, err := m.db.Exec(createTableSQL)

	// -- (선택) 만료/조회 최적화용 보조 인덱스
	if err != nil {
		return err
	}
	createIndex := `CREATE INDEX IF NOT EXISTS idx_dedup_expires ON deduplication_log (expires_at);`
	_, err = m.db.Exec(createIndex)
	if err != nil {
		return err
	}
	return nil
}

func (m *FileDBManager) checkDuplicationID(tx *sql.Tx, queueInfoID int64, dedupID string) (int64, error) {
	// delete 만료된 로그
	if err := m.expireDeduplicationLogs(tx, queueInfoID, dedupID); err != nil {
		return 0, fmt.Errorf("error expiring deduplication logs: %w", err)
	}
	// 중복 검사 및 삽입 시도
	// 1시간 후 만료
	expired := fmt.Sprintf("+%d seconds", expired_deduplication_log_cleanup_interval_seconds)
	res, err := m.insertDeduplicationLogs(tx, queueInfoID, dedupID, expired)
	if err != nil {
		return 0, fmt.Errorf("error inserting deduplication log: %w", err)
	}
	return res, nil
}

// expireDeduplicationLogs 만료된 deduplication_log 삭제
func (m *FileDBManager) expireDeduplicationLogs(tx *sql.Tx, queue_info_id int64, dedupID string) error {
	_, err := tx.Exec(`
		DELETE FROM deduplication_log
		WHERE
		queue_info_id = ? AND
		deduplication_id = ? AND
		expires_at <= CURRENT_TIMESTAMP;`,
		queue_info_id, dedupID)
	return err
}

// dedup_log 삽입
func (m *FileDBManager) insertDeduplicationLogs(tx *sql.Tx, queue_info_id int64, dedupID string, expiresIn string) (int64, error) {
	insertQuery :=
		`INSERT INTO deduplication_log (queue_info_id, deduplication_id, expires_at)
		VALUES (?, ?, DATETIME('now', ?))
		ON CONFLICT(queue_info_id, deduplication_id) DO NOTHING;`
	res, err := tx.Exec(insertQuery, queue_info_id, dedupID, expiresIn)
	if err != nil {
		return 0, fmt.Errorf("error inserting deduplication log: %w", err)
	}
	resAffected, err := res.RowsAffected()
	if err != nil || resAffected <= 0 {
		return 0, queue_error.ErrDuplicate
	}
	// return deduplication log id
	resID, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("error getting last insert id: %w", err)
	}
	return resID, nil
}

// updateDeduplicationLogWithQueueRowID deduplication_log의 queue_row_id 업데이트
func (m *FileDBManager) updateDeduplicationLogWithQueueRowID(tx *sql.Tx, log_id int64, queue_row_id int64) error {
	updateQuery := `
		UPDATE deduplication_log
		SET queue_row_id = ?
		WHERE id = ?;`
	_, err := tx.Exec(updateQuery, queue_row_id, log_id)
	if err != nil {
		return fmt.Errorf("error updating deduplication log with queue_row_id: %w", err)
	}
	return nil
}

// deleteDeduplicationLogs deduplication_log 삭제
func (m *FileDBManager) deleteDeduplicationLogs(tx *sql.Tx, log_id int64) error {
	deleteQuery := `
		DELETE FROM deduplication_log
		WHERE id = ?;`
	_, err := tx.Exec(deleteQuery, log_id)
	if err != nil {
		return fmt.Errorf("error deleting deduplication log: %w", err)
	}
	return nil
}
