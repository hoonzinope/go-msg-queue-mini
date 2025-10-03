package core

import "database/sql"

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
