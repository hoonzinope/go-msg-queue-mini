package core

import "database/sql"

// create acked table
func (m *FileDBManager) createAckedTable() error {
	createTableSQL :=
		`CREATE TABLE IF NOT EXISTS acked (
		queue_info_id INTEGER NOT NULL,                  -- 큐 정보 ID
		group_name  TEXT NOT NULL,                       -- 컨슈머 그룹
		partition_id INTEGER NOT NULL DEFAULT 0,		 -- 파티션 ID
		global_id TEXT NOT NULL DEFAULT '',			 -- 글로벌 ID (복제시 사용, UUID 사용)
		acked_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (queue_info_id, group_name, partition_id, global_id),                   -- 큐별, 그룹별 메시지 1개만 점유 가능
		FOREIGN KEY (queue_info_id) REFERENCES queue_info(id) ON DELETE CASCADE
	);`
	_, err := m.db.Exec(createTableSQL)
	if err != nil {
		return err
	}
	createIndex := `CREATE INDEX IF NOT EXISTS idx_acked_lookup
  					ON acked(queue_info_id, global_id, partition_id, group_name);`
	_, err = m.db.Exec(createIndex)
	if err != nil {
		return err
	}
	return nil
}

// insertAcked acked 테이블에 삽입
func (m *FileDBManager) insertAcked(tx *sql.Tx, queueInfoID int64, group string, partitionID int, globalID string) error {
	// acked에 삽입
	if _, err := tx.Exec(`
			INSERT OR IGNORE INTO acked 
			(queue_info_id, group_name, partition_id, global_id) 
			VALUES (?, ?, ?, ?)`, queueInfoID, group, partitionID, globalID); err != nil {
		return err
	}
	return nil
}

// deleteAckedMsg acked 테이블에서 오래된 항목 삭제
func (m *FileDBManager) deleteAckedMsg(tx *sql.Tx) (int64, error) {
	res, err := tx.Exec(`
			DELETE FROM acked
			WHERE (queue_info_id, acked_at) IN (
				SELECT qi.id, a.acked_at
				FROM acked a
					JOIN queue_info qi 
						ON qi.id = a.queue_info_id
				WHERE a.acked_at <= DATETIME('now', printf('-%d seconds', qi.retention_s))
		);`)
	if err != nil {
		return 0, err
	}
	resAffected, err := res.RowsAffected()
	return resAffected, err
}
