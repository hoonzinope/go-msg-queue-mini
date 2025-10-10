package core

import (
	"database/sql"
	"fmt"
)

// create queue info table
func (m *FileDBManager) createQueueInfoTable() error {
	createTableSQL :=
		`CREATE TABLE IF NOT EXISTS queue_info (
		id INTEGER PRIMARY KEY,                          -- rowid 기반 고유 PK
		name TEXT NOT NULL UNIQUE,                        -- 큐 이름
		retention_s  INTEGER NOT NULL DEFAULT 86400,  -- 메시지 보관기간(초) 정책
		max_delivery INTEGER NOT NULL DEFAULT 10,     -- 기본 최대 재시도
		backoff_base INTEGER NOT NULL DEFAULT 1,      -- 기본 백오프 초
		partition_n  INTEGER NOT NULL DEFAULT 1,      -- 파티션 개수(미래 대비)
		created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	);`
	_, err := m.db.Exec(createTableSQL)
	return err
}

func (m *FileDBManager) getQueueInfoID(name string) (int64, error) {
	var id int64
	if v, ok := m.queueNameMap.Load(name); ok {
		return v.(int64), nil
	}
	err := m.db.QueryRow(`SELECT id FROM queue_info WHERE name = ?`, name).Scan(&id)
	if err == sql.ErrNoRows {
		return -1, fmt.Errorf("queue not found: %s", name)
	}
	if err != nil {
		return -1, err
	}
	m.queueNameMap.Store(name, id)
	return id, nil
}

func (m *FileDBManager) ListQueues() ([]string, error) {
	rows, err := m.db.Query(`SELECT name FROM queue_info ORDER BY name ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var queues []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		queues = append(queues, name)
	}
	return queues, nil
}

func (m *FileDBManager) CreateQueue(name string) error {
	defaultRetention := OneDayInSeconds
	defaultMaxDelivery := 10
	defaultBackoffBase := 1
	defaultPartitionN := 1
	return m.createQueue(name, defaultRetention, defaultMaxDelivery, defaultBackoffBase, defaultPartitionN)
}

func (m *FileDBManager) createQueue(name string, retentionSec, maxDelivery, backoffBase, partitionN int) error {

	if name == "" {
		return fmt.Errorf("queue name is required")
	}
	// 중복 검사
	if _, err := m.getQueueInfoID(name); err == nil {
		return nil // 이미 존재하면 그냥 통과
	}

	row, err := m.db.Exec(`
		INSERT INTO queue_info (name, retention_s, max_delivery, backoff_base, partition_n)
		VALUES (?, ?, ?, ?, ?)`, name, retentionSec, maxDelivery, backoffBase, partitionN)
	if err != nil {
		return err
	}
	rowID, err := row.LastInsertId()
	if err != nil {
		return err
	}
	m.queueNameMap.Store(name, rowID)
	return nil
}

func (m *FileDBManager) DeleteQueue(name string) error {
	// 존재하는지 확인
	if _, err := m.getQueueInfoID(name); err != nil {
		return err
	}

	_, err := m.db.Exec(`DELETE FROM queue_info WHERE name = ?`, name)
	if err != nil {
		return err
	}
	m.queueNameMap.Delete(name)
	return nil
}
