package core

import (
	"database/sql"
	queue_error "go-msg-queue-mini/internal/queue_error"
)

// create inflight table
func (m *FileDBManager) createInflightTable() error {
	createTableSQL :=
		`CREATE TABLE IF NOT EXISTS inflight (
		q_id           INTEGER NOT NULL,
		queue_info_id  INTEGER NOT NULL,                  -- 큐 정보 ID
		group_name     TEXT    NOT NULL,
		consumer_id    TEXT    NOT NULL, -- 추가!
		lease_until    TIMESTAMP NOT NULL,
		delivery_count INTEGER NOT NULL DEFAULT 1,
		receipt        TEXT,
		last_error     TEXT,
		claimed_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		partition_id INTEGER NOT NULL DEFAULT 0,		 -- 파티션 ID
		global_id TEXT NOT NULL DEFAULT '',			 -- 글로벌 ID (복제시 사용, UUID 사용)
		PRIMARY KEY (queue_info_id, group_name, partition_id, q_id),
		FOREIGN KEY (q_id) REFERENCES queue(id) ON DELETE CASCADE,
		FOREIGN KEY (queue_info_id) REFERENCES queue_info(id) ON DELETE CASCADE
	);`
	_, err := m.db.Exec(createTableSQL)
	if err != nil {
		return err
	}
	createIndex := `CREATE INDEX IF NOT EXISTS idx_inflight_lease 
					ON inflight(queue_info_id, group_name, partition_id, lease_until);`
	_, err = m.db.Exec(createIndex)
	if err != nil {
		return err
	}
	createIndex2 := `CREATE UNIQUE INDEX IF NOT EXISTS idx_inflight_receipt 
					ON inflight(receipt);`
	_, err = m.db.Exec(createIndex2)
	if err != nil {
		return err
	}
	// 글로벌 ID + 파티션 ID + 큐 정보 ID + receipt으로도 조회 가능해야 함
	_, err = m.db.Exec(`
	CREATE INDEX IF NOT EXISTS idx_inflight_global_receipt
	ON inflight(queue_info_id, global_id, partition_id, receipt);
	`)
	if err != nil {
		return err
	}
	// 글로벌 ID + 파티션 ID + 큐 정보 ID + 그룹명으로도 조회 가능해야 함
	_, err = m.db.Exec(`
	CREATE INDEX IF NOT EXISTS idx_inflight_global_group
	ON inflight(queue_info_id, global_id, partition_id, group_name);
	`)
	if err != nil {
		return err
	}
	return nil
}

// 선점 시도 (UPSERT). leaseSec는 정수(초)
func (m *FileDBManager) upsertInflight(
	tx *sql.Tx, queueInfoID int64, group string, consumerID string, leaseSec int, candID int64, partitionID int, globalID string) error {
	// 2) 선점 시도 (UPSERT). leaseSec는 정수(초)
	res, err := tx.Exec(`
			INSERT INTO 
				inflight(
					q_id, queue_info_id, group_name, consumer_id, lease_until, 
					delivery_count, claimed_at, receipt, partition_id, 
					global_id)
			SELECT ?, ?, ?, ?, DATETIME('now', ? || ' seconds'),
				COALESCE(
				(SELECT delivery_count FROM inflight 
				WHERE queue_info_id = ? AND group_name=? AND partition_id=? AND q_id=?),0)+1,
				CURRENT_TIMESTAMP,
				lower(hex(randomblob(16))) AS receipt,
				?, ?
			WHERE NOT EXISTS (
				SELECT 1 FROM inflight
				WHERE queue_info_id = ? AND group_name=? AND partition_id=? AND q_id=? AND lease_until > CURRENT_TIMESTAMP
			)
			ON CONFLICT(queue_info_id, group_name, partition_id, q_id) DO UPDATE SET
			consumer_id    = excluded.consumer_id,
			lease_until    = excluded.lease_until,
			delivery_count = inflight.delivery_count + 1,
			claimed_at     = CURRENT_TIMESTAMP,
			receipt        = excluded.receipt
			WHERE inflight.lease_until <= CURRENT_TIMESTAMP
		`, candID, queueInfoID, group, consumerID, leaseSec,
		queueInfoID, group, partitionID, candID,
		partitionID, globalID,
		queueInfoID, group, partitionID, candID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if n == 0 || err != nil {
		// 경합으로 못 집었음 → 상위 레벨에서 재호출(또는 이 함수 내부에서 짧은 루프)
		return queue_error.ErrContended
	}
	return nil
}

// delete inflight record (ack)
func (m *FileDBManager) deleteInflight(tx *sql.Tx, queueInfoID int64, globalID string, partitionID int, group string, receipt string) (err error) {
	if _, err = tx.Exec(`
			DELETE FROM inflight WHERE 
				queue_info_id = ? AND 
				global_id = ? AND 
				partition_id = ? AND 
				group_name = ? AND 
				receipt = ?`, queueInfoID, globalID, partitionID, group, receipt); err != nil {
		return err
	}
	return nil
}

// get delivery count
func (m *FileDBManager) getInflightDeliveryCount(
	tx *sql.Tx, queueInfoID int64, globalID string, partitionID int, receipt string) (deliveryCount int, err error) {
	var retryCount int
	if err = tx.QueryRow(
		`SELECT delivery_count
		FROM inflight
		WHERE queue_info_id = ? AND global_id = ? AND partition_id = ? AND receipt = ?`,
		queueInfoID, globalID, partitionID, receipt).Scan(&retryCount); err != nil {
		return -1, err
	}
	return retryCount, nil
}

// update lease time, delivery count, last error (nack)
func (m *FileDBManager) updateInflightNack(
	tx *sql.Tx, queueInfoID int64, globalID string, partitionID int,
	receipt string, backoffSec int, lastError string) (err error) {
	res, err := tx.Exec(`
        UPDATE inflight
        SET lease_until    = DATETIME('now', ? || ' seconds'),
            delivery_count = delivery_count + 1,
            last_error     = ?
        WHERE queue_info_id = ? AND global_id = ? AND partition_id = ? AND receipt = ?
    	`, backoffSec,
		lastError,
		queueInfoID, globalID, partitionID, receipt)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return queue_error.ErrContended
	}
	return nil
}

// renew lease time
func (m *FileDBManager) renewInflightLease(
	tx *sql.Tx, queueInfoID int64, group, globalID string,
	receipt string, extendSec int) (err error) {
	res, err := tx.Exec(`
			UPDATE inflight
			SET lease_until = DATETIME('now', '+' || ? || ' seconds')
			WHERE queue_info_id = ? AND group_name = ? AND global_id = ? AND receipt = ?
			AND lease_until > CURRENT_TIMESTAMP
		`, extendSec,
		queueInfoID, group, globalID, receipt)
	if err != nil {
		return err
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return queue_error.ErrLeaseExpired
	}
	return nil
}
