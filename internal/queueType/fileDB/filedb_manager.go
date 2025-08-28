package fileDB

import (
	"database/sql"
	"errors"
	"fmt"
	"go-msg-queue-mini/internal"
	"sync"
	"time"

	"go-msg-queue-mini/util"

	_ "github.com/mattn/go-sqlite3"
)

type fileDBManager struct {
	db       *sql.DB
	stopChan chan struct{}
	doneChan chan struct{}
	stopSync sync.Once
}

type queueMsg struct {
	Id        int64
	Msg       []byte
	Insert_ts time.Time
	Receipt   string
	GlobalID  string // 복제시 큐메세지 식별용
	PartitionID int    // 복제시 파티션 식별용
}

var (
	ErrEmpty        = errors.New("queue empty")
	ErrContended    = errors.New("contention: message not claimed")
	ErrLeaseExpired = errors.New("lease expired")
)

func NewFileDBManager(dsn string) (*fileDBManager, error) {
	db, err := sql.Open("sqlite3", dsn) // dsn: "file:/path/db.sqlite3"
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA auto_vacuum=INCREMENTAL; VACUUM;`); err != nil {
		return nil, err
	}
	fm := &fileDBManager{
		db:       db,
		stopChan: make(chan struct{}),
		doneChan: make(chan struct{}),
	}
	if err := fm.initDB(); err != nil {
		_ = db.Close()
		return nil, err
	}
	go func() {
		_ = fm.intervalJob()
	}()
	return fm, nil
}

func (m *fileDBManager) intervalJob() error {
	timer := time.NewTicker(time.Second * 5) // 5s
	defer func() {
		timer.Stop()
		close(m.doneChan)
	}()
	for {
		select {
		case <-timer.C:
			fmt.Println("@@@ Running periodic cleanup tasks...")
			// 1. acked 테이블에서 오래된 항목 삭제
			if err := m.deleteAckedMsg(); err != nil {
				fmt.Println("Error deleting acked messages:", err)
			}
			// 2. queue 테이블에서 오래된 항목 삭제
			if err := m.deleteQueueMsg(); err != nil {
				fmt.Println("Error deleting queue messages:", err)
			}
			// 3. vacuum
			if err := m.vacuum(); err != nil {
				fmt.Println("Error during vacuum:", err)
			}
		case <-m.stopChan:
			fmt.Println("@@@ Stopping periodic cleanup tasks...")
			return nil
		}
	}
}

func (m *fileDBManager) deleteQueueMsg() error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()
	_, err = tx.Exec(`
        DELETE FROM queue
        WHERE id IN (
            SELECT q.id
            FROM queue q
            LEFT JOIN inflight i ON i.q_id = q.id
            WHERE i.q_id IS NULL
              AND q.insert_ts <= DATETIME('now', '-1 days')
        );
    `)
	return err
}

func (m *fileDBManager) deleteAckedMsg() error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()
	if _, err := tx.Exec(`DELETE FROM acked
			WHERE acked_at <= DATETIME('now', '-1 days');`); err != nil {
		return err
	}
	return nil
}

func (m *fileDBManager) vacuum() error {
	if _, err := m.db.Exec(`PRAGMA incremental_vacuum(1);`); err != nil {
		fmt.Println("Error during vacuum:", err)
		return err
	}
	return nil
}

func (m *fileDBManager) Close() error {
	m.stopSync.Do(func() {
		close(m.stopChan)
	})
	select {
	case <-m.doneChan:
	case <-time.After(3 * time.Second):
		fmt.Println("Timeout waiting for interval job to stop")
	}
	return m.db.Close()
}

func (m *fileDBManager) initDB() error {
	if err := m.createQueueTable(); err != nil {
		return err
	}
	if err := m.createInflightTable(); err != nil {
		return err
	}
	if err := m.createAckedTable(); err != nil {
		return err
	}
	if err := m.createDLQTable(); err != nil {
		return err
	}
	return nil
}

// create queue table
func (m *fileDBManager) createQueueTable() error {
	var res int
	err := m.db.QueryRow(`select 1 from queue;`).Scan(&res)
	if err != nil {
		return err
	}
	if res == 1 {
		return nil
	}

	createTableSQL :=
		`CREATE TABLE IF NOT EXISTS queue (
		id INTEGER PRIMARY KEY,                          -- rowid 기반 고유 PK
    	msg BLOB NOT NULL,                               -- 메시지 본문
		partition_id INTEGER NOT NULL DEFAULT 0,		 -- 파티션 ID
		global_id TEXT NOT NULL DEFAULT '',			 -- 글로벌 ID (복제시 사용, UUID 사용)
    	insert_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	);`
	_, err = m.db.Exec(createTableSQL) 
	if err != nil { return err }
	createIndex := `CREATE UNIQUE INDEX uq_queue_global ON queue(global_id);`
	_, err = m.db.Exec(createIndex)
	if err != nil { return err }
	createIndex2 := `CREATE INDEX idx_queue_partition ON queue(partition_id, id);`
	_, err = m.db.Exec(createIndex2)
	if err != nil { return err }
	return nil
}

// create inflight table
func (m *fileDBManager) createInflightTable() error {
	var res int
	err := m.db.QueryRow(`select 1 from inflight;`).Scan(&res)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if res == 1 {
		return nil
	}
	createTableSQL :=
		`CREATE TABLE IF NOT EXISTS inflight (
		q_id           INTEGER NOT NULL,
		group_name     TEXT    NOT NULL,
		consumer_id    TEXT    NOT NULL, -- 추가!
		lease_until    TIMESTAMP NOT NULL,
		delivery_count INTEGER NOT NULL DEFAULT 1,
		receipt        TEXT,
		last_error     TEXT,
		claimed_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		partition_id INTEGER NOT NULL DEFAULT 0,		 -- 파티션 ID
		global_id TEXT NOT NULL DEFAULT '',			 -- 글로벌 ID (복제시 사용, UUID 사용)
		PRIMARY KEY (group_name, partition_id, q_id)
	);`
	_, err = m.db.Exec(createTableSQL)
	if err != nil {
		return err
	}
	createIndex := `CREATE INDEX IF NOT EXISTS idx_inflight_lease ON inflight(group_name, partition_id, lease_until);`
	_, err = m.db.Exec(createIndex)
	if err != nil {
		return err
	}
	createIndex2 := `CREATE UNIQUE INDEX IF NOT EXISTS idx_inflight_receipt ON inflight(receipt);`
	_, err = m.db.Exec(createIndex2)
	if err != nil {
		return err
	}
	return nil
}

// create acked table
func (m *fileDBManager) createAckedTable() error {
	var res int
	err := m.db.QueryRow(`select 1 from acked;`).Scan(&res)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if res == 1 {
		return nil
	}
	createTableSQL :=
		`CREATE TABLE IF NOT EXISTS acked (
		group_name  TEXT NOT NULL,                       -- 컨슈머 그룹
		partition_id INTEGER NOT NULL DEFAULT 0,		 -- 파티션 ID
		global_id TEXT NOT NULL DEFAULT '',			 -- 글로벌 ID (복제시 사용, UUID 사용)
		acked_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (group_name, partition_id, global_id)                   -- 그룹별 메시지 1개만 점유 가능
	);`
	_, err = m.db.Exec(createTableSQL)
	return err
}

// create dlq table
func (m *fileDBManager) createDLQTable() error {
	var res int
	err := m.db.QueryRow(`select 1 from dlq;`).Scan(&res)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if res == 1 {
		return nil
	}
	createTableSQL :=
		`CREATE TABLE IF NOT EXISTS dlq (
		id INTEGER PRIMARY KEY,                          -- rowid 기반 PK
    	q_id INTEGER NOT NULL,                           -- 원본 queue.id
		partition_id INTEGER NOT NULL DEFAULT 0,		 -- 파티션 ID
		global_id TEXT NOT NULL DEFAULT '',			 -- 글로벌 ID (복제시 사용, UUID 사용)
    	msg BLOB,                                        -- 메시지 복사본
    	failed_group TEXT,                               -- 실패한 컨슈머 그룹
    	reason TEXT,                                     -- 실패 사유
    	insert_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP -- 그룹별 메시지 1개만 점유 가능
	);`
	_, err = m.db.Exec(createTableSQL)
	return err
}

func (m *fileDBManager) WriteMessage(msg []byte) (err error) {
	gid := util.GenerateGlobalID()
	pid := 0
	return m.WriteMessageWithMeta(msg, gid, pid)
}

func (m *fileDBManager) WriteMessageWithMeta(msg []byte, globalID string, partitionID int) (err error) {

	{
		tx, err := m.db.Begin()
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback()
			} else {
				err = tx.Commit()
			}
		}()
		_, err = tx.Exec(`INSERT INTO queue (msg, global_id, partition_id) VALUES (?, ?, ?)`, msg, globalID, partitionID)
		return err
	}
}

func (m *fileDBManager) ReadMessage(group, consumerID string, leaseSec int) (_ queueMsg, err error) {
	partitionID := 0
	return m.ReadMessageWithMeta(group, partitionID, consumerID, leaseSec)
}

func (m *fileDBManager) ReadMessageWithMeta(group string, partitionID int, consumerID string, leaseSec int) (_ queueMsg, err error) {
	tx, err := m.db.Begin()
	if err != nil {
		return queueMsg{}, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	// 1) 후보 조회
	var candID int64
	var globalID string
	err = tx.QueryRow(`
		SELECT q.id, q.global_id
		FROM queue q
		LEFT JOIN acked a   ON a.global_id = q.global_id AND a.group_name = ? AND a.partition_id = ?
		LEFT JOIN inflight i ON i.q_id = q.id AND i.group_name = ? AND i.partition_id = ?
		WHERE a.global_id IS NULL
		  AND (i.q_id IS NULL OR i.lease_until <= CURRENT_TIMESTAMP)
		  AND q.partition_id = ?
		ORDER BY q.id ASC
		LIMIT 1
	`, group, partitionID, group, partitionID, partitionID).Scan(&candID, &globalID)
	if err == sql.ErrNoRows {
		return queueMsg{}, ErrEmpty
	}
	if err != nil {
		return queueMsg{}, err
	}

	// 2) 선점 시도 (UPSERT). leaseSec는 정수(초)
	res, err := tx.Exec(`
		INSERT INTO inflight(q_id, group_name, consumer_id, lease_until, delivery_count, claimed_at, receipt, partition_id, global_id)
		SELECT ?, ?, ?, DATETIME('now', ? || ' seconds'),
			   COALESCE((SELECT delivery_count FROM inflight WHERE group_name=? AND partition_id=? AND q_id=?),0)+1,
			   CURRENT_TIMESTAMP,
			   lower(hex(randomblob(16))) AS receipt,
			   ?, ?
		WHERE NOT EXISTS (
			SELECT 1 FROM inflight
			WHERE group_name=? AND partition_id=? AND q_id=? AND lease_until > CURRENT_TIMESTAMP
		)
		ON CONFLICT(group_name, partition_id, q_id) DO UPDATE SET
		  consumer_id    = excluded.consumer_id,
		  lease_until    = excluded.lease_until,
		  delivery_count = inflight.delivery_count + 1,
		  claimed_at     = CURRENT_TIMESTAMP,
		  receipt        = excluded.receipt
		  WHERE inflight.lease_until <= CURRENT_TIMESTAMP
	`, candID, group, consumerID, leaseSec, group, partitionID, candID, partitionID, globalID, group, partitionID, candID)
	if err != nil {
		return queueMsg{}, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		// 경합으로 못 집었음 → 상위 레벨에서 재호출(또는 이 함수 내부에서 짧은 루프)
		return queueMsg{}, ErrContended
	}
	// 3) 내가 점유한 메시지 반환 (consumer_id로 한정)
	var msg queueMsg
	err = tx.QueryRow(`
		SELECT q.id, q.msg, q.insert_ts, i.receipt, q.global_id, q.partition_id
		FROM queue q
		JOIN inflight i ON i.q_id = q.id
		WHERE i.group_name = ? AND i.consumer_id = ? AND i.partition_id = ?
		ORDER BY i.claimed_at DESC
		LIMIT 1
	`, group, consumerID, partitionID).Scan(&msg.Id, &msg.Msg, &msg.Insert_ts, &msg.Receipt, &msg.GlobalID, &msg.PartitionID)
	if err != nil {
		return queueMsg{}, err
	}
	return msg, nil
}

func (m *fileDBManager) AckMessage(group string, msgID int64, receipt string) (err error) {
	var globalID string
	var partitionID int
	err = m.db.QueryRow(`SELECT global_id, partition_id FROM queue WHERE id = ?`, msgID).Scan(&globalID, &partitionID)
	if err != nil {
		return err
	}
	return m.AckMessageWithMeta(group, partitionID, globalID, receipt)
}

func (m *fileDBManager) AckMessageWithMeta(group string, partitionID int, globalID, receipt string) (err error) {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	if _, err = tx.Exec(`DELETE FROM inflight WHERE global_id = ? AND partition_id = ? AND group_name = ? AND receipt = ?`, globalID, partitionID, group, receipt); err != nil {
		return err
	}

	if _, err = tx.Exec(`INSERT OR IGNORE INTO acked (group_name, partition_id, global_id) VALUES (?, ?, ?)`, group, partitionID, globalID); err != nil {
		return err
	}
	return nil
}

func (m *fileDBManager) NackMessage(group string, msgID int64, receipt string, backoff time.Duration, maxDeliveries int, reason string) (err error) {
	var globalID string
	var partitionID int
	err = m.db.QueryRow(`SELECT global_id, partition_id FROM queue WHERE id = ?`, msgID).Scan(&globalID, &partitionID)
	if err != nil {
		return err
	}
	return m.NackMessageWithMeta(group, partitionID, globalID, receipt, backoff, maxDeliveries, reason)
}

func (m *fileDBManager) NackMessageWithMeta(group string, partitionID int, globalID, receipt string, backoff time.Duration, maxDeliveries int, reason string) (err error) {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	var retryCount int
	if err = tx.QueryRow(
		`SELECT delivery_count
		FROM inflight
		WHERE group_name = ? AND global_id = ? AND partition_id = ? AND receipt = ?`,
		group, globalID, partitionID, receipt).Scan(&retryCount); err != nil {
		return err
	}

	// 지수적 backoff 증가
	backoffSec := int(backoff.Seconds())
	if backoffSec < 1 {
		backoffSec = 1
	} // clamp
	jitter := util.GenerateJitter(backoffSec)
	backoffSec = backoffSec*(1<<(retryCount-1)) + jitter // 첫 호출 기준
	if backoffSec > 86400 {
		backoffSec = 86400
	}

	res, err := tx.Exec(`
        UPDATE inflight
        SET lease_until    = DATETIME('now', ? || ' seconds'),
            delivery_count = delivery_count + 1,
            last_error     = ?
        WHERE group_name = ? AND global_id = ? AND partition_id = ? AND receipt = ?
    `, backoffSec, reason, group, globalID, partitionID, receipt)
	if err != nil {
		return err
	}

	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrContended
	}

	var dc int
	if err = tx.QueryRow(`
        SELECT delivery_count FROM inflight WHERE group_name = ? AND global_id = ? AND partition_id = ? AND receipt = ?
    `, group, globalID, partitionID, receipt).Scan(&dc); err != nil {
		return err
	}
	if dc > maxDeliveries {
		if _, err = tx.Exec(`
		INSERT INTO dlq(q_id, global_id, partition_id, msg, failed_group, reason)
		SELECT q.id, q.global_id, q.partition_id, q.msg, ?, ?
		FROM queue q WHERE q.global_id = ?
        `, group, reason, globalID); err != nil {
			return err
		}

		if _, err = tx.Exec(`DELETE FROM inflight WHERE group_name = ? AND global_id = ? AND partition_id = ? AND receipt = ?`, group, globalID, partitionID, receipt); err != nil {
			return err
		}
		if _, err = tx.Exec(`INSERT OR IGNORE INTO acked (group_name, partition_id, global_id) VALUES (?, ?, ?)`, group, partitionID, globalID); err != nil {
			return err
		}
		fmt.Printf("Message %s exceeded max deliveries (%d). Moving to DLQ.\n", globalID, maxDeliveries)
	}
	return nil
}

func (m *fileDBManager) GetStatus() (internal.QueueStatus, error) {
	var status internal.QueueStatus = internal.QueueStatus{
		QueueType:        "fileDB",
		TotalMessages:    0,
		AckedMessages:    0,
		InflightMessages: 0,
		DLQMessages:      0,
	}
	// total messages
	row := m.db.QueryRow(`SELECT COUNT(*) FROM queue`)
	if err := row.Scan(&status.TotalMessages); err != nil {
		return status, err
	}
	// acked messages
	row = m.db.QueryRow(`SELECT COUNT(*) FROM acked`)
	if err := row.Scan(&status.AckedMessages); err != nil {
		return status, err
	}
	// inflight messages
	row = m.db.QueryRow(`SELECT COUNT(*) FROM inflight`)
	if err := row.Scan(&status.InflightMessages); err != nil {
		return status, err
	}
	// dlq messages
	row = m.db.QueryRow(`SELECT COUNT(*) FROM dlq`)
	if err := row.Scan(&status.DLQMessages); err != nil {
		return status, err
	}
	return status, nil
}

func (m *fileDBManager) PeekMessage(group string) (_ queueMsg, err error) {
	partitionID := 0
	return m.PeekMessageWithMeta(group, partitionID)
}

func (m *fileDBManager) PeekMessageWithMeta(group string, partitionID int) (_ queueMsg, err error) {
	tx, err := m.db.Begin()
	if err != nil {
		return queueMsg{}, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	var msg queueMsg
	// Implement the logic to peek a message from the queue
	err = tx.QueryRow(`
		SELECT q.id, q.msg, q.insert_ts, "" as receipt, q.global_id, q.partition_id
		FROM queue q
		LEFT JOIN acked a   ON a.global_id = q.global_id AND a.group_name = ? AND a.partition_id = ?
		LEFT JOIN inflight i ON i.q_id = q.id AND i.group_name = ? AND i.partition_id = ?
		WHERE a.global_id IS NULL
		  AND (i.q_id IS NULL OR i.lease_until <= CURRENT_TIMESTAMP)
		  AND q.partition_id = ?
		ORDER BY q.id ASC
		LIMIT 1
	`, group, partitionID, group, partitionID, partitionID).Scan(&msg.Id, &msg.Msg, &msg.Insert_ts, &msg.Receipt, &msg.GlobalID, &msg.PartitionID)
	if err == sql.ErrNoRows {
		return queueMsg{}, ErrEmpty
	}
	if err != nil {
		return queueMsg{}, err
	}
	return msg, nil
}

func (m *fileDBManager) RenewMessage(group string, msgID int64, receipt string, extendSec int) error {
	var globalID string
	err := m.db.QueryRow(`SELECT global_id FROM queue WHERE id = ?`, msgID).Scan(&globalID)
	if err != nil {
		return err
	}
	return m.RenewMessageWithMeta(group, globalID, receipt, extendSec)
}

func (m *fileDBManager) RenewMessageWithMeta(group string, globalID, receipt string, extendSec int) error {
	if extendSec < 1 {
		extendSec = 1
	}
	res, err := m.db.Exec(`
		UPDATE inflight
		SET lease_until = DATETIME('now', '+' || ? || ' seconds')
		WHERE group_name = ? AND global_id = ? AND receipt = ?
		AND lease_until > CURRENT_TIMESTAMP
	`, extendSec, group, globalID, receipt)
	if err != nil {
		return err
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrLeaseExpired
	}
	return nil
}
