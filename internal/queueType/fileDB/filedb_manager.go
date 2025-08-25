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
	stopSync sync.Once
}

type queueMsg struct {
	Id        int64
	Msg       []byte
	Insert_ts time.Time
}

var (
	ErrEmpty     = errors.New("queue empty")
	ErrContended = errors.New("contention: message not claimed")
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

func (m *fileDBManager) stop() error {
	m.stopChan <- struct{}{}
	return nil
}
func (m *fileDBManager) Close() error {
	m.stopSync.Do(func() {
		_ = m.stop()
	})
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
	createTableSQL :=
		`CREATE TABLE IF NOT EXISTS queue (
		id INTEGER PRIMARY KEY,                          -- rowid 기반 고유 PK
    	msg BLOB NOT NULL,                               -- 메시지 본문
    	insert_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	);`
	_, err := m.db.Exec(createTableSQL)
	return err
}

// create inflight table
func (m *fileDBManager) createInflightTable() error {
	createTableSQL :=
		`CREATE TABLE IF NOT EXISTS inflight (
		q_id           INTEGER NOT NULL,
		group_name     TEXT    NOT NULL,
		consumer_id    TEXT    NOT NULL, -- 추가!
		lease_until    TIMESTAMP NOT NULL,
		delivery_count INTEGER NOT NULL DEFAULT 1,
		last_error     TEXT,
		claimed_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (group_name, q_id)
	);`
	_, err := m.db.Exec(createTableSQL)
	if err != nil {
		return err
	}
	createIndex := `CREATE INDEX IF NOT EXISTS idx_inflight_lease ON inflight(group_name, lease_until);`
	_, err = m.db.Exec(createIndex)
	return err
}

// create acked table
func (m *fileDBManager) createAckedTable() error {
	createTableSQL :=
		`CREATE TABLE IF NOT EXISTS acked (
		q_id        INTEGER NOT NULL,                    -- queue.id
		group_name  TEXT NOT NULL,                       -- 컨슈머 그룹
		acked_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (group_name, q_id)                   -- 그룹별 메시지 1개만 점유 가능
	);`
	_, err := m.db.Exec(createTableSQL)
	return err
}

// create dlq table
func (m *fileDBManager) createDLQTable() error {
	createTableSQL :=
		`CREATE TABLE IF NOT EXISTS dlq (
		id INTEGER PRIMARY KEY,                          -- rowid 기반 PK
    	q_id INTEGER NOT NULL,                           -- 원본 queue.id
    	msg BLOB,                                        -- 메시지 복사본
    	failed_group TEXT,                               -- 실패한 컨슈머 그룹
    	reason TEXT,                                     -- 실패 사유
    	insert_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP -- 그룹별 메시지 1개만 점유 가능
	);`
	_, err := m.db.Exec(createTableSQL)
	return err
}

func (m *fileDBManager) WriteMessage(msg []byte) (err error) {
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
	_, err = tx.Exec(`INSERT INTO queue (msg) VALUES (?)`, msg)
	return err
}

func (m *fileDBManager) ReadMessage(group, consumerID string, leaseSec int) (_ queueMsg, err error) {
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
	err = tx.QueryRow(`
        SELECT q.id
        FROM queue q
        LEFT JOIN acked a   ON a.q_id = q.id AND a.group_name = ?
        LEFT JOIN inflight i ON i.q_id = q.id AND i.group_name = ?
        WHERE a.q_id IS NULL
          AND (i.q_id IS NULL OR i.lease_until <= CURRENT_TIMESTAMP)
        ORDER BY q.id ASC
        LIMIT 1
    `, group, group).Scan(&candID)
	if err == sql.ErrNoRows {
		return queueMsg{}, ErrEmpty
	}
	if err != nil {
		return queueMsg{}, err
	}

	// 2) 선점 시도 (UPSERT). leaseSec는 정수(초)
	res, err := tx.Exec(`
        INSERT INTO inflight(q_id, group_name, consumer_id, lease_until, delivery_count, claimed_at)
        SELECT ?, ?, ?, DATETIME('now', ? || ' seconds'),
               COALESCE((SELECT delivery_count FROM inflight WHERE group_name=? AND q_id=?),0)+1,
               CURRENT_TIMESTAMP
        WHERE NOT EXISTS (
            SELECT 1 FROM inflight
            WHERE group_name=? AND q_id=? AND lease_until > CURRENT_TIMESTAMP
        )
        ON CONFLICT(group_name, q_id) DO UPDATE SET
          consumer_id    = excluded.consumer_id,
          lease_until    = excluded.lease_until,
          delivery_count = inflight.delivery_count + 1,
          claimed_at     = CURRENT_TIMESTAMP
		  WHERE inflight.lease_until <= CURRENT_TIMESTAMP;
    `, candID, group, consumerID, leaseSec, group, candID, group, candID)
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
        SELECT q.id, q.msg, q.insert_ts
        FROM queue q
        JOIN inflight i ON i.q_id = q.id
        WHERE i.group_name = ? AND i.consumer_id = ?
        ORDER BY i.claimed_at DESC
        LIMIT 1
    `, group, consumerID).Scan(&msg.Id, &msg.Msg, &msg.Insert_ts)
	if err != nil {
		return queueMsg{}, err
	}

	return msg, nil
}

func (m *fileDBManager) AckMessage(group string, msgID int64) (err error) {
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
	if _, err = tx.Exec(`INSERT OR IGNORE INTO acked (q_id, group_name) VALUES (?, ?)`, msgID, group); err != nil {
		return err
	}
	if _, err = tx.Exec(`DELETE FROM inflight WHERE q_id = ? AND group_name = ?`, msgID, group); err != nil {
		return err
	}
	return nil
}

func (m *fileDBManager) NackMessage(group string, msgID int64, backoff time.Duration, maxDeliveries int, reason string) (err error) {
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
	backoffSecInt := int(backoff.Seconds()) // backoff를 초 단위 정수로 변환
	// 지수적 backoff 증가
	var retryCount int
	if err = tx.QueryRow(
		`SELECT delivery_count 
		FROM inflight 
		WHERE group_name = ? AND q_id = ?`,
		group, msgID).Scan(&retryCount); err != nil {
		return err
	}

	backoffSecInt = backoffSecInt * (1 << (retryCount - 1)) // 2^(retryCount-1) 배 증가
	jitter := util.GenerateJitter(backoffSecInt)
	backoffSecInt += jitter
	backoffSecInt = max(1, min(backoffSecInt, 86400)) // 1초~24시간 클램프 예시

	if _, err = tx.Exec(`
        UPDATE inflight
        SET lease_until    = DATETIME('now', ? || ' seconds'),
            delivery_count = delivery_count + 1,
            last_error     = ?
        WHERE group_name = ? AND q_id = ?
    `, backoffSecInt, reason, group, msgID); err != nil {
		return err
	}

	var dc int
	if err = tx.QueryRow(`
        SELECT delivery_count FROM inflight WHERE group_name = ? AND q_id = ?
    `, group, msgID).Scan(&dc); err != nil {
		return err
	}
	if dc > maxDeliveries {
		if _, err = tx.Exec(`
		INSERT INTO dlq(q_id, msg, failed_group, reason)
		SELECT q.id, q.msg, ?, ?
		FROM queue q WHERE q.id = ?
        `, group, reason, msgID); err != nil {
			return err
		}

		if _, err = tx.Exec(`INSERT OR IGNORE INTO acked (q_id, group_name) VALUES (?, ?)`, msgID, group); err != nil {
			return err
		}
		if _, err = tx.Exec(`DELETE FROM inflight WHERE group_name = ? AND q_id = ?`, group, msgID); err != nil {
			return err
		}
		fmt.Printf("Message %d exceeded max deliveries (%d). Moving to DLQ.\n", msgID, maxDeliveries)
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
