package core

import (
	"database/sql"
	"fmt"
	"go-msg-queue-mini/internal"
	queue_error "go-msg-queue-mini/internal/queue_error"
	"sync"
	"time"

	"go-msg-queue-mini/util"

	_ "github.com/mattn/go-sqlite3"
)

type FileDBManager struct {
	db           *sql.DB
	stopChan     chan struct{}
	doneChan     chan struct{}
	stopSync     sync.Once
	queueType    string
	queueNameMap sync.Map // key: queue name, value: queue info id
}

type queueMsg struct {
	QueueName   string
	ID          int64
	Msg         []byte
	InsertTS    time.Time
	Receipt     string
	GlobalID    string // 복제시 큐메세지 식별용
	PartitionID int    // 복제시 파티션 식별용
}

var OneDayInSeconds = 24 * 60 * 60 // 24 hours

func NewFileDBManager(dsn string, queueType string) (*FileDBManager, error) {
	db, err := sql.Open("sqlite3", dsn) // dsn: "file:/path/db.sqlite3"
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA auto_vacuum=INCREMENTAL;`); err != nil {
		return nil, err
	}
	fm := &FileDBManager{
		db:        db,
		stopChan:  make(chan struct{}),
		doneChan:  make(chan struct{}),
		queueType: queueType,
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

func (m *FileDBManager) intervalJob() error {
	timer := time.NewTicker(time.Second * 60) // 60s
	defer func() {
		timer.Stop()
		close(m.doneChan)
	}()
	for {
		select {
		case <-timer.C:
			// fmt.Println("@@@ Running periodic cleanup tasks...")
			// 1. queue 테이블에서 오래된 항목 삭제
			deleteMsgCnt, err := m.deleteQueueMsg()
			if err != nil {
				util.Error(fmt.Sprintf("Error deleting queue messages: %v", err))
			}
			// 2. acked 테이블에서 오래된 항목 삭제
			deleteAckedCnt, err := m.deleteAckedMsg()
			if err != nil {
				util.Error(fmt.Sprintf("Error deleting acked messages: %v", err))
			}
			// 3. 실제로 지워진 데이터가 있을 경우, vacuum -> db 파일 크기 줄이기
			if (deleteAckedCnt + deleteMsgCnt) > 0 {
				if err := m.vacuum(); err != nil {
					util.Error(fmt.Sprintf("Error during vacuum: %v", err))
				}
			}
		case <-m.stopChan:
			// fmt.Println("@@@ Stopping periodic cleanup tasks...")
			return nil
		}
	}
}

func (m *FileDBManager) deleteQueueMsg() (int64, error) {
	return m.inTxWithCount(func(tx *sql.Tx) (int64, error) {
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
		return resAffected, err
	})
}

func (m *FileDBManager) deleteAckedMsg() (int64, error) {
	return m.inTxWithCount(func(tx *sql.Tx) (int64, error) {
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
	})
}

func (m *FileDBManager) vacuum() error {
	if _, err := m.db.Exec(`PRAGMA incremental_vacuum(1);`); err != nil {
		util.Error(fmt.Sprintf("Error during vacuum: %v", err))
		return err
	}
	return nil
}

func (m *FileDBManager) Close() error {
	m.stopSync.Do(func() {
		close(m.stopChan)
	})
	select {
	case <-m.doneChan:
	case <-time.After(3 * time.Second):
		util.Error("Timeout waiting for interval job to stop")
	}
	return m.db.Close()
}

func (m *FileDBManager) initDB() error {
	if err := m.createQueueInfoTable(); err != nil {
		return err
	}
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
	createIndex := `CREATE UNIQUE INDEX IF NOT EXISTS uq_queue_global ON queue(queue_info_id, global_id);`
	_, err = m.db.Exec(createIndex)
	if err != nil {
		return err
	}
	createIndex2 := `CREATE INDEX IF NOT EXISTS idx_queue_partition ON queue(queue_info_id, partition_id, id);`
	_, err = m.db.Exec(createIndex2)
	if err != nil {
		return err
	}
	return nil
}

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

func (m *FileDBManager) WriteMessage(queue_name string, msg []byte) (err error) {
	gid := util.GenerateGlobalID()
	pid := 0
	return m.WriteMessageWithMeta(queue_name, msg, gid, pid)
}

func (m *FileDBManager) WriteMessageWithMeta(queue_name string, msg []byte, globalID string, partitionID int) (err error) {
	queueInfoID, err := m.getQueueInfoID(queue_name)
	if queueInfoID < 0 || err != nil {
		return fmt.Errorf("queue not found: %s", queue_name)
	}
	return m.inTx(func(tx *sql.Tx) error {
		_, err = tx.Exec(`
		INSERT INTO queue 
		(queue_info_id, msg, global_id, partition_id) 
		VALUES (?, ?, ?, ?)`,
			queueInfoID, msg, globalID, partitionID)
		return err
	})
}

func (m *FileDBManager) WriteMessagesBatch(queue_name string, msgs [][]byte) (int, error) {
	pid := 0
	return m.WriteMessagesBatchWithMeta(queue_name, msgs, pid)
}

func (m *FileDBManager) WriteMessagesBatchWithMeta(queue_name string, msgs [][]byte, partitionID int) (int, error) {
	queueInfoID, err := m.getQueueInfoID(queue_name)
	if err != nil {
		return 0, err
	}
	successCount := 0
	err = m.inTx(func(tx *sql.Tx) error {
		for _, msg := range msgs {
			globalID := util.GenerateGlobalID()
			_, insertErr := tx.Exec(`
				INSERT INTO queue 
				(queue_info_id, msg, global_id, partition_id) 
				VALUES (?, ?, ?, ?)`,
				queueInfoID, msg, globalID, partitionID)
			if insertErr != nil {
				util.Error(fmt.Sprintf("Error writing message to queue: %v", insertErr))
				return insertErr
			}
			successCount++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return successCount, nil
}

func (m *FileDBManager) ReadMessage(queue_name, group, consumerID string, leaseSec int) (_ queueMsg, err error) {
	partitionID := 0
	return m.ReadMessageWithMeta(queue_name, group, partitionID, consumerID, leaseSec)
}

func (m *FileDBManager) ReadMessageWithMeta(queue_name, group string, partitionID int, consumerID string, leaseSec int) (_ queueMsg, err error) {
	queueInfoID, err := m.getQueueInfoID(queue_name)
	if queueInfoID < 0 || err != nil {
		return queueMsg{}, fmt.Errorf("queue not found: %s", queue_name)
	}
	// 트랜잭션 시작
	return m.inTxWithQueueMsg(func(tx *sql.Tx) (queueMsg, error) {
		// 1) 후보 조회
		var candID int64
		var globalID string
		err = tx.QueryRow(`
			SELECT q.id, q.global_id
			FROM queue q
			LEFT JOIN acked a   ON 
				a.queue_info_id = q.queue_info_id AND 
				a.global_id = q.global_id AND 
				a.group_name = ? AND 
				a.partition_id = ?
			LEFT JOIN inflight i ON
				i.queue_info_id = q.queue_info_id AND 
				i.q_id = q.id AND 
				i.group_name = ? AND 
				i.partition_id = ?
			WHERE
			q.queue_info_id = ? 
			AND a.global_id IS NULL
			AND (i.q_id IS NULL OR i.lease_until <= CURRENT_TIMESTAMP)
			AND q.partition_id = ?
			ORDER BY q.id ASC
			LIMIT 1
		`, group, partitionID, group, partitionID, queueInfoID, partitionID).Scan(&candID, &globalID)
		if err == sql.ErrNoRows {
			return queueMsg{}, queue_error.ErrEmpty
		}
		if err != nil {
			return queueMsg{}, err
		}

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
			return queueMsg{}, err
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			// 경합으로 못 집었음 → 상위 레벨에서 재호출(또는 이 함수 내부에서 짧은 루프)
			return queueMsg{}, queue_error.ErrContended
		}
		// 3) 내가 점유한 메시지 반환 (consumer_id로 한정)
		var msg queueMsg
		msg.QueueName = queue_name
		err = tx.QueryRow(`
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
	})
}

func (m *FileDBManager) AckMessage(queue_name, group string, msgID int64, receipt string) (err error) {
	var globalID string
	var partitionID int
	err = m.db.QueryRow(`SELECT global_id, partition_id FROM queue WHERE id = ?`, msgID).Scan(&globalID, &partitionID)
	if err != nil {
		return err
	}
	return m.AckMessageWithMeta(queue_name, group, partitionID, globalID, receipt)
}

func (m *FileDBManager) AckMessageWithMeta(queue_name, group string, partitionID int, globalID, receipt string) (err error) {
	queueInfoID, err := m.getQueueInfoID(queue_name)
	if queueInfoID < 0 || err != nil {
		return fmt.Errorf("queue not found: %s", queue_name)
	}

	return m.inTx(func(tx *sql.Tx) error {
		if _, err = tx.Exec(`
			DELETE FROM inflight WHERE 
				queue_info_id = ? AND 
				global_id = ? AND 
				partition_id = ? AND 
				group_name = ? AND 
				receipt = ?`, queueInfoID, globalID, partitionID, group, receipt); err != nil {
			return err
		}

		if _, err = tx.Exec(`
			INSERT OR IGNORE INTO acked 
			(queue_info_id, group_name, partition_id, global_id) 
			VALUES (?, ?, ?, ?)`, queueInfoID, group, partitionID, globalID); err != nil {
			return err
		}
		return nil
	})
}

func (m *FileDBManager) NackMessage(
	queue_name, group string, msgID int64, receipt string,
	backoff time.Duration, maxDeliveries int, reason string) (err error) {
	var globalID string
	var partitionID int
	err = m.db.QueryRow(`SELECT global_id, partition_id FROM queue WHERE id = ?`, msgID).Scan(&globalID, &partitionID)
	if err != nil {
		return err
	}
	return m.NackMessageWithMeta(queue_name, group, partitionID, globalID, receipt, backoff, maxDeliveries, reason)
}

func (m *FileDBManager) NackMessageWithMeta(
	queue_name, group string, partitionID int, globalID, receipt string,
	backoff time.Duration, maxDeliveries int, reason string) (err error) {
	queueInfoID, err := m.getQueueInfoID(queue_name)
	if queueInfoID < 0 || err != nil {
		return fmt.Errorf("queue not found: %s", queue_name)
	}

	return m.inTx(func(tx *sql.Tx) error {
		var retryCount int
		if err = tx.QueryRow(
			`SELECT delivery_count
		FROM inflight
		WHERE queue_info_id = ? AND global_id = ? AND partition_id = ? AND receipt = ?`,
			queueInfoID, globalID, partitionID, receipt).Scan(&retryCount); err != nil {
			return err
		}

		// 지수적 backoff 증가
		backoffSec := int(backoff.Seconds())
		if backoffSec < 1 {
			backoffSec = 1
		} // clamp
		jitter := util.GenerateJitter(backoffSec)
		backoffSec = backoffSec*(1<<(retryCount-1)) + jitter // 첫 호출 기준 2^(n-1)
		if backoffSec > OneDayInSeconds {
			backoffSec = OneDayInSeconds
		}

		res, err := tx.Exec(`
        UPDATE inflight
        SET lease_until    = DATETIME('now', ? || ' seconds'),
            delivery_count = delivery_count + 1,
            last_error     = ?
        WHERE queue_info_id = ? AND global_id = ? AND partition_id = ? AND receipt = ?
    `, backoffSec,
			reason,
			queueInfoID, globalID, partitionID, receipt)
		if err != nil {
			return err
		}

		n, _ := res.RowsAffected()
		if n == 0 {
			return queue_error.ErrContended
		}

		var dc int
		if err = tx.QueryRow(`
        SELECT delivery_count 
		FROM inflight 
		WHERE queue_info_id = ? AND group_name = ? AND global_id = ? AND partition_id = ? AND receipt = ?
    `, queueInfoID, group, globalID, partitionID, receipt).Scan(&dc); err != nil {
			return err
		}
		if dc > maxDeliveries {
			if _, err = tx.Exec(`
		INSERT INTO dlq(q_id, queue_info_id, global_id, partition_id, msg, failed_group, reason)
		SELECT q.id, q.queue_info_id, q.global_id, q.partition_id, q.msg, ?, ?
		FROM queue q WHERE q.queue_info_id = ? AND q.global_id = ? AND q.partition_id = ?
        `, group, reason, queueInfoID, globalID, partitionID); err != nil {
				return err
			}

			if _, err = tx.Exec(`
		DELETE FROM inflight 
		WHERE 
			queue_info_id = ? 
			AND group_name = ? 
			AND global_id = ? 
			AND partition_id = ? 
			AND receipt = ?`,
				queueInfoID, group, globalID, partitionID, receipt); err != nil {
				return err
			}
			if _, err = tx.Exec(`
		INSERT OR IGNORE INTO acked 
		(queue_info_id, group_name, partition_id, global_id) 
		VALUES (?, ?, ?, ?)`, queueInfoID, group, partitionID, globalID); err != nil {
				return err
			}
			fmt.Printf("Message %s exceeded max deliveries (%d). Moving to DLQ.\n", globalID, maxDeliveries)
		}
		return nil
	})
}

func (m *FileDBManager) GetStatus(queue_name string) (internal.QueueStatus, error) {
	var status internal.QueueStatus = internal.QueueStatus{
		QueueType:        m.queueType,
		QueueName:        queue_name,
		TotalMessages:    0,
		AckedMessages:    0,
		InflightMessages: 0,
		DLQMessages:      0,
	}
	queueInfoID, err := m.getQueueInfoID(queue_name)
	if queueInfoID < 0 || err != nil {
		return status, fmt.Errorf("queue not found: %s", queue_name)
	}
	// total messages
	row := m.db.QueryRow(`SELECT COUNT(*) FROM queue WHERE queue_info_id = ?`, queueInfoID)
	if err := row.Scan(&status.TotalMessages); err != nil {
		return status, err
	}
	// acked messages
	row = m.db.QueryRow(`SELECT COUNT(*) FROM acked WHERE queue_info_id = ?`, queueInfoID)
	if err := row.Scan(&status.AckedMessages); err != nil {
		return status, err
	}
	// inflight messages
	row = m.db.QueryRow(`SELECT COUNT(*) FROM inflight WHERE queue_info_id = ?`, queueInfoID)
	if err := row.Scan(&status.InflightMessages); err != nil {
		return status, err
	}
	// dlq messages
	row = m.db.QueryRow(`SELECT COUNT(*) FROM dlq WHERE queue_info_id = ?`, queueInfoID)
	if err := row.Scan(&status.DLQMessages); err != nil {
		return status, err
	}
	return status, nil
}

func (m *FileDBManager) GetAllStatus() ([]internal.QueueStatus, error) {
	rows, err := m.db.Query(`
		SELECT
		qi.name as queue_name,
		COALESCE(q.cnt, 0) as total_messages,
		COALESCE(a.cnt, 0) as acked_messages,
		COALESCE(i.cnt, 0) as inflight_messages,
		COALESCE(d.cnt, 0) as dlq_messages
	FROM queue_info qi
	LEFT JOIN (SELECT queue_info_id, COUNT(*) as cnt FROM queue GROUP BY queue_info_id) q ON q.queue_info_id = qi.id
	LEFT JOIN (SELECT queue_info_id, COUNT(*) as cnt FROM acked GROUP BY queue_info_id) a ON a.queue_info_id = qi.id
	LEFT JOIN (SELECT queue_info_id, COUNT(*) as cnt FROM inflight GROUP BY queue_info_id) i ON i.queue_info_id = qi.id
	LEFT JOIN (SELECT queue_info_id, COUNT(*) as cnt FROM dlq GROUP BY queue_info_id) d ON d.queue_info_id = qi.id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var statuses []internal.QueueStatus
	for rows.Next() {
		var status internal.QueueStatus
		status.QueueType = m.queueType
		if err := rows.Scan(
			&status.QueueName,
			&status.TotalMessages,
			&status.AckedMessages,
			&status.InflightMessages,
			&status.DLQMessages); err != nil {
			return nil, err
		}
		statuses = append(statuses, status)
	}
	return statuses, nil
}

func (m *FileDBManager) PeekMessage(queue_name, group string) (_ queueMsg, err error) {
	partitionID := 0
	return m.PeekMessageWithMeta(queue_name, group, partitionID)
}

func (m *FileDBManager) PeekMessageWithMeta(queue_name, group string, partitionID int) (_ queueMsg, err error) {
	queueInfoID, err := m.getQueueInfoID(queue_name)
	if queueInfoID < 0 || err != nil {
		return queueMsg{}, fmt.Errorf("queue not found: %s", queue_name)
	}
	// 트랜잭션 시작
	return m.inTxWithQueueMsg(func(tx *sql.Tx) (queueMsg, error) {
		var msg queueMsg
		msg.QueueName = queue_name
		// Implement the logic to peek a message from the queue
		err = tx.QueryRow(`
		SELECT 
		q.id, q.msg, q.insert_ts, "" as receipt, q.global_id, q.partition_id
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
		JOIN queue_info qi ON
			qi.id = q.queue_info_id
		WHERE
		  q.queue_info_id = ?  
		  AND a.global_id IS NULL
		  AND (i.q_id IS NULL OR i.lease_until <= CURRENT_TIMESTAMP)
		  AND q.partition_id = ?
		ORDER BY q.id ASC
		LIMIT 1
	`, group, partitionID,
			group, partitionID,
			queueInfoID, partitionID).Scan(&msg.ID, &msg.Msg, &msg.InsertTS, &msg.Receipt, &msg.GlobalID, &msg.PartitionID)
		if err == sql.ErrNoRows {
			return queueMsg{}, queue_error.ErrEmpty
		}
		if err != nil {
			return queueMsg{}, err
		}
		return msg, nil
	})
}

func (m *FileDBManager) RenewMessage(queue_name, group string, msgID int64, receipt string, extendSec int) error {
	var globalID string
	err := m.db.QueryRow(`SELECT global_id FROM queue WHERE id = ?`, msgID).Scan(&globalID)
	if err != nil {
		return err
	}
	return m.RenewMessageWithMeta(queue_name, group, globalID, receipt, extendSec)
}

func (m *FileDBManager) RenewMessageWithMeta(queue_name, group string, globalID, receipt string, extendSec int) error {
	if extendSec < 1 {
		extendSec = 1
	}
	queueInfoID, err := m.getQueueInfoID(queue_name)
	if queueInfoID < 0 || err != nil {
		return fmt.Errorf("queue not found: %s", queue_name)
	}

	return m.inTx(func(tx *sql.Tx) error {
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
	})
}

func (m *FileDBManager) inTx(txFunc func(tx *sql.Tx) error) error {
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
	err = txFunc(tx)
	return err
}

func (m *FileDBManager) inTxWithCount(txFunc func(tx *sql.Tx) (int64, error)) (int64, error) {
	tx, err := m.db.Begin()
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()
	count, err := txFunc(tx)
	return count, err
}

func (m *FileDBManager) inTxWithQueueMsg(txFunc func(tx *sql.Tx) (queueMsg, error)) (queueMsg, error) {
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
	queueMsg, err := txFunc(tx)
	return queueMsg, err
}
