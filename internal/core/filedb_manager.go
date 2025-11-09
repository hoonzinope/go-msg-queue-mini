package core

import (
	"database/sql"
	"fmt"
	"go-msg-queue-mini/internal"
	queue_error "go-msg-queue-mini/internal/queue_error"
	"log/slog"
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
	logger       *slog.Logger
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

func NewFileDBManager(dsn string, queueType string, logger *slog.Logger) (*FileDBManager, error) {
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
		logger:    logger,
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
			deleteErr := m.deleteInterval()
			if deleteErr != nil {
				m.logger.Warn("Error during periodic cleanup", "error", deleteErr)
			}
		case <-m.stopChan:
			// fmt.Println("@@@ Stopping periodic cleanup tasks...")
			return nil
		}
	}
}

func (m *FileDBManager) Close() error {
	m.stopSync.Do(func() {
		close(m.stopChan)
	})
	select {
	case <-m.doneChan:
	case <-time.After(3 * time.Second):
		m.logger.Info("Timeout waiting for interval job to stop")
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
	if err := m.createDeduplicationLogTable(); err != nil {
		return err
	}
	return nil
}

func (m *FileDBManager) deleteInterval() error {
	return m.inTx(func(tx *sql.Tx) error {
		deleteCnt, deleteQueueErr := m.deleteQueueMsg(tx)
		if deleteQueueErr != nil {
			return deleteQueueErr
		}
		ackedCnt, deleteAckedErr := m.deleteAckedMsg(tx)
		if deleteAckedErr != nil {
			return deleteAckedErr
		}
		deleteExpiredDupLogErr := m.expireAllDeduplicationLogs(tx)
		if deleteExpiredDupLogErr != nil {
			return deleteExpiredDupLogErr
		}
		if deleteCnt > 0 || ackedCnt > 0 {
			if _, err := tx.Exec(`PRAGMA incremental_vacuum(1);`); err != nil {
				return err
			}
		}
		return nil
	})
}

func (m *FileDBManager) WriteMessage(
	queue_name string, msg []byte, globalID string, partitionID int, delay string, deduplicationID string) (err error) {
	queueInfoID, err := m.getQueueInfoID(queue_name)
	if queueInfoID < 0 || err != nil {
		return fmt.Errorf("queue not found: %s", queue_name)
	}
	mod, err := util.DelayToSeconds(delay) // "+0 seconds"
	if err != nil {
		return err
	}
	return m.inTx(func(tx *sql.Tx) error {
		// deduplicationID가 있으면 중복 검사
		if deduplicationID != "" {
			logId, err := m.checkDuplicationID(tx, queueInfoID, deduplicationID)
			if err != nil {
				return fmt.Errorf("error checking duplication ID: %w", err)
			}
			// 메시지 삽입
			queueRowId, err := m.insertMessage(tx, queueInfoID, msg, globalID, partitionID, mod)
			if err != nil || queueRowId <= 0 {
				_ = m.deleteDeduplicationLogs(tx, logId)
				return fmt.Errorf("error inserting message: %w", err)
			}
			// log_id, queue_row_id 업데이트
			if err := m.updateDeduplicationLogWithQueueRowID(tx, logId, queueRowId); err != nil {
				_ = m.deleteDeduplicationLogs(tx, logId)
				return fmt.Errorf("error updating deduplication log with queue row ID: %w", err)
			}
			return nil
		} else {
			// 메시지 삽입
			queueRowId, err := m.insertMessage(tx, queueInfoID, msg, globalID, partitionID, mod)
			if err != nil || queueRowId <= 0 {
				return fmt.Errorf("error inserting message: %w", err)
			}
			return nil
		}
	})
}

func (m *FileDBManager) WriteMessagesBatchWithMeta(
	queue_name string, msgs [][]byte, partitionID int, delays []string, deduplicationIDs []string) (int64, error) {
	queueInfoID, err := m.getQueueInfoID(queue_name)
	if err != nil {
		return 0, err
	}

	successCount, err := m.inTxWithCount(func(tx *sql.Tx) (int64, error) {
		successCount, err := m.insertMessageBatchStopOnFailure(tx, queueInfoID, msgs, partitionID, delays, deduplicationIDs)
		return successCount, err
	})
	if err != nil {
		return 0, err
	}
	return successCount, nil
}

func (m *FileDBManager) WriteMessagesBatchWithMetaAndReturnFailed(
	queue_name string, msgs [][]byte, partitionID int,
	delays []string, deduplicationIDs []string) (int64, []internal.FailedMessage, error) {
	queueInfoID, err := m.getQueueInfoID(queue_name)
	if err != nil {
		return 0, nil, err
	}
	var returnFailedMessages []internal.FailedMessage
	successCount, err := m.inTxWithCount(func(tx *sql.Tx) (int64, error) {
		cnt, failedMessages, err := m.insertMessageBatchReturnFailed(tx, queueInfoID, msgs, partitionID, delays, deduplicationIDs, 0)
		for _, fm := range failedMessages {
			returnFailedMessages = append(returnFailedMessages, internal.FailedMessage{
				Index:   fm.Index,
				Reason:  fm.Reason,
				Message: fm.Message,
			})
		}
		if err != nil {
			return 0, err
		}
		return cnt, nil
	})
	return successCount, returnFailedMessages, err
}

func (m *FileDBManager) ReadMessage(queue_name, group string, partitionID int, consumerID string, leaseSec int) (_ queueMsg, err error) {
	queueInfoID, err := m.getQueueInfoID(queue_name)
	if queueInfoID < 0 || err != nil {
		return queueMsg{}, fmt.Errorf("queue not found: %s", queue_name)
	}
	// 트랜잭션 시작
	return m.inTxWithQueueMsg(func(tx *sql.Tx) (queueMsg, error) {
		// 1) 후보 조회
		candidateMsg, selectCandidateErr := m.getCandidateQueueMsg(tx, queueInfoID, group, partitionID)
		if selectCandidateErr != nil {
			return queueMsg{}, selectCandidateErr
		}
		if candidateMsg.ID < 0 || candidateMsg.GlobalID == "" {
			return queueMsg{}, queue_error.ErrEmpty
		}

		// 2) 선점 시도 (UPSERT). leaseSec는 정수(초)
		err = m.upsertInflight(tx, queueInfoID, group, consumerID, leaseSec, candidateMsg.ID, partitionID, candidateMsg.GlobalID)
		if err != nil {
			return queueMsg{}, err
		}
		// 3) 내가 점유한 메시지 반환 (consumer_id로 한정)
		var msg queueMsg
		msg, err := m.getLeaseMsg(tx, queueInfoID, group, consumerID, partitionID)
		if err != nil {
			return queueMsg{}, err
		}
		msg.QueueName = queue_name
		return msg, nil
	})
}

func (m *FileDBManager) AckMessage(queue_name, group string, msgID int64, receipt string) (err error) {
	queueInfoID, err := m.getQueueInfoID(queue_name)
	if queueInfoID < 0 || err != nil {
		return fmt.Errorf("queue not found: %s", queue_name)
	}

	return m.inTx(func(tx *sql.Tx) error {
		// msgID -> globalID, partitionID 조회
		globalID, partitionID, checkErr := m.existsMsgID(tx, msgID)
		if checkErr != nil {
			return checkErr
		}
		// inflight에서 삭제
		inFlightDeleteErr := m.deleteInflight(tx, queueInfoID, globalID, partitionID, group, receipt)
		if inFlightDeleteErr != nil {
			return inFlightDeleteErr
		}
		// acked에 삽입
		ackErr := m.insertAcked(tx, queueInfoID, group, partitionID, globalID)
		if ackErr != nil {
			return ackErr
		}
		return nil
	})
}

func (m *FileDBManager) NackMessage(
	queue_name, group string, msgID int64, receipt string,
	backoff time.Duration, maxDeliveries int, reason string) (err error) {
	queueInfoID, err := m.getQueueInfoID(queue_name)
	if queueInfoID < 0 || err != nil {
		return fmt.Errorf("queue not found: %s", queue_name)
	}

	return m.inTx(func(tx *sql.Tx) error {
		var globalID string
		var partitionID int
		// msgID -> globalID, partitionID 조회
		globalID, partitionID, checkErr := m.existsMsgID(tx, msgID)
		if checkErr != nil {
			return checkErr
		}

		// 현재 delivery_count 조회
		deliveryCount, deliveryCntErr := m.getInflightDeliveryCount(tx, queueInfoID, globalID, partitionID, receipt)
		if deliveryCntErr != nil || deliveryCount < 1 {
			return deliveryCntErr
		}

		if deliveryCount < maxDeliveries {
			// 지수적 backoff 증가
			backoffSec := int(backoff.Seconds())
			if backoffSec < 1 {
				backoffSec = 1
			} // clamp
			jitter := util.GenerateJitter(backoffSec)
			backoffSec = backoffSec*(1<<(deliveryCount-1)) + jitter // 첫 호출 기준 2^(n-1)
			if backoffSec > OneDayInSeconds {
				backoffSec = OneDayInSeconds
			}

			// inflight 업데이트
			updateInflightNackErr := m.updateInflightNack(
				tx, queueInfoID, globalID, partitionID,
				receipt, backoffSec, reason)
			if updateInflightNackErr != nil {
				return updateInflightNackErr
			}
		} else {
			// 최대 재전송 횟수 초과 → DLQ로 이동
			insertDLQErr := m.insertDLQ(tx, queueInfoID, partitionID, globalID, nil, group, reason)
			if insertDLQErr != nil {
				return insertDLQErr
			}

			// inflight에서 삭제
			deleteInflightErr := m.deleteInflight(tx, queueInfoID, globalID, partitionID, group, receipt)
			if deleteInflightErr != nil {
				return deleteInflightErr
			}

			// acked에도 삽입(중복 방지용)
			insertAckedErr := m.insertAcked(tx, queueInfoID, group, partitionID, globalID)
			if insertAckedErr != nil {
				return insertAckedErr
			}

			// 로그 남기기
			m.logger.Warn("Message exceeded max deliveries, moved to DLQ",
				"queue", queue_name,
				"group", group,
				"global_id", globalID,
				"partition_id", partitionID,
				"max_deliveries", maxDeliveries,
			)
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

func (m *FileDBManager) PeekMessage(queue_name, group string, partitionID int) (_ queueMsg, err error) {
	queueInfoID, err := m.getQueueInfoID(queue_name)
	if queueInfoID < 0 || err != nil {
		return queueMsg{}, fmt.Errorf("queue not found: %s", queue_name)
	}
	// 트랜잭션 시작
	return m.inTxWithQueueMsg(func(tx *sql.Tx) (queueMsg, error) {
		var msg queueMsg
		msg, err := m.getCandidateQueueMsg(tx, queueInfoID, group, partitionID)
		if err != nil {
			return queueMsg{}, err
		}
		msg.QueueName = queue_name
		return msg, nil
	})
}

func (m *FileDBManager) PeekMessages(queue_name, group string, partitionID int, options internal.PeekOptions) ([]queueMsg, error) {
	queueInfoID, err := m.getQueueInfoID(queue_name)
	if queueInfoID < 0 || err != nil {
		return nil, fmt.Errorf("queue not found: %s", queue_name)
	}
	// 트랜잭션 시작
	var result []queueMsg
	err = m.inTx(func(tx *sql.Tx) error {
		msgs, err := m.peekCandidateQueueMsgsWithOptions(tx, queueInfoID, group, partitionID, options)
		if err != nil {
			return err
		}
		result = append(result, msgs...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	for i := range result {
		result[i].QueueName = queue_name
	}
	return result, nil
}

func (m *FileDBManager) GetMessageDetail(queue_name string, messageId int64) (queueMsg, error) {
	queueInfoID, err := m.getQueueInfoID(queue_name)
	if queueInfoID < 0 || err != nil {
		return queueMsg{}, fmt.Errorf("queue not found: %s", queue_name)
	}
	// 트랜잭션 시작
	result, err := m.inTxWithQueueMsg(func(tx *sql.Tx) (queueMsg, error) {
		msg, err := m.getQueueMsgByID(tx, queueInfoID, messageId)
		if err != nil {
			return queueMsg{}, err
		}
		return msg, nil
	})
	if err != nil {
		return queueMsg{}, err
	}
	result.QueueName = queue_name
	return result, nil
}

func (m *FileDBManager) RenewMessage(queue_name, group string, msgID int64, receipt string, extendSec int) error {
	if extendSec < 1 {
		extendSec = 1
	}
	queueInfoID, err := m.getQueueInfoID(queue_name)
	if queueInfoID < 0 || err != nil {
		return fmt.Errorf("queue not found: %s", queue_name)
	}

	return m.inTx(func(tx *sql.Tx) error {
		globalID, _, err := m.existsMsgID(tx, msgID)
		if err != nil {
			return err
		}
		renewErr := m.renewInflightLease(tx, queueInfoID, group, globalID, receipt, extendSec)
		if renewErr != nil {
			return renewErr
		}
		return nil
	})
}

func (m *FileDBManager) ListDLQMessages(queue_name string, options internal.PeekOptions) ([]DLQMessage, error) {
	queueInfoID, err := m.getQueueInfoID(queue_name)
	if queueInfoID < 0 || err != nil {
		return nil, fmt.Errorf("queue not found: %s", queue_name)
	}
	// 트랜잭션 시작
	var result []DLQMessage
	err = m.inTx(func(tx *sql.Tx) error {
		msgs, err := m.ListDLQ(tx, queueInfoID, options)
		if err != nil {
			return err
		}
		result = append(result, msgs...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (m *FileDBManager) GetDLQMessageDetail(queue_name string, messageId int64) (DLQMessage, error) {
	queueInfoID, err := m.getQueueInfoID(queue_name)
	if queueInfoID < 0 || err != nil {
		return DLQMessage{}, fmt.Errorf("queue not found: %s", queue_name)
	}
	// 트랜잭션 시작
	var dlqMsg DLQMessage
	txErr := m.inTx(func(tx *sql.Tx) error {
		msg, queryErr := m.DetailDLQ(tx, queueInfoID, messageId)
		if queryErr != nil {
			return queryErr
		}
		dlqMsg = msg
		return nil
	})
	if txErr != nil {
		return DLQMessage{}, txErr
	}
	return dlqMsg, nil
}

func (m *FileDBManager) RedriveDLQMessages(queue_name string, messageIDs []int64) error {
	queueInfoID, err := m.getQueueInfoID(queue_name)
	if queueInfoID < 0 || err != nil {
		return fmt.Errorf("queue not found: %s", queue_name)
	}
	return m.inTx(func(tx *sql.Tx) error {
		for _, msgID := range messageIDs {
			// DLQ에서 메시지 조회
			msg, dlqDetailErr := m.DetailDLQ(tx, queueInfoID, msgID)
			if dlqDetailErr != nil {
				return dlqDetailErr
			}
			mod, _ := util.DelayToSeconds("") // 즉시 재삽입
			global_id := util.GenerateGlobalID()
			// 메시지 재삽입
			_, insertErr := m.insertMessage(tx, queueInfoID, msg.Msg, global_id, msg.PartitionID, mod)
			if insertErr != nil {
				return insertErr
			}
			// DLQ에서 메시지 삭제
			deleteDLQErr := m.deleteDLQByID(tx, queueInfoID, msgID)
			if deleteDLQErr != nil {
				return deleteDLQErr
			}
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
