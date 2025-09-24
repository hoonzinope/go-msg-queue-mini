package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/internal/metrics"
	"go-msg-queue-mini/internal/queue_error"
	"log/slog"
	"os"
	"sync"
	"time"
)

type fileDBQueue struct {
	manager        *FileDBManager
	groupLock      sync.Map // map[string]*sync.Mutex
	maxRetry       int
	retryInterval  time.Duration
	leaseDuration  time.Duration
	MQMetrics      *metrics.MQMetrics
	stopStatusChan chan struct{}
	registerOnce   sync.Once
	logger         *slog.Logger
}

func NewFileDBQueue(config *internal.Config, logger *slog.Logger) (*fileDBQueue, error) {
	dbpath := ""
	queueType := config.Persistence.Type
	switch queueType {
	case "memory":
		dbpath = ":memory:"
	case "file":
		if config.Persistence.Options.DirsPath == "" {
			return nil, fmt.Errorf("filedb_queue requires a valid directory path")
		}
		if info, err := os.Stat(config.Persistence.Options.DirsPath); err != nil || !info.IsDir() {
			// if not exist, create it
			if err := os.MkdirAll(config.Persistence.Options.DirsPath, 0755); err != nil {
				return nil, fmt.Errorf("failed to create directory: %w", err)
			}
		}
		dbpath = config.Persistence.Options.DirsPath + "/filedb_queue.db"
		// options
		dboptions := "?_txlock=immediate&_busy_timeout=5000&_journal=WAL&_sync=NORMAL&_fk=1"
		dbpath = fmt.Sprintf("file:%s%s", dbpath, dboptions)
	default:
		return nil, fmt.Errorf("unsupported queue type: %s", queueType)
	}

	retryInterval, err := time.ParseDuration(config.RetryInterval)
	if err != nil {
		return nil, fmt.Errorf("invalid retry interval: %w", err)
	}

	leaseDuration, err := time.ParseDuration(config.LeaseDuration)
	if err != nil {
		return nil, fmt.Errorf("invalid lease duration: %w", err)
	}

	manager, err := NewFileDBManager(dbpath, queueType, logger)
	if err != nil {
		return nil, err
	}
	fq := &fileDBQueue{
		manager:        manager,
		groupLock:      sync.Map{},
		maxRetry:       config.MaxRetry,
		retryInterval:  retryInterval,
		leaseDuration:  leaseDuration,
		MQMetrics:      metrics.NewMQMetrics(),
		stopStatusChan: make(chan struct{}),
		logger:         logger,
	}
	go fq.intervalStatusMetrics()
	return fq, nil
}

func (q *fileDBQueue) Lock(queue_name, group string) func() {
	key := fmt.Sprintf("%s:%s", queue_name, group)
	val, _ := q.groupLock.LoadOrStore(key, &sync.Mutex{})
	groupLock := val.(*sync.Mutex)
	groupLock.Lock()

	return func() {
		groupLock.Unlock()
	}
}

func (q *fileDBQueue) CreateQueue(queue_name string) error {
	return q.manager.CreateQueue(queue_name)
}

func (q *fileDBQueue) DeleteQueue(queue_name string) error {
	return q.manager.DeleteQueue(queue_name)
}

func (q *fileDBQueue) Enqueue(queue_name string, item interface{}) error {
	msg, err := json.Marshal(item)
	if err != nil {
		return err
	}
	if err := q.manager.WriteMessage(queue_name, msg); err != nil {
		q.logger.Error("Error writing message to queue", "error", err)
		return err
	}
	q.MQMetrics.EnqueueCounter.WithLabelValues(queue_name).Inc()
	return nil
}

func (q *fileDBQueue) EnqueueBatch(queue_name string, items []interface{}) (int64, error) {
	var successCount int64 = 0
	msgs := make([][]byte, 0, len(items))
	for _, item := range items {
		msg, err := json.Marshal(item)
		if err != nil {
			q.logger.Error("Error marshaling message", "error", err)
			return successCount, err
		}
		msgs = append(msgs, msg)
	}

	successCount, err := q.manager.WriteMessagesBatch(queue_name, msgs)
	if err != nil {
		q.logger.Error("Error writing batch messages to queue", "error", err)
		return successCount, err
	}
	q.MQMetrics.EnqueueCounter.WithLabelValues(queue_name).Add(float64(successCount))
	return successCount, nil
}

func (q *fileDBQueue) Dequeue(queue_name, consumer_group string, consumer_id string) (internal.QueueMessage, error) {
	unLock := q.Lock(queue_name, consumer_group)
	defer unLock()

	queueMessage := internal.QueueMessage{
		Payload: nil,
		ID:      -1,
		Receipt: "",
	}
	msg, err := q.manager.ReadMessage(queue_name, consumer_group, consumer_id, int(q.leaseDuration.Seconds()))
	if err != nil {
		switch err {
		case queue_error.ErrEmpty:
			return queueMessage, queue_error.ErrEmpty
		case queue_error.ErrContended:
			return queueMessage, queue_error.ErrContended
		default:
			return queueMessage, fmt.Errorf("unexpected error: %w", err)
		}
	}

	// 매니저가 아직 (queueMsg{}, nil)로 빈 큐를 표현한다면 방어
	if msg.ID == 0 || len(msg.Msg) == 0 {
		return queueMessage, queue_error.ErrEmpty
	}

	var item any
	if err := json.Unmarshal(msg.Msg, &item); err != nil {
		return queueMessage, err
	}
	// if item empty
	if item == nil {
		return queueMessage, fmt.Errorf("no message available")
	}

	queueMessage.Payload = item
	queueMessage.ID = msg.ID
	queueMessage.Receipt = msg.Receipt

	q.MQMetrics.DequeueCounter.WithLabelValues(queue_name).Inc()
	return queueMessage, nil
}

func (q *fileDBQueue) Ack(queue_name, consumer_group string, msg_id int64, receipt string) error {
	if err := q.manager.AckMessage(queue_name, consumer_group, msg_id, receipt); err != nil {
		q.logger.Error("Error acknowledging message", "error", err)
		return err
	}
	q.MQMetrics.AckCounter.WithLabelValues(queue_name).Inc()
	return nil
}

func (q *fileDBQueue) Nack(queue_name, consumer_group string, msg_id int64, receipt string) error {
	maxRetry := q.maxRetry
	retryInterval := q.retryInterval
	if err := q.manager.NackMessage(queue_name, consumer_group, msg_id, receipt, retryInterval, maxRetry, "Nack Message"); err != nil {
		q.logger.Error("Error not acknowledging message", "error", err)
		return err
	}
	q.MQMetrics.NackCounter.WithLabelValues(queue_name).Inc()
	return nil
}

func (q *fileDBQueue) Shutdown() error {
	q.registerOnce.Do(func() {
		close(q.stopStatusChan)
	})
	if err := q.manager.Close(); err != nil {
		q.logger.Error("Error shutting down queue", "error", err)
		return err
	}
	return nil
}

func (q *fileDBQueue) Status(queue_name string) (internal.QueueStatus, error) {
	return q.manager.GetStatus(queue_name)
}

// bellow new api for http, gRPC
func (q *fileDBQueue) Peek(queue_name, group_name string) (internal.QueueMessage, error) {
	msg, err := q.manager.PeekMessage(queue_name, group_name)
	if err != nil {
		if errors.Is(err, queue_error.ErrEmpty) {
			return internal.QueueMessage{}, err
		}
		return internal.QueueMessage{}, err
	}
	var item any
	if err := json.Unmarshal(msg.Msg, &item); err != nil {
		return internal.QueueMessage{}, err
	}
	return internal.QueueMessage{
		Payload: item,
		ID:      msg.ID,
		Receipt: "",
	}, nil
}

func (q *fileDBQueue) Renew(queue_name, group_name string, messageID int64, receipt string, extendSec int) error {
	return q.manager.RenewMessage(queue_name, group_name, messageID, receipt, extendSec)
}

func (q *fileDBQueue) intervalStatusMetrics() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			q.statusMetrics()
		case <-q.stopStatusChan:
			return
		}
	}
}

func (q *fileDBQueue) statusMetrics() {
	statuses, err := q.manager.GetAllStatus()
	if err != nil {
		q.logger.Error("Error getting all queue statuses", "error", err)
		return
	}
	for _, status := range statuses {
		q.MQMetrics.TotalMessages.WithLabelValues(status.QueueName).Set(float64(status.TotalMessages))
		q.MQMetrics.InFlightMessages.WithLabelValues(status.QueueName).Set(float64(status.InflightMessages))
		q.MQMetrics.DLQMessages.WithLabelValues(status.QueueName).Set(float64(status.DLQMessages))
	}
}
