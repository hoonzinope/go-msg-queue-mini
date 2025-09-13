package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/internal/metrics"
	"go-msg-queue-mini/internal/queue_error"
	"go-msg-queue-mini/util"
	"sync"
	"time"
)

type fileDBQueue struct {
	manager        *FileDBManager
	mu             sync.Mutex
	groupLock      map[string]*sync.Mutex
	maxRetry       int
	retryInterval  time.Duration
	leaseDuration  time.Duration
	MQMetrics      *metrics.MQMetrics
	stopStatusChan chan struct{}
	registerOnce   sync.Once
}

func NewFileDBQueue(config *internal.Config) (*fileDBQueue, error) {
	dbpath := config.Persistence.Options.DirsPath + "/filedb_queue.db"
	// options
	dboptions := "?_txlock=immediate&_busy_timeout=5000&_journal=WAL&_sync=NORMAL&_fk=1"
	dbpath = fmt.Sprintf("file:%s%s", dbpath, dboptions)
	queueType := config.Persistence.Type
	if queueType == "memory" {
		dbpath = ":memory:"
	}

	retryInterval, err := time.ParseDuration(config.RetryInterval)
	if err != nil {
		return nil, fmt.Errorf("invalid retry interval: %w", err)
	}

	leaseDuration, err := time.ParseDuration(config.LeaseDuration)
	if err != nil {
		return nil, fmt.Errorf("invalid lease duration: %w", err)
	}

	manager, err := NewFileDBManager(dbpath, queueType)
	if err != nil {
		return nil, err
	}
	fq := &fileDBQueue{
		manager:        manager,
		groupLock:      map[string]*sync.Mutex{},
		maxRetry:       config.MaxRetry,
		retryInterval:  retryInterval,
		leaseDuration:  leaseDuration,
		MQMetrics:      metrics.NewMQMetrics(),
		stopStatusChan: make(chan struct{}),
	}
	go fq.intervalStatusMetrics()
	return fq, nil
}

func (q *fileDBQueue) Lock(group string) func() {
	q.mu.Lock()
	defer q.mu.Unlock()

	groupLock, ok := q.groupLock[group]
	if !ok {
		groupLock = &sync.Mutex{}
		q.groupLock[group] = groupLock
	}
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
		fmt.Println("Error writing message to queue:", err)
		return err
	}
	q.MQMetrics.EnqueueCounter.WithLabelValues(queue_name).Inc()
	return nil
}

func (q *fileDBQueue) Dequeue(queue_name, consumer_group string, consumer_id string) (internal.QueueMessage, error) {
	unLock := q.Lock(consumer_group)
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
		util.Error(fmt.Sprintf("Error acknowledging message: %v", err))
		return err
	}
	q.MQMetrics.AckCounter.WithLabelValues(queue_name).Inc()
	return nil
}

func (q *fileDBQueue) Nack(queue_name, consumer_group string, msg_id int64, receipt string) error {
	maxRetry := q.maxRetry
	retryInterval := q.retryInterval
	if err := q.manager.NackMessage(queue_name, consumer_group, msg_id, receipt, retryInterval, maxRetry, "Nack Message"); err != nil {
		util.Error(fmt.Sprintf("Error not acknowledging message: %v", err))
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
		util.Error(fmt.Sprintf("Error shutting down queue: %v", err))
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
	queue_names, err := q.manager.ListQueues()
	if err != nil {
		util.Error(fmt.Sprintf("Error listing queues: %v", err))
		return
	}
	for _, name := range queue_names {
		status, err := q.manager.GetStatus(name)
		if err != nil {
			util.Error(fmt.Sprintf("Error getting queue status: %v", err))
		}
		q.MQMetrics.TotalMessages.WithLabelValues(name).Set(float64(status.TotalMessages))
		q.MQMetrics.InFlightMessages.WithLabelValues(name).Set(float64(status.InflightMessages))
		q.MQMetrics.DLQMessages.WithLabelValues(name).Set(float64(status.DLQMessages))
	}
}
