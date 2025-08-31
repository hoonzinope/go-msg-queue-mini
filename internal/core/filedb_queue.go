package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/util"
	"sync"
	"time"
)

type fileDBQueue struct {
	manager       *FileDBManager
	mu            sync.Mutex
	groupLock     map[string]*sync.Mutex
	maxRetry      int
	retryInterval time.Duration
	leaseDuration time.Duration
}

func NewFileDBQueue(config *internal.Config) (*fileDBQueue, error) {
	dbpath := config.Persistence.Options.DirsPath + "/filedb_queue.db"
	dbpath = fmt.Sprintf("file:%s?_txlock=immediate&_busy_timeout=5000&_journal=WAL&_sync=NORMAL", dbpath)
	if config.Persistence.Type == "memory" {
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

	manager, err := NewFileDBManager(dbpath)
	if err != nil {
		return nil, err
	}
	fq := &fileDBQueue{
		manager:       manager,
		groupLock:     map[string]*sync.Mutex{},
		maxRetry:      config.MaxRetry,
		retryInterval: retryInterval,
		leaseDuration: leaseDuration,
	}
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

func (q *fileDBQueue) Enqueue(item interface{}) error {
	msg, err := json.Marshal(item)
	if err != nil {
		return err
	}
	if err := q.manager.WriteMessage(msg); err != nil {
		fmt.Println("Error writing message to queue:", err)
		return err
	}
	return nil
}

func (q *fileDBQueue) Dequeue(consumer_group string, consumer_id string) (internal.QueueMessage, error) {
	unLock := q.Lock(consumer_group)
	defer unLock()

	queueMessage := internal.QueueMessage{
		Payload: nil,
		ID:      -1,
		Receipt: "",
	}
	msg, err := q.manager.ReadMessage(consumer_group, consumer_id, int(q.leaseDuration.Seconds()))
	if err != nil {
		switch err {
		case ErrEmpty:
			return queueMessage, ErrEmpty
		case ErrContended:
			return queueMessage, ErrContended
		default:
			return queueMessage, fmt.Errorf("unexpected error: %w", err)
		}
	}

	// 매니저가 아직 (queueMsg{}, nil)로 빈 큐를 표현한다면 방어
	if msg.ID == 0 || len(msg.Msg) == 0 {
		return queueMessage, ErrEmpty
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
	return queueMessage, nil
}

func (q *fileDBQueue) Ack(consumer_group string, msg_id int64, receipt string) error {
	if err := q.manager.AckMessage(consumer_group, msg_id, receipt); err != nil {
		util.Error(fmt.Sprintf("Error acknowledging message: %v", err))
		return err
	}
	return nil
}

func (q *fileDBQueue) Nack(consumer_group string, msg_id int64, receipt string) error {
	maxRetry := q.maxRetry
	retryInterval := q.retryInterval
	if err := q.manager.NackMessage(consumer_group, msg_id, receipt, retryInterval, maxRetry, "Nack Message"); err != nil {
		util.Error(fmt.Sprintf("Error not acknowledging message: %v", err))
		return err
	}
	return nil
}

func (q *fileDBQueue) Shutdown() error {
	if err := q.manager.Close(); err != nil {
		util.Error(fmt.Sprintf("Error shutting down queue: %v", err))
		return err
	}
	return nil
}

func (q *fileDBQueue) Status() (internal.QueueStatus, error) {
	return q.manager.GetStatus()
}

// bellow new api for http, gRPC
func (q *fileDBQueue) Peek(group_name string) (internal.QueueMessage, error) {
	msg, err := q.manager.PeekMessage(group_name)
	if err != nil {
		if errors.Is(err, ErrEmpty) {
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

func (q *fileDBQueue) Renew(group_name string, messageID int64, receipt string, extendSec int) error {
	return q.manager.RenewMessage(group_name, messageID, receipt, extendSec)
}
