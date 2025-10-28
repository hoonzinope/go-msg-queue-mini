package core

import (
	"fmt"
	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/internal/metrics"
	"go-msg-queue-mini/internal/queue_error"
	"go-msg-queue-mini/util"
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

func (q *fileDBQueue) Enqueue(queue_name string, enqMsg internal.EnqueueMessage) error {
	item := enqMsg.Item
	delay := enqMsg.Delay
	deduplicationID := enqMsg.DeduplicationID

	msg := item
	gid := util.GenerateGlobalID()
	pid := 0
	if err := q.manager.WriteMessage(queue_name, msg, gid, pid, delay, deduplicationID); err != nil {
		q.logger.Error("Error writing message to queue", "error", err)
		return err
	}
	q.MQMetrics.EnqueueCounter.WithLabelValues(queue_name).Inc()
	return nil
}

func (q *fileDBQueue) _enqueueBatchStopOnFailure(queue_name string, items [][]byte, delays []string, deduplicationIDs []string) (int64, error) {
	var successCount int64 = 0
	msgs := make([][]byte, 0, len(items))
	for _, item := range items {
		msg := item
		msgs = append(msgs, msg)
	}
	pid := 0
	successCount, err := q.manager.WriteMessagesBatchWithMeta(queue_name, msgs, pid, delays, deduplicationIDs)
	if err != nil {
		q.logger.Error("Error writing batch messages to queue", "error", err)
		return successCount, err
	}
	q.MQMetrics.EnqueueCounter.WithLabelValues(queue_name).Add(float64(successCount))
	return successCount, nil
}

func (q *fileDBQueue) _enqueueBatchPartialSuccess(queue_name string, items [][]byte, startIndex int64, delays []string, deduplicationIDs []string) (int64, []internal.FailedMessage, error) {
	var successCount int64 = 0
	var failedMessages []internal.FailedMessage
	msgs := make([][]byte, 0, len(items))
	for _, item := range items {
		msg := item
		msgs = append(msgs, msg)
	}
	pid := 0
	successCount, insertFailedMessages, err := q.manager.WriteMessagesBatchWithMetaAndReturnFailed(queue_name, msgs, pid, delays, deduplicationIDs)
	if err != nil {
		q.logger.Error("Error writing batch messages to queue", "error", err)
	}
	// Mark insertFailedMessages as failed
	for _, ifm := range insertFailedMessages {
		failedMessages = append(failedMessages, internal.FailedMessage{
			Index:   ifm.Index + startIndex,
			Message: items[ifm.Index],
			Reason:  ifm.Reason,
		})
	}
	q.MQMetrics.EnqueueCounter.WithLabelValues(queue_name).Add(float64(successCount))
	return successCount, failedMessages, err
}

func (q *fileDBQueue) EnqueueBatch(queue_name, mode string, enqMsg []internal.EnqueueMessage) (internal.BatchResult, error) {
	items := make([][]byte, 0, len(enqMsg))
	delays := make([]string, 0, len(enqMsg))
	deduplicationIDs := make([]string, 0, len(enqMsg))
	for _, em := range enqMsg {
		items = append(items, em.Item)
		delays = append(delays, em.Delay)
		deduplicationIDs = append(deduplicationIDs, em.DeduplicationID)
	}

	chunkedMsgs := util.ChunkSlice(items, 100) // Chunk size of 100
	chunkedDelays := util.ChunkSlice(delays, 100)
	chunkedDedupIDs := util.ChunkSlice(deduplicationIDs, 100)
	var totalSuccess int64 = 0
	var stopError error = nil
	var failedMessages []internal.FailedMessage
	switch mode {
	case "stopOnFailure":
		for i, chunk := range chunkedMsgs {
			successCount, err := q._enqueueBatchStopOnFailure(queue_name, chunk, chunkedDelays[i], chunkedDedupIDs[i])
			if err != nil {
				q.logger.Error("Error enqueuing messages", "error", err)
				return internal.BatchResult{
					SuccessCount:   totalSuccess,
					FailedCount:    int64(len(items)) - totalSuccess,
					FailedMessages: failedMessages,
				}, err
			}
			// If some messages in the chunk failed, stop processing further
			totalSuccess += successCount
			stopError = err
			if successCount < int64(len(chunk)) {
				break
			}
		}
		return internal.BatchResult{
			SuccessCount:   totalSuccess,
			FailedCount:    int64(len(items)) - totalSuccess,
			FailedMessages: failedMessages,
		}, stopError
	case "partialSuccess":
		returnFailedMessages := []internal.FailedMessage{}
		currentIndex := int64(0)
		// Track the current index across chunks
		for i, chunk := range chunkedMsgs {
			successCount, failedMessages, err := q._enqueueBatchPartialSuccess(queue_name, chunk, currentIndex, chunkedDelays[i], chunkedDedupIDs[i])
			if err != nil {
				q.logger.Error("Error enqueuing messages", "error", err)
			} else {
				totalSuccess += successCount
			}
			returnFailedMessages = append(returnFailedMessages, failedMessages...)
			currentIndex += int64(len(chunk))
		}
		return internal.BatchResult{
			SuccessCount:   totalSuccess,
			FailedCount:    int64(len(items)) - totalSuccess,
			FailedMessages: returnFailedMessages,
		}, nil
	default:
		return internal.BatchResult{}, fmt.Errorf("invalid mode: %s", mode)
	}
}

func (q *fileDBQueue) Dequeue(queue_name, consumer_group string, consumer_id string) (internal.QueueMessage, error) {
	unLock := q.Lock(queue_name, consumer_group)
	defer unLock()

	queueMessage := internal.QueueMessage{
		Payload: nil,
		ID:      -1,
		Receipt: "",
	}
	partitionID := 0
	msg, err := q.manager.ReadMessage(queue_name, consumer_group, partitionID, consumer_id, int(q.leaseDuration.Seconds()))
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

	// if item empty
	payload := msg.Msg
	if payload == nil {
		return queueMessage, fmt.Errorf("no message available")
	}

	queueMessage.Payload = payload
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

// bellow new api for http, gRPC
func (q *fileDBQueue) Peek(queue_name, group_name string, options internal.PeekOptions) ([]internal.PeekMessage, error) {
	partitionID := 0
	msgs, err := q.manager.PeekMessages(queue_name, group_name, partitionID, options)
	if err != nil {
		return nil, err
	}
	var items []internal.PeekMessage
	for _, msg := range msgs {
		items = append(items, internal.PeekMessage{
			Payload:    msg.Msg,
			ID:         msg.ID,
			Receipt:    msg.Receipt,
			InsertedAt: msg.InsertTS,
		})
	}
	return items, nil
}

func (q *fileDBQueue) Detail(queue_name string, messageId int64) (internal.PeekMessage, error) {
	msg, err := q.manager.GetMessageDetail(queue_name, messageId)
	if err != nil {
		return internal.PeekMessage{}, err
	}
	return internal.PeekMessage{
		Payload:    msg.Msg,
		ID:         msg.ID,
		Receipt:    msg.Receipt,
		InsertedAt: msg.InsertTS,
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

// implements QueueInspector interface
func (q *fileDBQueue) Status(queue_name string) (internal.QueueStatus, error) {
	return q.manager.GetStatus(queue_name)
}

func (q *fileDBQueue) StatusAll() (map[string]internal.QueueStatus, error) {
	statuses, err := q.manager.GetAllStatus()
	if err != nil {
		return nil, err
	}
	statusMap := make(map[string]internal.QueueStatus)
	for _, status := range statuses {
		statusMap[status.QueueName] = status
	}
	return statusMap, nil
}
