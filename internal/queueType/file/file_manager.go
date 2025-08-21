package file

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"go-msg-queue-mini/internal"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

type fileManager struct {

	// path to file queue
	dirs string

	// the name of the file where information are stored
	eventLog                 string // wal event log
	snapshotLSN              string // event -> snapshot LSN
	offsetState              string // event -> offset state
	retryState               string // event -> retry state
	segmentFileMetadataState string // event -> segment file metadata
	deadLetterQueueLog       string // event -> dead-letter queue

	// file handles for the logs
	eventLogFile *os.File

	queue                  []internal.Msg           // Queue to store messages
	deadLetterQueue        []internal.Msg           // Dead-letter queue for failed messages
	offsetMap              map[string]int64         // consumerID -> offset
	retryMap               map[string]map[int64]int // consumerID -> messageID -> retry count
	segmentFileMetadataMap map[string]int64         // segment file name -> last message ID

	maxSize    int        // Maximum size of the file in MB
	maxAge     int        // Maximum age of the file in days
	appliedLSN int64      // Last applied LSN for the file queue
	LSN        int64      // Last sequence number for the file queue
	mutex      sync.Mutex // Mutex for thread-safe operations

	// Map to track message ID to index in the queue
	msgIdToIdx map[int64]int
}

type event struct {
	Lsn  int64
	Ts   int64
	Type string // Type of event (e.g., "ACK", "NACK", "DLQ" etc.)
	Data interface{}
}

type nack struct {
	ConsumerID string
	MessageID  int64
	RetryCount int
}

type ack struct {
	ConsumerID string
	MessageID  int64
}

func NewFileManager(logDir string, maxSizeMB int, maxAgeDays int) (*fileManager, error) {
	if logDir == "" {
		return nil, fmt.Errorf("log directory path is empty")
	}

	fm := &fileManager{
		dirs:                   logDir,
		maxSize:                maxSizeMB,
		maxAge:                 maxAgeDays,
		queue:                  make([]internal.Msg, 0),
		deadLetterQueue:        make([]internal.Msg, 0),
		offsetMap:              make(map[string]int64),
		retryMap:               make(map[string]map[int64]int),
		segmentFileMetadataMap: make(map[string]int64),
		msgIdToIdx:             make(map[int64]int),
		mutex:                  sync.Mutex{},
	}

	return fm, nil
}

func (f *fileManager) OpenFiles() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	f.LSN = 0
	f.offsetMap = make(map[string]int64)
	f.retryMap = make(map[string]map[int64]int)
	f.segmentFileMetadataMap = make(map[string]int64)
	f.queue = make([]internal.Msg, 0)
	f.msgIdToIdx = make(map[int64]int) // Initialize the message ID to index map

	if err := f.openSnapshotFile(); err != nil {
		return err
	}
	if f.LSN > 0 {
		if err := f.openOffsetFile(); err != nil {
			return err
		}
		if err := f.openSegmentFileMetadata(); err != nil {
			return err
		}
		if err := f.openRetryFile(); err != nil {
			return err
		}
		if err := f.openDeadLetterQueueLog(); err != nil {
			return err
		}
		if err := f.replayEventLog(); err != nil {
			return err
		}
	} else {
		f.offsetState = filepath.Join(f.dirs, "offset.state")
		f.retryState = filepath.Join(f.dirs, "retry.state")
		f.segmentFileMetadataState = filepath.Join(f.dirs, "segment_file_metadata.state")
		f.deadLetterQueueLog = filepath.Join(f.dirs, "dead_letter_queue.log")
	}

	if err := f.openEventLog(); err != nil {
		return err
	}

	return nil
}

func (f *fileManager) CloseFiles() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	f.saveEventToMetadata()

	if f.eventLogFile != nil {
		if err := f.eventLogFile.Close(); err != nil {
			return fmt.Errorf("error closing event log file: %v", err)
		}
	}

	return nil
}

func (f *fileManager) openFileAndParse(filePath string, parseFunc func(*os.File) error) error {
	file, err := os.OpenFile(filePath, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("error opening file %s: %v", filePath, err)
	}
	defer file.Close()

	if err := parseFunc(file); err != nil {
		return fmt.Errorf("error parsing file %s: %v", filePath, err)
	}

	return nil
}

func (f *fileManager) openSnapshotFile() error {
	f.snapshotLSN = filepath.Join(f.dirs, "snapshot.lsn")

	parseFunc := func(file *os.File) error {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var lsn int64
			if _, err := fmt.Sscanf(scanner.Text(), "%d", &lsn); err != nil {
				return fmt.Errorf("error parsing snapshot LSN file: %v", err)
			}
			f.appliedLSN = lsn // Update the last applied LSN
			f.LSN = lsn        // Update the last sequence number
		}
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("error reading snapshot LSN file: %v", err)
		}
		return nil
	}
	f.openFileAndParse(f.snapshotLSN, parseFunc)
	return nil
}

func (f *fileManager) openOffsetFile() error {
	f.offsetState = filepath.Join(f.dirs, "offset.state")

	parseFunc := func(file *os.File) error {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var consumerID string
			var offset int64
			if _, err := fmt.Sscanf(scanner.Text(), "%s %d", &consumerID, &offset); err != nil {
				return fmt.Errorf("error parsing offset state file: %v", err)
			}
			f.offsetMap[consumerID] = offset // Update the offset for the consumer

		}
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("error reading offset state file: %v", err)
		}
		return nil
	}
	f.openFileAndParse(f.offsetState, parseFunc)
	return nil
}

func (f *fileManager) openRetryFile() error {
	f.retryState = filepath.Join(f.dirs, "retry.state")

	parseFunc := func(file *os.File) error {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var consumerID string
			var messageID int64
			var messageIDStr string
			var retryCount int
			if _, err := fmt.Sscanf(scanner.Text(), "%s %s %d", &consumerID, &messageIDStr, &retryCount); err != nil {
				return fmt.Errorf("error parsing retry state file: %v", err)
			}
			messageID, err := strconv.ParseInt(messageIDStr, 10, 64)
			if err != nil {
				return fmt.Errorf("error converting messageID to int64: %v", err)
			}
			if _, ok := f.retryMap[consumerID]; !ok {
				f.retryMap[consumerID] = make(map[int64]int)
			}
			f.retryMap[consumerID][messageID] = retryCount // Update the retry count for the message
		}
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("error reading retry state file: %v", err)
		}
		return nil
	}
	f.openFileAndParse(f.retryState, parseFunc)

	return nil
}

func (f *fileManager) openSegmentFileMetadata() error {
	f.segmentFileMetadataState = filepath.Join(f.dirs, "segment_file_metadata.state")

	parseFunc := func(file *os.File) error {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var order int
			var segmentName string
			var endLSN int64
			if _, err := fmt.Sscanf(scanner.Text(), "%d %s %d", &order, &segmentName, &endLSN); err != nil {
				return fmt.Errorf("error parsing segment file metadata state file: %v", err)
			}
			f.segmentFileMetadataMap[segmentName] = endLSN
		}
		return nil
	}
	if err := f.openFileAndParse(f.segmentFileMetadataState, parseFunc); err != nil {
		return fmt.Errorf("error opening segment file metadata state file: %v", err)
	}

	for segmentName, lsn := range f.segmentFileMetadataMap {
		if lsn > f.appliedLSN {
			// file loading & add msg to queue
			segmentFilePath := filepath.Join(f.dirs, segmentName)
			parseFunc := func(file *os.File) error {
				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					var msg internal.Msg
					if err := json.Unmarshal([]byte(scanner.Text()), &msg); err != nil {
						return fmt.Errorf("error unmarshalling message from segment file %s: %v", segmentFilePath, err)
					}
					f.queue = append(f.queue, msg)          // Add message to the queue
					f.msgIdToIdx[msg.Id] = len(f.queue) - 1 // Update message ID to index map
				}
				return nil
			}
			if err := f.openFileAndParse(segmentFilePath, parseFunc); err != nil {
				return fmt.Errorf("error parsing segment file %s: %v", segmentFilePath, err)
			}
		}
	}

	return nil
}

func (f *fileManager) openDeadLetterQueueLog() error {
	f.deadLetterQueueLog = filepath.Join(f.dirs, "dead_letter_queue.log")
	parseFunc := func(file *os.File) error {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var msg internal.Msg
			if err := json.Unmarshal([]byte(scanner.Text()), &msg); err != nil {
				return fmt.Errorf("error unmarshalling message from dead-letter queue log file: %v", err)
			}
			f.deadLetterQueue = append(f.deadLetterQueue, msg) // Add message to the dead-letter queue
		}
		return nil
	}
	if err := f.openFileAndParse(f.deadLetterQueueLog, parseFunc); err != nil {
		return fmt.Errorf("error parsing dead-letter queue log file: %v", err)
	}
	return nil
}

func (f *fileManager) replayEventLog() error {
	p := filepath.Join(f.dirs, "event.log")
	parseFunc := func(file *os.File) error {
		dec := json.NewDecoder(file)
		for {
			var ev event
			if err := dec.Decode(&ev); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return fmt.Errorf("decode event.log: %w", err)
			}
			if ev.Lsn <= f.appliedLSN {
				continue
			} // Skip events that are already applied
			f.applyEvent(ev) // Apply the event to the queue
			if ev.Lsn > f.LSN {
				f.LSN = ev.Lsn // Update the last sequence number
			}
		}
		return nil
	}
	if err := f.openFileAndParse(p, parseFunc); err != nil {
		return fmt.Errorf("error parsing event log file: %v", err)
	}
	return nil
}

func (f *fileManager) applyEvent(ev event) {
	switch ev.Type {
	case "ENQUEUE":
		var msg internal.Msg
		if err := remarshal(ev.Data, &msg); err != nil {
			fmt.Printf("Error remarshalling ENQUEUE event data: %v\n", err)
			return
		}
		f.queue = append(f.queue, msg)          // Add message to the queue
		f.msgIdToIdx[msg.Id] = len(f.queue) - 1 // Update message ID to index map

	case "ACK":
		ackData := ack{}
		if err := remarshal(ev.Data, &ackData); err != nil {
			fmt.Printf("Error remarshalling ACK event data: %v\n", err)
			return
		}
		consumerID := ackData.ConsumerID
		messageID := ackData.MessageID
		f.offsetMap[consumerID] = messageID // Update offset for the consumer

	case "NACK":
		fmt.Println("Processing NACK event")
		nackdata := nack{}
		if err := remarshal(ev.Data, &nackdata); err != nil {
			fmt.Printf("Error remarshalling NACK event data: %v\n", err)
			return
		}

		consumerID := nackdata.ConsumerID
		messageID := nackdata.MessageID
		retryCount := nackdata.RetryCount
		if _, ok := f.retryMap[consumerID]; !ok {
			f.retryMap[consumerID] = make(map[int64]int)
		}
		f.retryMap[consumerID][messageID] = retryCount // Update retry count for the message

	case "DLQ":
		var msg internal.Msg
		if err := remarshal(ev.Data, &msg); err != nil {
			fmt.Printf("Error remarshalling dead-letter queue message data: %v\n", err)
			return
		}
		f.deadLetterQueue = append(f.deadLetterQueue, msg) // Add message to the dead-letter queue
	default:
		fmt.Printf("Unknown event type: %s\n", ev.Type)
	}
}

func (f *fileManager) openEventLog() error {
	p := filepath.Join(f.dirs, "event.log")
	f.eventLog = p
	af, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	f.eventLogFile = af
	return nil
}

func (f *fileManager) WriteMsg(msg internal.Msg) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	// Increment the last sequence number
	f.LSN++

	// Write the event to the event log file
	event := event{
		Lsn:  f.LSN,
		Ts:   time.Now().UnixNano(),
		Type: "ENQUEUE",
		Data: msg,
	}
	if err := f.writeEventToLog(event); err != nil {
		return fmt.Errorf("error writing event to log: %v", err)
	}
	f.msgIdToIdx[msg.Id] = len(f.queue) // Map message ID to index in the queue
	f.queue = append(f.queue, msg)      // Add the message to the queue
	return nil
}

func (f *fileManager) WriteOffset(consumerID string, messageID int64) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if consumerID == "" {
		return fmt.Errorf("consumer ID is empty")
	}
	if messageID == 0 {
		return fmt.Errorf("message ID is zero")
	}
	ackEvent := ack{
		ConsumerID: consumerID,
		MessageID:  messageID,
	}
	f.LSN++
	event := event{
		Lsn:  f.LSN,
		Ts:   time.Now().UnixNano(),
		Type: "ACK",
		Data: ackEvent,
	}
	if err := f.writeEventToLog(event); err != nil {
		return fmt.Errorf("error writing event to log: %v", err)
	}

	if f.offsetMap[consumerID] < messageID { // Update the offset for the consumer
		f.offsetMap[consumerID] = messageID
	}
	if f.retryMap[consumerID] != nil {
		delete(f.retryMap[consumerID], messageID) // Remove from retry map
	}
	return nil
}

func (f *fileManager) WriteRetry(consumerID string, messageID int64, MaxRetry int) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if consumerID == "" {
		return fmt.Errorf("consumer ID is empty")
	}
	if messageID == 0 {
		return fmt.Errorf("message ID is zero")
	}

	if _, ok := f.retryMap[consumerID]; !ok {
		f.retryMap[consumerID] = make(map[int64]int)
	}
	retryCount := f.retryMap[consumerID][messageID] + 1 // Increment retry count
	nackEvent := nack{
		ConsumerID: consumerID,
		MessageID:  messageID,
		RetryCount: retryCount,
	}
	f.LSN++
	var ev = event{
		Lsn:  f.LSN,
		Ts:   time.Now().UnixNano(),
		Type: "NACK",
		Data: nackEvent,
	}
	if err := f.writeEventToLog(ev); err != nil {
		return fmt.Errorf("error writing event to log: %v", err)
	}
	f.retryMap[consumerID][messageID] = retryCount // Update the retry count

	if retryCount >= MaxRetry {
		var msg internal.Msg = f.queue[f.msgIdToIdx[messageID]] // Get the message from the queue
		f.LSN++
		var ev = event{
			Lsn:  f.LSN,
			Ts:   time.Now().UnixNano(),
			Type: "DLQ",
			Data: msg,
		}
		if err := f.writeEventToLog(ev); err != nil {
			return fmt.Errorf("error writing event to log: %v", err)
		}
		f.offsetMap[consumerID] = messageID       // Update offset for the consumer
		delete(f.retryMap[consumerID], messageID) // Remove from retry map
		if len(f.retryMap[consumerID]) == 0 {
			delete(f.retryMap, consumerID) // Clean up empty retry map
		}
		f.deadLetterQueue = append(f.deadLetterQueue, msg) // Add message to the dead-letter queue
	}

	return nil
}

func (f *fileManager) writeEventToLog(event event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("error marshalling event: %v", err)
	}
	if _, err := f.eventLogFile.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("error writing to event log file: %v", err)
	}

	return f.eventLogFile.Sync()
}

func (f *fileManager) ReadMsg(consumerID string, maxCount int) ([]internal.Msg, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if consumerID == "" {
		return nil, fmt.Errorf("consumer ID is empty")
	}
	if maxCount <= 0 {
		return nil, fmt.Errorf("max count must be greater than 0")
	}

	var messages []internal.Msg
	for _, msg := range f.queue {
		if msg.Id > f.offsetMap[consumerID] && len(messages) < maxCount {
			messages = append(messages, msg)
		}
	}
	if len(messages) == 0 {
		return nil, fmt.Errorf("no new messages for consumer %s", consumerID)
	}

	return messages, nil
}

// 유틸
func remarshal(src any, dst any) error {
	b, err := json.Marshal(src)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dst)
}
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func atomicWrite(path string, write func(*os.File) error) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if err := write(tmp); err != nil {
		tmp.Close()
		os.Remove(name)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(name)
		return err
	}
	if err := os.Rename(name, path); err != nil {
		os.Remove(name)
		return err
	}
	return fsyncDir(dir)
}

func (f *fileManager) saveEventToMetadata() error {
	// 0) 스냅샷 경계 고정
	S := f.LSN

	// 1) event.log 새 이벤트 스캔(읽기 핸들 별도)
	rf, err := os.OpenFile(f.eventLog, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("open event.log: %w", err)
	}
	defer rf.Close()

	// 임시 상태 = "현재 메모리 상태"에서 시작해 증분 적용
	tempOffset := make(map[string]int64, len(f.offsetMap))
	for k, v := range f.offsetMap {
		tempOffset[k] = v
	}
	tempRetry := make(map[string]map[int64]int, len(f.retryMap))
	for c, m := range f.retryMap {
		mm := make(map[int64]int, len(m))
		for id, rc := range m {
			mm[id] = rc
		}
		tempRetry[c] = mm
	}
	tempDLQ := make(map[int64]internal.Msg, len(f.deadLetterQueue))
	for _, msg := range f.deadLetterQueue {
		tempDLQ[msg.Id] = msg
	}

	// 이번 스냅샷에서 새로 추가될 ENQUEUE만 수집(이전 스냅샷 분은 기존 segment 스냅샷 파일에 이미 있음)
	newEnq := make([]internal.Msg, 0, 1024)

	dec := json.NewDecoder(rf)
	for {
		var ev event
		if err := dec.Decode(&ev); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("decode event.log: %w", err)
		}
		if ev.Lsn <= f.appliedLSN { // 이전 스냅샷 포함분은 건너뛰
			continue
		}
		switch ev.Type {
		case "ENQUEUE":
			var m internal.Msg
			if err := remarshal(ev.Data, &m); err != nil {
				return fmt.Errorf("ev ENQUEUE data: %w", err)
			}
			newEnq = append(newEnq, m)
		case "ACK":
			var p struct {
				ConsumerID string `json:"consumerID"`
				MessageID  int64  `json:"messageID"`
			}
			if err := remarshal(ev.Data, &p); err != nil {
				return fmt.Errorf("ev ACK data: %w", err)
			}
			if tempOffset[p.ConsumerID] < p.MessageID {
				tempOffset[p.ConsumerID] = p.MessageID
			}
		case "NACK": // 현재 설계 유지 시
			var p struct {
				ConsumerID string
				MessageID  int64
				RetryCount int
			}
			if err := remarshal(ev.Data, &p); err != nil {
				return fmt.Errorf("ev NACK data: %w", err)
			}
			mm := tempRetry[p.ConsumerID]
			if mm == nil {
				mm = map[int64]int{}
				tempRetry[p.ConsumerID] = mm
			}
			mm[p.MessageID] = p.RetryCount
		case "DLQ":
			var m internal.Msg
			if err := remarshal(ev.Data, &m); err != nil {
				return fmt.Errorf("ev DLQ data: %w", err)
			}
			// DLQ 메시지 추가
			if _, exists := tempDLQ[m.Id]; !exists {
				tempDLQ[m.Id] = m // Add to DLQ if not already present
			}
		}
	}

	// 2) 최종 cut_id 계산
	cutID := int64(0)
	first := true
	for _, off := range tempOffset {
		if first || off < cutID {
			cutID = off
			first = false
		}
	}

	// cutId 이전 message를 queue에서 제거 & msgIdToIdx 갱신
	for i := 0; i < len(f.queue); i++ {
		if f.queue[i].Id <= cutID {
			continue
		}
		// cutID 이후 메시지만 남김
		f.queue = f.queue[i:]
		break
	}
	// msgIdToIdx 갱신
	f.msgIdToIdx = make(map[int64]int, len(f.queue))
	for i, msg := range f.queue {
		f.msgIdToIdx[msg.Id] = i
	}

	// 3) segment 스냅샷 파일 작성: (cutID < id ≤ S) && !DLQ 에 해당하는 **이번 증분 ENQUEUE만**
	// (과거 분은 기존 segment 스냅샷 파일들에 이미 있음)
	// 필요시 크기 기준 분할 구현. 여기선 단일 파일 예시.
	messageToWrite := []internal.Msg{}
	for _, m := range newEnq {
		if m.Id <= cutID {
			continue
		}
		if _, dead := tempDLQ[m.Id]; dead {
			continue
		}
		messageToWrite = append(messageToWrite, m)

	}
	if len(messageToWrite) > 0 {
		segName := fmt.Sprintf("segment_%d.snap", S)
		f.segmentFileMetadataMap[segName] = S
		if err := atomicWrite(filepath.Join(f.dirs, segName), func(f *os.File) error {
			w := bufio.NewWriter(f)
			for _, m := range messageToWrite {
				b, _ := json.Marshal(m)
				if _, err := w.Write(append(b, '\n')); err != nil {
					return err
				}
			}
			return w.Flush()
		}); err != nil {
			return fmt.Errorf("write segment: %w", err)
		}
	}

	// 4) segment 메타 갱신(전체 목록 재기록 권장)
	// 기존 맵에 새 파일 등록
	// 정렬 출력 + 헤더
	if err := atomicWrite(f.segmentFileMetadataState, func(file *os.File) error {
		keys := make([]string, 0, len(f.segmentFileMetadataMap))
		for k := range f.segmentFileMetadataMap {
			keys = append(keys, k)
		}
		// sort keys by value (end LSN)
		sort.Slice(keys, func(i, j int) bool {
			return f.segmentFileMetadataMap[keys[i]] < f.segmentFileMetadataMap[keys[j]]
		})
		bw := bufio.NewWriter(file)
		for i, k := range keys {
			if _, err := fmt.Fprintf(bw, "%d %s %d\n", i+1, k, f.segmentFileMetadataMap[k]); err != nil {
				return err
			}
		}
		return bw.Flush()
	}); err != nil {
		return fmt.Errorf("write segment meta: %w", err)
	}

	// 5) 오프셋/리트라이/DLQ 전체 스냅샷 재기록
	if err := atomicWrite(f.offsetState, func(file *os.File) error {
		bw := bufio.NewWriter(file)
		// consumerID 정렬
		ids := make([]string, 0, len(tempOffset))
		for id := range tempOffset {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			if _, err := fmt.Fprintf(bw, "%s %d\n", id, tempOffset[id]); err != nil {
				return err
			}
		}
		return bw.Flush()
	}); err != nil {
		return fmt.Errorf("write offset: %w", err)
	}

	if err := atomicWrite(f.retryState, func(file *os.File) error {
		bw := bufio.NewWriter(file)
		ids := make([]string, 0, len(tempRetry))
		for id := range tempRetry {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			// messageID 정렬
			mids := make([]int64, 0, len(tempRetry[id]))
			for mid := range tempRetry[id] {
				mids = append(mids, mid)
			}
			sort.Slice(mids, func(i, j int) bool { return mids[i] < mids[j] })
			for _, mid := range mids {
				if _, err := fmt.Fprintf(bw, "%s %d %d\n", id, mid, tempRetry[id][mid]); err != nil {
					return err
				}
			}
		}
		return bw.Flush()
	}); err != nil {
		return fmt.Errorf("write retry: %w", err)
	}

	if err := atomicWrite(f.deadLetterQueueLog, func(file *os.File) error {
		bw := bufio.NewWriter(file)
		// DLQ id 정렬(선택)
		ids := make([]int64, 0, len(tempDLQ))
		for id := range tempDLQ {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		for _, id := range ids {
			// DLQ 메시지 본문 저장
			m := tempDLQ[id]
			b, _ := json.Marshal(m)
			if _, err := bw.Write(append(b, '\n')); err != nil {
				return err
			}
		}
		return bw.Flush()
	}); err != nil {
		return fmt.Errorf("write dlq: %w", err)
	}

	// 6) snapshot.lsn 커밋 (원자 포인터)
	if err := f.saveSnapshotLSN(S); err != nil {
		return fmt.Errorf("save snapshot lsn: %w", err)
	}

	// 7) WAL truncate/rotate (별도 함수로 실행)
	if err := f.truncateEventLog(S); err != nil {
		return fmt.Errorf("truncate wal: %w", err)
	}
	fmt.Println("segment file metadata map:", f.segmentFileMetadataMap)
	// 8) 메모리 경계 업데이트
	f.appliedLSN = S
	return nil
}

func (f *fileManager) saveSnapshotLSN(S int64) error {
	// 6) snapshot.lsn 커밋 (원자 포인터)
	if err := atomicWrite(f.snapshotLSN, func(file *os.File) error {
		_, err := fmt.Fprintf(file, "%d\n", S)
		return err
	}); err != nil {
		return fmt.Errorf("write snapshot.lsn: %w", err)
	}
	return nil
}

func (f *fileManager) truncateEventLog(S int64) error {
	// append 핸들 닫기
	if f.eventLogFile != nil {
		if err := f.eventLogFile.Sync(); err != nil {
			return err
		}
		if err := f.eventLogFile.Close(); err != nil {
			return err
		}
		f.eventLogFile = nil
	}
	// tail만 새 파일로 복사
	oldPath := f.eventLog
	newPath := filepath.Join(f.dirs, "event.log.new")
	rf, err := os.OpenFile(oldPath, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(rf)

	wf, err := os.OpenFile(newPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		rf.Close()
		return err
	}
	bw := bufio.NewWriter(wf)

	for {
		var ev event
		if err := dec.Decode(&ev); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			rf.Close()
			wf.Close()
			os.Remove(newPath)
			return err
		}
		if ev.Lsn <= S {
			continue
		}
		b, _ := json.Marshal(ev)
		if _, err := bw.Write(append(b, '\n')); err != nil {
			rf.Close()
			wf.Close()
			os.Remove(newPath)
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		rf.Close()
		wf.Close()
		os.Remove(newPath)
		return err
	}
	if err := wf.Sync(); err != nil {
		rf.Close()
		wf.Close()
		os.Remove(newPath)
		return err
	}
	if err := wf.Close(); err != nil {
		rf.Close()
		os.Remove(newPath)
		return err
	}
	if err := rf.Close(); err != nil {
		os.Remove(newPath)
		return err
	}

	// 교체 + 디렉터리 fsync
	if err := os.Rename(newPath, oldPath); err != nil {
		os.Remove(newPath)
		return err
	}
	if err := fsyncDir(f.dirs); err != nil {
		return err
	}

	// append 핸들 재오픈
	af, err := os.OpenFile(oldPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	f.eventLogFile = af
	return nil
}

func (f *fileManager) GetStatus() (internal.QueueStatus, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	var consumerStatuses = make(map[string]internal.ConsumerStatus)
	for consumerID, offset := range f.offsetMap {
		lag := int64(0) // Calculate lag based on the latest message ID and consumer offset
		queueLen := len(f.queue)
		if queueLen > 0 {
			current_index := f.msgIdToIdx[offset]
			if current_index < queueLen {
				lag = f.queue[queueLen-1].Id - f.queue[current_index].Id
			}
		}
		consumerStatuses[consumerID] = internal.ConsumerStatus{
			ConsumerID: consumerID,
			LastOffset: offset,
			Lag:        lag,
		}
	}

	totalSegmentFiles := len(f.segmentFileMetadataMap) + 1 // +1 for the append log file
	extraInfo := map[string]interface{}{
		"TotalSegmentFiles": totalSegmentFiles,
		"LatestLSN":         f.LSN,
		"AppliedLSN":        f.appliedLSN,
	}

	status := internal.QueueStatus{
		QueueType:        "file",
		ActiveConsumers:  len(f.offsetMap),
		ExtraInfo:        extraInfo,
		ConsumerStatuses: consumerStatuses,
	}

	return status, nil
}
