package file

import (
	"bufio"
	"encoding/json"
	"fmt"
	"go-msg-queue-mini/internal"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

var dirs string // Directory where log files are stored

var segmentPath string
var segmentFile *os.File

// Maps consumer ID to the last acknowledged message ID
var offsetLogPath string
var offsetMap map[string]int64
var offsetFile *os.File

// Maps segment file names to their last message ID
var segmentMetadataPath string
var segmentMetadata *os.File
var segmentFileMetadataMap map[string]int64

var maxSize int // Maximum size of the segment file in MB
var maxAge int  // Maximum age of the segment file in days

var mutex = &sync.Mutex{} // Mutex to protect concurrent access

var latestMessageID int64 // Track the latest message ID

func OpenFiles(logdirs string, maxSizeMB int, maxAgeDays int) error {
	mutex.Lock()
	defer mutex.Unlock()
	if logdirs == "" {
		return fmt.Errorf("log directory path is empty")
	}
	if _, err := os.Stat(logdirs); os.IsNotExist(err) {
		return fmt.Errorf("log directory does not exist: %s", logdirs)
	}
	dirs = logdirs
	maxSize = maxSizeMB
	maxAge = maxAgeDays

	segmentPath = filepath.Join(dirs, "segments.log")
	var err error
	segmentFile, err = os.OpenFile(segmentPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening segment file: %v", err)
	}

	offsetLogPath = filepath.Join(dirs, "offset.state")
	offsetMap = make(map[string]int64)
	if _, err := os.Stat(offsetLogPath); os.IsNotExist(err) {
		// create offset log file if it does not exist
		offsetFile, err = os.Create(offsetLogPath)
		if err != nil {
			segmentFile.Close() // Close segment file if offset file creation fails
			return fmt.Errorf("error creating offset log file: %v", err)
		}
	} else {
		// If the offset log file exists, we can read it to restore the offsets
		offsetFile, err = os.Open(offsetLogPath)
		if err != nil {
			segmentFile.Close() // Close segment file if offset file opening fails
			return fmt.Errorf("error opening offset log file: %v", err)
		}

		var consumerID string
		var offset int64
		for {
			_, err := fmt.Fscanf(offsetFile, "%s %d\n", &consumerID, &offset)
			if err != nil {
				if err == io.EOF {
					break // End of file reached
				}
				return fmt.Errorf("error reading offset log file: %v", err)
			}
			offsetMap[consumerID] = offset
		}
	}

	segmentMetadataPath = filepath.Join(dirs, "segment_metadata.state")
	if _, err := os.Stat(segmentMetadataPath); os.IsNotExist(err) {
		// create segment metadata file if it does not exist
		segmentMetadata, err = os.Create(segmentMetadataPath)
		if err != nil {
			segmentFile.Close() // Close segment file if segment metadata file creation fails
			offsetFile.Close()  // Close offset file if segment metadata file creation fails
			return fmt.Errorf("error creating segment metadata file: %v", err)
		}
	} else {
		// If the segment metadata file exists, we can read it to restore the metadata
		segmentMetadata, err = os.Open(segmentMetadataPath)
		if err != nil {
			segmentFile.Close() // Close segment file if segment metadata file opening fails
			offsetFile.Close()  // Close offset file if segment metadata file opening fails
			return fmt.Errorf("error opening segment metadata file: %v", err)
		}
		segmentFileMetadataMap = make(map[string]int64)
		var segmentFileName string
		var lastMessageID int64
		for {
			_, err := fmt.Fscanf(segmentMetadata, "%s %d\n", &segmentFileName, &lastMessageID)
			if err != nil {
				if err == io.EOF {
					break // End of file reached
				}
				return fmt.Errorf("error reading segment metadata file: %v", err)
			}
			segmentFileMetadataMap[segmentFileName] = lastMessageID
		}
	}

	return nil
}

func CloseFiles() error {
	mutex.Lock()
	defer mutex.Unlock()

	var errMsg string

	if segmentFile != nil {
		if err := segmentFile.Close(); err != nil {
			if errMsg != "" {
				errMsg += "; "
			}
			errMsg += fmt.Sprintf("segment file: %v", err)
		}
		segmentFile = nil
	}

	if offsetFile != nil {
		if err := offsetFile.Close(); err != nil {
			if errMsg != "" {
				errMsg += "; "
			}
			errMsg += fmt.Sprintf("offset file: %v", err)
		}
		offsetFile = nil
	}

	if segmentMetadata != nil {
		if err := segmentMetadata.Close(); err != nil {
			if errMsg != "" {
				errMsg += "; "
			}
			errMsg += fmt.Sprintf("segment metadata file: %v", err)
		}
		segmentMetadata = nil
	}

	if errMsg != "" {
		return fmt.Errorf("close errors: %s", errMsg)
	}
	return nil
}

func WriteMsgToSegment(msg internal.Msg) error {
	mutex.Lock()
	defer mutex.Unlock()
	// Implement logic to write a message to a specific segment file
	if segmentFile == nil {
		return fmt.Errorf("segment file is not open")
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("error marshaling message to JSON: %v", err)
	}
	if _, err := segmentFile.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("error writing message to segment file: %v", err)
	}

	// if segment file split(max-size or max-age), update segment file metadata
	if segmentFile != nil {
		stat, err := segmentFile.Stat()
		if err != nil {
			return fmt.Errorf("error getting segment file stats: %v", err)
		}
		// fmt.Println(stat.Size(), maxSize, stat.ModTime(), maxAge)
		if stat.Size() > (int64(maxSize*1024*1024)) || stat.ModTime().AddDate(0, 0, maxAge).Before(time.Now()) {
			// Split the segment file based on size or age
			oldSegmentFilePath, err := splitSegmentFile()
			if err != nil {
				return fmt.Errorf("error splitting segment file: %v", err)
			}
			// Update segment file metadata
			if err := updateSegmentFileMetadata(oldSegmentFilePath, msg.Id); err != nil {
				return fmt.Errorf("error updating segment file metadata: %v", err)
			}
		}

	}

	// Update the latest message ID
	if msg.Id > latestMessageID {
		latestMessageID = msg.Id
	}

	return nil
}

func ReadMsgForConsumer(consumerID string, maxCount int) ([]internal.Msg, error) {
	mutex.Lock()
	defer mutex.Unlock()

	if offsetMap == nil {
		return nil, fmt.Errorf("offset map is not initialized")
	}
	if consumerID == "" {
		return nil, fmt.Errorf("consumer ID is empty")
	}
	if maxCount <= 0 {
		return nil, fmt.Errorf("max count must be greater than 0")
	}
	if offsetFile == nil {
		return nil, fmt.Errorf("offset file is not open")
	}
	// Read the offset for the consumer
	lastOffset, exists := offsetMap[consumerID]
	if !exists {
		offsetMap[consumerID] = int64(0) // Initialize offset if it does not exist
	}
	// Read messages from the segment file starting from the last offset
	allMessages := []internal.Msg{}
	fileNames := make([]string, 0, len(segmentFileMetadataMap))
	for fileName := range segmentFileMetadataMap {
		fileNames = append(fileNames, fileName)
	}
	if _, err := os.Stat(filepath.Join(dirs, "segments.log")); err == nil {
		fileNames = append(fileNames, "segments.log") // Include the current segment file
	}

	sort.Strings(fileNames)
	for _, fileName := range fileNames {
		filePath := filepath.Join(dirs, fileName)
		messages, err := readMsgFromSegmentFile(filePath, lastOffset, maxCount)
		if err != nil {
			return nil, fmt.Errorf("error reading messages from segment file %s: %v", filePath, err)
		}
		allMessages = append(allMessages, messages...)
		if len(allMessages) >= maxCount {
			break // Stop reading if we have enough messages
		}
	}

	// if len(allMessages) == 0 {
	// 	return nil, fmt.Errorf("no messages available for consumer %s", consumerID)
	// }
	return allMessages, nil
}

func readMsgFromSegmentFile(filepath string, lastOffset int64, maxCount int) ([]internal.Msg, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("error opening segment file %s: %v", filepath, err)
	}
	defer f.Close()
	var messages []internal.Msg
	var msg internal.Msg
	count := int(0)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			fmt.Printf("invalid JSON line: %s", scanner.Text())
			continue
		}
		// Check if the message ID is greater than the last offset
		if msg.Id > lastOffset {
			messages = append(messages, msg)
			count++
			if count >= maxCount {
				break
			}
		}
	}
	return messages, nil
}

func WriteOffset(consumer_id string, messageID int64) error {
	mutex.Lock()
	defer mutex.Unlock()
	offsetMap[consumer_id] = messageID

	// 1. 임시 파일 생성
	tempFile, err := os.CreateTemp(filepath.Dir(offsetLogPath), "offset.temp")
	if err != nil {
		return fmt.Errorf("error creating temporary offset file: %v", err)
	}

	// 2. 임시 파일에 새로운 오프셋 맵 쓰기
	for consumerID, offset := range offsetMap {
		if _, err := tempFile.WriteString(fmt.Sprintf("%s %d\n", consumerID, offset)); err != nil {
			tempFile.Close()
			os.Remove(tempFile.Name()) // 에러 발생 시 임시 파일 삭제
			return fmt.Errorf("error writing offset to temporary file: %v", err)
		}
	}

	// 3. 쓰기 완료 후 임시 파일을 닫아야 Rename 가능
	if err := tempFile.Close(); err != nil {
		os.Remove(tempFile.Name())
		return fmt.Errorf("error closing temporary offset file: %v", err)
	}

	// 4. 원자적으로 임시 파일을 원본 파일로 변경 (덮어쓰기)
	if err := os.Rename(tempFile.Name(), offsetLogPath); err != nil {
		os.Remove(tempFile.Name())
		return fmt.Errorf("error renaming temporary offset file: %v", err)
	}

	return nil
}

func splitSegmentFile() (string, error) {
	// Split the segment file based on size
	if segmentFile == nil {
		return "", fmt.Errorf("segment file is not open")
	}
	segmentFile.Close()

	oldSegmentFilePath := filepath.Join(filepath.Dir(dirs), fmt.Sprintf("segment_%d.log", time.Now().UnixNano()))
	if err := os.Rename(segmentPath, oldSegmentFilePath); err != nil {
		return "", fmt.Errorf("error renaming segment file: %v", err)
	}
	// Create a new segment file
	var err error
	segmentFile, err = os.Create(segmentPath)
	if err != nil {
		// If we fail to create a new segment file, we should restore the old one
		if err := os.Rename(oldSegmentFilePath, segmentPath); err != nil {
			return "", fmt.Errorf("error restoring old segment file: %v", err)
		}
		return "", fmt.Errorf("error creating new segment file: %v", err)
	}
	return oldSegmentFilePath, nil
}

func deleteSegmentFile(minMessageID int64) error {
	// Implement logic to delete a specific segment file
	for segmentFileName, lastMessageID := range segmentFileMetadataMap {
		if lastMessageID < minMessageID {
			// Delete the segment file
			err := os.Remove(segmentFileName)
			if err != nil {
				return fmt.Errorf("error deleting segment file %s: %v", segmentFileName, err)
			}
			// Remove the metadata entry
			delete(segmentFileMetadataMap, segmentFileName)
		}
	}

	return nil
}

// segment file이 split 되었을 때, segment file metadata를 기록하는 함수
func updateSegmentFileMetadata(segmentFileName string, lastMessageID int64) error {
	if segmentFileMetadataMap == nil {
		segmentFileMetadataMap = make(map[string]int64)
	}
	segmentFileMetadataMap[segmentFileName] = lastMessageID
	// segmentFileMetadata to file save
	if err := writeSegmentFileMetadata(); err != nil {
		return fmt.Errorf("error writing segment file metadata: %v", err)
	}
	return nil
}

func writeSegmentFileMetadata() error {
	// Implement logic to write segment file metadata to a persistent storage
	if segmentMetadata == nil {
		return fmt.Errorf("segment metadata file is not open")
	}
	minMessageID := int64(0)
	segmentMetadata.Truncate(0) // Clear the file before writing new metadata
	segmentMetadata.Seek(0, 0)  // Reset the file pointer to the beginning
	for segmentFileName, lastMessageID := range segmentFileMetadataMap {
		if _, err := segmentMetadata.WriteString(fmt.Sprintf("%s %d\n", segmentFileName, lastMessageID)); err != nil {
			return fmt.Errorf("error writing segment file metadata: %v", err)
		}
		if minMessageID == 0 || lastMessageID < minMessageID {
			minMessageID = lastMessageID
		}
	}
	if err := segmentMetadata.Sync(); err != nil {
		return fmt.Errorf("error syncing segment metadata file: %v", err)
	}

	// Delete old segment files if necessary
	if err := deleteSegmentFile(minMessageID); err != nil {
		return fmt.Errorf("error deleting old segment files: %v", err)
	}
	return nil
}
