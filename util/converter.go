package util

import (
	"encoding/json"
	"fmt"
)

func PreviewStringRuneSafe(s string, maxRunes int) string {
	runes := []rune(s)
	if len(runes) > maxRunes {
		return string(runes[:maxRunes]) + "..."
	}
	return s
}

func ParseStringToInt64(s string, defaultValue int64) int64 {
	var result int64
	_, err := fmt.Sscan(s, &result)
	if err != nil {
		return defaultValue
	}
	return result
}

func ParseBytesToJsonRawMessage(data []byte) (json.RawMessage, error) {
	var raw json.RawMessage
	err := json.Unmarshal(data, &raw)
	if err != nil {
		return nil, err
	}
	return raw, nil
}
