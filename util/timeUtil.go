package util

import (
	"fmt"
	"time"
)

// delay 문자열을 seconds 단위로 변환
// 빈 문자열인 경우 즉시 처리 (0초 지연)

func DelayToSeconds(delay string) (string, error) {
	mod := fmt.Sprintf("+%d seconds", 0)
	if delay == "" {
		return mod, nil
	}
	dur, err := time.ParseDuration(delay)
	if err != nil {
		return "", err
	}
	sec := int(dur.Round(time.Second).Seconds())
	if sec < 0 {
		sec = 0
	}
	return fmt.Sprintf("+%d seconds", sec), nil
}
