package util

import (
	"math/rand"
)

func GenerateItem() interface{} {
	// timestamp := time.Now().Format("2006-01-02 15:04:05")
	message := _stringWithCharset(GenerateNumber(2, 20), charset) // Generate a random message of 20 characters
	return message
}

// charset use random string
const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// stringWithCharset return of random string
func _stringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func GenerateNumber(min int, max int) int {
	if min >= max {
		panic("min should be less than max")
	}
	return rand.Intn(max-min) + min // Generate a random integer between min and max
}

func GenerateJitter(backoff int) int {
	if backoff < 2 {
		return backoff
	}
	jitter := rand.Intn(backoff / 2) // Generate a random jitter between 0 and backoff/2
	return backoff + jitter
}
