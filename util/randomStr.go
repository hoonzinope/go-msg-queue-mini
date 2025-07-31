package util

import (
	"fmt"
	"math/rand"
	"time"
)

func GenerateItem() interface{} {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	message := stringWithCharset(20, charset) // Generate a random message of 20 characters
	return fmt.Sprintf("%s %s", timestamp, message)
}

// charset use random string
const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

// stringWithCharset return of random string
func stringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}
