package util

import (
	"math/rand"
)

func GenerateNumber(min int, max int) int {
	if min >= max {
		panic("min should be less than max")
	}
	return rand.Intn(max-min) + min // Generate a random integer between min and max
}

func GenerateJitter(backoff int) int {
	jitter := rand.Intn(backoff / 2) // Generate a random jitter between 0 and backoff/2
	return backoff + jitter
}
