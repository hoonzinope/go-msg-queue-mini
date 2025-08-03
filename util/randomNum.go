package util

import (
	"math/rand"
)

func GenerateNumber(min int, max int) int {
	return rand.Intn(max-min) + min // Generate a random integer between min and max
}
