package util

import (
	"math/rand"
)

func init() {
	rand.New(rand.NewSource(42)) // Seed the random number generator for reproducibility
}

func GenerateNumber(min int, max int) int {
	if min >= max {
		panic("min should be less than max")
	}
	return rand.Intn(max-min) + min // Generate a random integer between min and max
}
