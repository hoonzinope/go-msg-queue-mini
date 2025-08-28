package util

import (
	"github.com/google/uuid"
)

func GenerateGlobalID() string {
	return uuid.New().String()
}