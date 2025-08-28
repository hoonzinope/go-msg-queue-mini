package util

import (
	"fmt"

	"github.com/google/uuid"
)

func GenerateGlobalID() string {
	return fmt.Sprintf("%d", uuid.New().ID())
}