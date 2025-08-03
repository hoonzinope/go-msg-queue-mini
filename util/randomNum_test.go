package util

import (
	"fmt"
	"testing"
)

func TestGenerateItem(t *testing.T) {
	item := GenerateItem()
	if item == nil {
		t.Error("Expected a non-nil item, got nil")
	}

	strItem, ok := item.(string)
	if !ok {
		t.Errorf("Expected item to be of type string, got %T", item)
	}

	if len(strItem) < 20 {
		t.Errorf("Expected item to be at least 20 characters long, got %s", strItem)
	}

	fmt.Println("Generated item:", strItem)
}

func TestGenerateNumber(t *testing.T) {
	min, max := 1, 10
	num := GenerateNumber(min, max)

	if num < min || num >= max {
		t.Errorf("Expected number to be between %d and %d, got %d", min, max, num)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic when min is not less than max, but did not panic")
		}
	}()

	// This should panic
	GenerateNumber(max, min)
}
