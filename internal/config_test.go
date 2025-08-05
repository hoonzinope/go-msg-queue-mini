package internal

import (
	"testing"
)

func TestReadConfig(t *testing.T) {
	config, err := ReadConfig("../config.yml")
	if err != nil {
		t.Fatalf("Failed to read config: %v", err)
	} else {
		t.Logf("Config read successfully: %+v", config)
	}
	config_type := config.Persistence.Type
	switch config_type {
	case "memory":
		t.Logf("Config type is memory: %s", config_type)
	case "file":
		t.Logf("Config type is file: %s", config_type)
		config_options := config.Persistence.Options
		if config_options.DirsPath == "" {
			t.Error("DirsPath is empty in file persistence config")
		} else {
			t.Logf("DirsPath is set to: %s", config_options.DirsPath)
		}
	}
}
