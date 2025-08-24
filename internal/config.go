package internal

import (
	"fmt"
	"os"

	"go.yaml.in/yaml/v3"
)

type Config struct {
	Persistence struct {
		Type    string `yaml:"type"`
		Options struct {
			DirsPath string `yaml:"dirs-path"`
		} `yaml:"options"`
	} `yaml:"persistence"`
	MaxRetry int `yaml:"max-retry"` // Maximum number of retries for message processing
}

func ReadConfig(filePath string) (*Config, error) {
	yamlFile, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}
	var config Config
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling config file: %w", err)
	}
	return &config, nil
}
