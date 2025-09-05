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
	MaxRetry      int    `yaml:"max-retry"`      // Maximum number of retries for message processing
	RetryInterval string `yaml:"retry-interval"` // Interval between retries
	LeaseDuration string `yaml:"lease-duration"` // Duration for message lease
	Debug         bool   `yaml:"debug"`          // Enable debug mode

	HTTP struct {
		Enabled bool `yaml:"enabled"`
		Port    int  `yaml:"port"`
		Rate    struct {
			Limit int `yaml:"limit"`
			Burst int `yaml:"burst"`
		} `yaml:"rate"`
		Auth    struct {
			APIKey string `yaml:"api_key"`
		} `yaml:"auth"`
	} `yaml:"http"`

	GRPC struct {
		Enabled bool `yaml:"enabled"`
		Port    int  `yaml:"port"`
		Auth    struct {
			APIKey string `yaml:"api_key"`
		} `yaml:"auth"`
	} `yaml:"grpc"`
}

func ReadConfig(filePath string) (*Config, error) {
	yamlFile, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

    // os.ExpandEnv to replace environment variables in the YAML content
    expandedYaml := os.ExpandEnv(string(yamlFile))

	var config Config
	err = yaml.Unmarshal([]byte(expandedYaml), &config)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling config file: %w", err)
	}
	return &config, nil
}
