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
		Auth    struct {
			APIKey string `yaml:"api_key"`
		} `yaml:"auth"`
	} `yaml:"http"`
}

func ReadConfig(filePath string) (*Config, error) {
	yamlFile, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	// --- 추가된 부분 시작 ---
    // os.ExpandEnv 함수를 사용해 ${VAR} 또는 $VAR 형태의 문자열을
    // 해당 환경 변수의 값으로 치환합니다.
    expandedYaml := os.ExpandEnv(string(yamlFile))
    // --- 추가된 부분 끝 ---

	var config Config
	err = yaml.Unmarshal([]byte(expandedYaml), &config)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling config file: %w", err)
	}
	return &config, nil
}
