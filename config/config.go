package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// FrameworkConfig 框架配置（从 YAML 文件加载）
type FrameworkConfig struct {
	System    SystemConfig    `yaml:"system"`
	Heartbeat HeartbeatConfig `yaml:"heartbeat"`
	Triggers  []TriggerConfig `yaml:"triggers"`
	Plugin    yaml.Node       `yaml:"plugin"` // 延迟解析，留给插件
}

// SystemConfig 系统配置
type SystemConfig struct {
	Name       string `yaml:"name"`
	Version    string `yaml:"version"`
	Env        string `yaml:"env"`
	StorageURL string `yaml:"storage_url"`
}

// HeartbeatConfig 心跳配置
type HeartbeatConfig struct {
	ServerIP   string `yaml:"server_ip"`
	ServerPort int    `yaml:"server_port"`
	Interval   int    `yaml:"interval"`
}

// TriggerConfig 触发器配置
type TriggerConfig struct {
	Name     string                 `yaml:"name" json:"name"`
	Type     string                 `yaml:"type" json:"type"`
	Settings map[string]interface{} `yaml:"settings" json:"settings"`
}

// LoadFrameworkConfig 从 YAML 文件加载框架配置
func LoadFrameworkConfig(path string) (*FrameworkConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	var cfg FrameworkConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", path, err)
	}

	return &cfg, nil
}
