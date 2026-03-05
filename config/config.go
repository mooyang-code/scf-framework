package config

import (
	"fmt"
	"os"

	"github.com/mooyang-code/scf-framework/dnsproxy"
	"gopkg.in/yaml.v3"
)

// FrameworkConfig 框架配置（从 YAML 文件加载）
type FrameworkConfig struct {
	System    SystemConfig     `yaml:"system"`
	Heartbeat HeartbeatConfig  `yaml:"heartbeat"`
	Triggers  []TriggerConfig  `yaml:"triggers"`
	DNSProxy  *dnsproxy.Config `yaml:"dns_proxy,omitempty"` // DNS 代理配置，可选
	Storage   *StorageConfig   `yaml:"storage,omitempty"`   // xData 存储配置，可选
	Plugin    yaml.Node        `yaml:"plugin"`              // 延迟解析，留给插件
}

// StorageConfig xData 存储配置
type StorageConfig struct {
	AuthInfo  AuthInfoConfig `yaml:"auth_info"`
	ProjectID int            `yaml:"project_id"`
	DatasetID int            `yaml:"dataset_id"`
	WriteMode string         `yaml:"write_mode,omitempty"` // "set_data"（默认）或 "upsert_object"
	Freq      string         `yaml:"freq,omitempty"`       // SetData 时序频率，如 "1m"
}

// AuthInfoConfig xData 认证信息
type AuthInfoConfig struct {
	AppID  string `yaml:"app_id"`
	AppKey string `yaml:"app_key"`
}

// SystemConfig 系统配置
type SystemConfig struct {
	Name    string `yaml:"name"`
	Version string `yaml:"version"`
	Env     string `yaml:"env"`
}

// HeartbeatConfig 心跳配置
type HeartbeatConfig struct {
	Interval int `yaml:"interval"`
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
