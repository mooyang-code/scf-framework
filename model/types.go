package model

import (
	"encoding/json"
	"time"
)

// ========== 触发器相关 ==========

// TriggerType 触发源类型
type TriggerType string

const (
	TriggerTimer TriggerType = "timer"
	TriggerNATS  TriggerType = "nats"
	TriggerHTTP  TriggerType = "http"
)

// TriggerEvent 触发事件
type TriggerEvent struct {
	Type     TriggerType       `json:"type"`
	Name     string            `json:"name"`
	Payload  json.RawMessage   `json:"payload,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// TriggerConfig 触发器配置（从 YAML 解析）
type TriggerConfig struct {
	Name     string                 `yaml:"name" json:"name"`
	Type     string                 `yaml:"type" json:"type"`
	Settings map[string]interface{} `yaml:"settings" json:"settings"`
}

// ========== 心跳相关 ==========

// CloudFunctionEvent 云函数事件（Web 函数版本，通过 HTTP 接收）
type CloudFunctionEvent struct {
	Action     string                 `json:"action"`
	Source     string                 `json:"source,omitempty"`
	Data       map[string]interface{} `json:"data,omitempty"`
	RequestID  string                 `json:"request_id,omitempty"`
	Timestamp  string                 `json:"timestamp,omitempty"`
	ServerIP   string                 `json:"server_ip,omitempty"`
	ServerPort int                    `json:"server_port,omitempty"`
}

// HeartbeatPayload 心跳上报负载（业务特有字段通过 HeartbeatContributor 注入）
type HeartbeatPayload struct {
	NodeID       string                 `json:"node_id"`
	NodeType     string                 `json:"node_type"`
	Timestamp    time.Time              `json:"timestamp"`
	RunningTasks []*TaskSummary         `json:"running_tasks"`
	Metrics      *NodeMetrics           `json:"metrics"`
	TasksMD5     string                 `json:"tasks_md5"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
}

// NodeInfo 节点信息
type NodeInfo struct {
	NodeID       string            `json:"node_id"`
	NodeType     string            `json:"node_type"`
	Region       string            `json:"region,omitempty"`
	Namespace    string            `json:"namespace,omitempty"`
	Version      string            `json:"version,omitempty"`
	RunningTasks []string          `json:"running_tasks"`
	Capabilities []string          `json:"capabilities,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

// NodeMetrics 节点指标
type NodeMetrics struct {
	CPUUsage    float64   `json:"cpu_usage"`
	MemoryUsage float64  `json:"memory_usage"`
	TaskCount   int       `json:"task_count"`
	SuccessRate float64   `json:"success_rate"`
	ErrorCount  int       `json:"error_count"`
	Timestamp   time.Time `json:"timestamp"`
}

// TaskSummary 任务摘要
type TaskSummary struct {
	TaskID string `json:"task_id"`
	Status string `json:"status"`
}

// Response 通用响应
type Response struct {
	Success   bool        `json:"success"`
	Message   string      `json:"message"`
	Data      interface{} `json:"data,omitempty"`
	RequestID string      `json:"request_id,omitempty"`
	Timestamp time.Time   `json:"timestamp"`
}

// ========== 探测相关 ==========

// ProbeResponse 探测响应
type ProbeResponse struct {
	NodeID    string       `json:"node_id"`
	State     string       `json:"state"`
	Timestamp time.Time    `json:"timestamp"`
	Details   ProbeDetails `json:"details"`
}

// ProbeDetails 探测详情
type ProbeDetails struct {
	NodeInfo      *NodeInfo      `json:"node_info"`
	RunningTasks  []*TaskSummary `json:"running_tasks,omitempty"`
	TaskStats     TaskStatsInfo  `json:"task_stats"`
	Metrics       *NodeMetrics   `json:"metrics"`
	SystemInfo    SystemInfo     `json:"system_info"`
	HeartbeatInfo HeartbeatInfo  `json:"heartbeat_info"`
}

// TaskStatsInfo 任务统计信息
type TaskStatsInfo struct {
	Total   int `json:"total"`
	Running int `json:"running"`
	Pending int `json:"pending"`
	Stopped int `json:"stopped"`
	Error   int `json:"error"`
}

// SystemInfo 系统信息
type SystemInfo struct {
	GoVersion    string `json:"go_version"`
	OS           string `json:"os"`
	Arch         string `json:"arch"`
	NumCPU       int    `json:"num_cpu"`
	NumGoroutine int    `json:"num_goroutine"`
}

// HeartbeatInfo 心跳信息
type HeartbeatInfo struct {
	LastReport  time.Time `json:"last_report"`
	ReportCount int64     `json:"report_count"`
	ErrorCount  int64     `json:"error_count"`
	Interval    string    `json:"interval"`
	ServerIP    string    `json:"server_ip"`
	ServerPort  int       `json:"server_port"`
}

// ========== 任务执行结果 ==========

// 任务状态常量
const (
	TaskStatusSuccess = 2 // 执行成功
	TaskStatusFailed  = 4 // 执行失败
)

// TaskResult 插件返回的单个任务执行结果
type TaskResult struct {
	TaskID string `json:"task_id"`
	Status int    `json:"status"` // 2=成功, 4=失败
	Result string `json:"result"` // 失败原因（成功时为空）
}

// TriggerResponse 插件对触发事件的响应
type TriggerResponse struct {
	TaskResults []TaskResult `json:"task_results,omitempty"`
}

// ========== 任务实例 ==========

// TaskInstance 通用任务实例
type TaskInstance struct {
	ID         int                    `json:"id"`
	TaskID     string                 `json:"task_id"`
	RuleID     string                 `json:"rule_id"`
	NodeID     string                 `json:"planned_exec_node"`
	TaskParams string                 `json:"task_params"`
	Invalid    int                    `json:"invalid"`
	Extra      map[string]interface{} `json:"extra,omitempty"`
}

// ========== 服务端响应 ==========

// ServerResponse 服务端响应结构
type ServerResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    []any  `json:"data"`
	Total   *int64 `json:"total,omitempty"`
}
