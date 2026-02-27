package config

import (
	"os"
	"sync"
)

// RuntimeState 运行时状态管理
type RuntimeState struct {
	mu         sync.RWMutex
	nodeID     string
	version    string
	serverIP   string
	serverPort int
}

// NewRuntimeState 从配置初始化运行时状态
func NewRuntimeState(cfg *FrameworkConfig) *RuntimeState {
	rs := &RuntimeState{
		version:    cfg.System.Version,
		serverIP:   cfg.Heartbeat.ServerIP,
		serverPort: cfg.Heartbeat.ServerPort,
	}
	return rs
}

// InitNodeIDFromEnv 从 SCF 环境变量读取 NodeID
func (rs *RuntimeState) InitNodeIDFromEnv() {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if nodeID := os.Getenv("SCF_FUNCTIONNAME"); nodeID != "" {
		rs.nodeID = nodeID
		return
	}
	if nodeID := os.Getenv("TENCENTCLOUD_FUNCTIONNAME"); nodeID != "" {
		rs.nodeID = nodeID
	}
}

// GetNodeID 获取节点ID
func (rs *RuntimeState) GetNodeID() string {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.nodeID
}

// SetNodeID 设置节点ID
func (rs *RuntimeState) SetNodeID(id string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.nodeID = id
}

// GetNodeInfo 获取节点信息
func (rs *RuntimeState) GetNodeInfo() (nodeID, version string) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.nodeID, rs.version
}

// UpdateNodeInfo 更新节点信息
func (rs *RuntimeState) UpdateNodeInfo(nodeID, version string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.nodeID = nodeID
	rs.version = version
}

// GetServerInfo 获取服务端地址
func (rs *RuntimeState) GetServerInfo() (ip string, port int) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.serverIP, rs.serverPort
}

// UpdateServerInfo 更新服务端地址
func (rs *RuntimeState) UpdateServerInfo(ip string, port int) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if ip != "" {
		rs.serverIP = ip
	}
	if port > 0 {
		rs.serverPort = port
	}
}
