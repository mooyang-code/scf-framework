package config

import (
	"os"
	"sync"
)

// RuntimeState 运行时状态管理
type RuntimeState struct {
	mu               sync.RWMutex
	nodeID           string
	version          string
	mooxServerURL    string // Moox Server 网关地址（由探测报文下发）
	storageServerURL string // xData 存储服务地址（由探测报文下发）
	storageServerRPC string // xData 存储服务 RPC 地址（由探测报文下发，格式 ip://host:port）
}

// NewRuntimeState 从配置初始化运行时状态
func NewRuntimeState(cfg *FrameworkConfig) *RuntimeState {
	return &RuntimeState{
		version: cfg.System.Version,
	}
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

// GetMooxServerURL 获取 Moox Server 网关地址
func (rs *RuntimeState) GetMooxServerURL() string {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.mooxServerURL
}

// UpdateMooxServerURL 更新 Moox Server 网关地址
func (rs *RuntimeState) UpdateMooxServerURL(url string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if url != "" {
		rs.mooxServerURL = url
	}
}

// GetStorageServerURL 获取 xData 存储服务地址
func (rs *RuntimeState) GetStorageServerURL() string {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.storageServerURL
}

// UpdateStorageServerURL 更新 xData 存储服务地址
func (rs *RuntimeState) UpdateStorageServerURL(url string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if url != "" {
		rs.storageServerURL = url
	}
}

// GetStorageServerRPC 获取 xData 存储服务 RPC 地址
func (rs *RuntimeState) GetStorageServerRPC() string {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.storageServerRPC
}

// UpdateStorageServerRPC 更新 xData 存储服务 RPC 地址
func (rs *RuntimeState) UpdateStorageServerRPC(rpcAddr string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rpcAddr != "" {
		rs.storageServerRPC = rpcAddr
	}
}
