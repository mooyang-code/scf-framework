package heartbeat

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/mooyang-code/scf-framework/config"
	"github.com/mooyang-code/scf-framework/model"
	"github.com/mooyang-code/scf-framework/plugin"
	"trpc.group/trpc-go/trpc-go/log"
)

// ProbeHandler 探测请求处理器
type ProbeHandler struct {
	runtime *config.RuntimeState
	plugin  plugin.Plugin
}

// NewProbeHandler 创建探测处理器
func NewProbeHandler(rs *config.RuntimeState, p plugin.Plugin) *ProbeHandler {
	return &ProbeHandler{
		runtime: rs,
		plugin:  p,
	}
}

// ProcessProbe 处理探测请求
func (h *ProbeHandler) ProcessProbe(ctx context.Context, event model.CloudFunctionEvent) (*model.Response, error) {
	// 从 SCF 环境变量获取函数名
	functionName := os.Getenv("SCF_FUNCTIONNAME")
	if functionName == "" {
		functionName = os.Getenv("TENCENTCLOUD_FUNCTIONNAME")
	}

	currentNodeID, currentVersion := h.runtime.GetNodeInfo()
	log.WithContextFields(ctx, "func", "ProcessProbe", "version", currentVersion, "nodeID", currentNodeID)

	log.DebugContextf(ctx, "[ProcessProbe] functionName=%s, currentNodeID=%s, version=%s",
		functionName, currentNodeID, currentVersion)

	// 更新 NodeID
	if functionName != "" {
		h.runtime.UpdateNodeInfo(functionName, currentVersion)
		log.DebugContextf(ctx, "[ProcessProbe] NodeID 已更新为 %s", functionName)
	}

	// 更新服务端连接信息
	if event.ServerIP != "" && event.ServerPort > 0 {
		log.DebugContextf(ctx, "[ProcessProbe] 更新服务端地址 %s:%d", event.ServerIP, event.ServerPort)
		h.runtime.UpdateServerInfo(event.ServerIP, event.ServerPort)
	} else {
		log.WarnContextf(ctx, "[ProcessProbe] 服务端地址信息缺失 ServerIP=%s, ServerPort=%d",
			event.ServerIP, event.ServerPort)
	}

	// 构建探测响应
	probeResponse, err := h.buildProbeResponse()
	if err != nil {
		return &model.Response{
			Success: false,
			Message: fmt.Sprintf("failed to build response: %v", err),
		}, nil
	}

	return &model.Response{
		Success:   true,
		Message:   "probe handled successfully",
		Data:      probeResponse,
		Timestamp: time.Now(),
	}, nil
}

// buildProbeResponse 构建探测响应
func (h *ProbeHandler) buildProbeResponse() (*model.ProbeResponse, error) {
	nodeID, version := h.runtime.GetNodeInfo()
	if nodeID == "" {
		return nil, fmt.Errorf("node ID is empty")
	}

	serverIP, serverPort := h.runtime.GetServerInfo()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return &model.ProbeResponse{
		NodeID:    nodeID,
		State:     "running",
		Timestamp: time.Now(),
		Details: model.ProbeDetails{
			NodeInfo: &model.NodeInfo{
				NodeID:       nodeID,
				NodeType:     "scf",
				Version:      version,
				RunningTasks: make([]string, 0),
				Capabilities: []string{h.plugin.Name()},
				Metadata: map[string]string{
					"go_version": runtime.Version(),
					"os":         runtime.GOOS,
					"arch":       runtime.GOARCH,
				},
			},
			TaskStats: model.TaskStatsInfo{},
			Metrics: &model.NodeMetrics{
				CPUUsage:    0,
				MemoryUsage: float64(memStats.Alloc) / 1024 / 1024,
				Timestamp:   time.Now(),
			},
			SystemInfo: model.SystemInfo{
				GoVersion:    runtime.Version(),
				OS:           runtime.GOOS,
				Arch:         runtime.GOARCH,
				NumCPU:       runtime.NumCPU(),
				NumGoroutine: runtime.NumGoroutine(),
			},
			HeartbeatInfo: model.HeartbeatInfo{
				LastReport: time.Now(),
				Interval:   "30s",
				ServerIP:   serverIP,
				ServerPort: serverPort,
			},
		},
	}, nil
}
