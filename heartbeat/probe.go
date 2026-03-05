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
	"github.com/mooyang-code/scf-framework/storage"
	"trpc.group/trpc-go/trpc-go/log"
)

// ProbeHandler 探测请求处理器
type ProbeHandler struct {
	runtime       *config.RuntimeState
	plugin        plugin.Plugin
	storageWriter *storage.RPCWriter
	storageReader *storage.Reader
}

// NewProbeHandler 创建探测处理器
func NewProbeHandler(rs *config.RuntimeState, p plugin.Plugin, sw *storage.RPCWriter, sr *storage.Reader) *ProbeHandler {
	return &ProbeHandler{
		runtime:       rs,
		plugin:        p,
		storageWriter: sw,
		storageReader: sr,
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
	if event.MooxServerURL != "" {
		log.DebugContextf(ctx, "[ProcessProbe] 更新 Moox Server 地址 %s", event.MooxServerURL)
		h.runtime.UpdateMooxServerURL(event.MooxServerURL)
	} else {
		log.WarnContextf(ctx, "[ProcessProbe] Moox Server 地址信息缺失")
	}

	// 更新存储服务地址
	if event.StorageServerURL != "" {
		log.DebugContextf(ctx, "[ProcessProbe] 更新存储服务地址 %s", event.StorageServerURL)
		h.runtime.UpdateStorageServerURL(event.StorageServerURL)
	}

	// 更新存储服务 RPC 地址，并动态刷新 storageWriter/storageReader 的 target
	if event.StorageServerRPC != "" {
		log.DebugContextf(ctx, "[ProcessProbe] 更新存储服务 RPC 地址 %s", event.StorageServerRPC)
		h.runtime.UpdateStorageServerRPC(event.StorageServerRPC)
		if h.storageWriter != nil {
			h.storageWriter.UpdateURL(event.StorageServerRPC)
		}
		if h.storageReader != nil {
			h.storageReader.UpdateURL(event.StorageServerRPC)
		}
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

	serverURL := h.runtime.GetMooxServerURL()

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
				LastReport:    time.Now(),
				Interval:      "30s",
				MooxServerURL: serverURL,
			},
		},
	}, nil
}
