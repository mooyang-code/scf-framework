package heartbeat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"time"

	"github.com/avast/retry-go"
	"github.com/mooyang-code/scf-framework/config"
	"github.com/mooyang-code/scf-framework/model"
	"github.com/mooyang-code/scf-framework/plugin"
	"trpc.group/trpc-go/trpc-go"
	"trpc.group/trpc-go/trpc-go/log"
)

// Reporter 心跳上报器
type Reporter struct {
	runtime   *config.RuntimeState
	taskStore *config.TaskInstanceStore
	plugin    plugin.Plugin
	client    *http.Client
}

// NewReporter 创建心跳上报器
func NewReporter(rs *config.RuntimeState, ts *config.TaskInstanceStore, p plugin.Plugin) *Reporter {
	return &Reporter{
		runtime:   rs,
		taskStore: ts,
		plugin:    p,
		client:    &http.Client{Timeout: 5 * time.Second},
	}
}

// ScheduledHeartbeat TRPC Timer 入口函数
func (r *Reporter) ScheduledHeartbeat(c context.Context, _ string) error {
	ctx := trpc.CloneContext(c)
	nodeID, version := r.runtime.GetNodeInfo()
	log.WithContextFields(ctx, "func", "ScheduledHeartbeat", "version", version, "nodeID", nodeID)

	log.DebugContextf(ctx, "ScheduledHeartbeat Enter")
	if err := r.Report(ctx); err != nil {
		log.ErrorContextf(ctx, "scheduled heartbeat failed: %v", err)
		return err
	}
	log.DebugContextf(ctx, "ScheduledHeartbeat Success")
	return nil
}

// Report 执行心跳上报
func (r *Reporter) Report(ctx context.Context) error {
	serverIP, serverPort := r.runtime.GetServerInfo()
	nodeID, localVersion := r.runtime.GetNodeInfo()

	log.DebugContextf(ctx, "ReportHeartbeat: serverIP=%s:%d, nodeID=%s, version=%s",
		serverIP, serverPort, nodeID, localVersion)

	if nodeID == "" {
		log.WarnContextf(ctx, "NodeID 为空，跳过心跳上报")
		return nil
	}
	if serverIP == "" {
		log.WarnContextf(ctx, "服务端 IP 未配置，跳过心跳上报")
		return nil
	}

	payload := r.buildPayload()
	packageVersion, err := r.sendToServer(ctx, payload, serverIP, serverPort)
	if err != nil {
		log.ErrorContextf(ctx, "failed to send heartbeat: %v", err)
		return fmt.Errorf("failed to send heartbeat: %w", err)
	}

	// 检查版本一致性
	if packageVersion != "" && packageVersion != localVersion {
		log.FatalContextf(ctx, "版本不一致，终止服务 - 本地版本: %s, 服务端版本: %s",
			localVersion, packageVersion)
	}
	return nil
}

// buildPayload 构建心跳负载
func (r *Reporter) buildPayload() map[string]interface{} {
	nodeID, version := r.runtime.GetNodeInfo()
	tasksMD5 := r.taskStore.GetCurrentMD5()

	payload := map[string]interface{}{
		"node_id":   nodeID,
		"node_type": "scf",
		"metadata": map[string]interface{}{
			"version":    version,
			"go_version": runtime.Version(),
			"os":         runtime.GOOS,
			"arch":       runtime.GOARCH,
		},
		"tasks_md5": tasksMD5,
	}

	// 检查插件是否实现了 HeartbeatContributor 接口
	if contributor, ok := r.plugin.(plugin.HeartbeatContributor); ok {
		extra := contributor.HeartbeatExtra()
		for k, v := range extra {
			payload[k] = v
		}
	}

	// 检查插件是否实现了 DynamicHeartbeatContributor 接口
	if dynContributor, ok := r.plugin.(plugin.DynamicHeartbeatContributor); ok {
		fn := dynContributor.HeartbeatExtraFunc()
		if fn != nil {
			for k, v := range fn() {
				payload[k] = v
			}
		}
	}

	return payload
}

// sendToServer POST 心跳数据到服务端，retry-go 5 次 BackOff
func (r *Reporter) sendToServer(ctx context.Context, payload map[string]interface{}, serverIP string, serverPort int) (string, error) {
	if serverIP == "" || serverPort <= 0 {
		return "", fmt.Errorf("invalid server address: %s:%d", serverIP, serverPort)
	}

	url := fmt.Sprintf("http://%s:%d/gateway/cloudnode/ReportHeartbeatInner", serverIP, serverPort)

	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal heartbeat payload: %w", err)
	}

	var packageVersion string

	err = retry.Do(
		func() error {
			req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(data))
			if err != nil {
				return fmt.Errorf("failed to create heartbeat request: %w", err)
			}
			req.Header.Set("Content-Type", "application/json")

			resp, err := r.client.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				respData, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("heartbeat request failed with status: %d, response: %s", resp.StatusCode, string(respData))
			}

			respData, err := io.ReadAll(resp.Body)
			if err != nil {
				return fmt.Errorf("failed to read response body: %w", err)
			}

			version, parseErr := r.parseServerResponse(ctx, respData)
			if parseErr != nil {
				log.WarnContextf(ctx, "failed to parse server response: %v", parseErr)
				return nil
			}
			packageVersion = version
			return nil
		},
		retry.Attempts(5),
		retry.Delay(1*time.Second),
		retry.DelayType(retry.BackOffDelay),
		retry.LastErrorOnly(true),
		retry.OnRetry(func(n uint, err error) {
			log.WarnContextf(ctx, "retrying heartbeat request, attempt: %d, error: %v", n+1, err)
		}),
		retry.Context(ctx),
	)
	return packageVersion, err
}

// parseServerResponse 解析服务端响应，提取 package_version 和 task_instances
func (r *Reporter) parseServerResponse(ctx context.Context, respData []byte) (string, error) {
	var serverResp model.ServerResponse
	if err := json.Unmarshal(respData, &serverResp); err != nil {
		return "", fmt.Errorf("failed to parse server response: %w", err)
	}

	if serverResp.Code != 200 {
		return "", fmt.Errorf("server returned error code: %d, message: %s", serverResp.Code, serverResp.Message)
	}

	if len(serverResp.Data) == 0 {
		return "", nil
	}

	dataMap, ok := serverResp.Data[0].(map[string]interface{})
	if !ok {
		return "", nil
	}

	packageVersion := extractPackageVersion(dataMap)
	r.processTaskInstances(ctx, dataMap)

	return packageVersion, nil
}

// extractPackageVersion 从响应数据中提取 package_version
func extractPackageVersion(dataMap map[string]interface{}) string {
	pv, exists := dataMap["package_version"]
	if !exists {
		return ""
	}
	versionStr, ok := pv.(string)
	if !ok {
		return ""
	}
	return versionStr
}

// processTaskInstances 解析并更新任务实例
func (r *Reporter) processTaskInstances(ctx context.Context, dataMap map[string]interface{}) {
	taskInstances, exists := dataMap["task_instances"]
	if !exists || taskInstances == nil {
		log.DebugContextf(ctx, "[Heartbeat] 响应中无任务实例数据")
		return
	}

	taskInstancesJSON, err := json.Marshal(taskInstances)
	if err != nil {
		log.WarnContextf(ctx, "[Heartbeat] failed to marshal task instances: %v", err)
		return
	}

	var tasks []model.TaskInstance
	if err := json.Unmarshal(taskInstancesJSON, &tasks); err != nil {
		log.WarnContextf(ctx, "[Heartbeat] failed to unmarshal task instances: %v", err)
		return
	}

	if len(tasks) == 0 {
		log.DebugContextf(ctx, "[Heartbeat] 任务MD5匹配，无需更新")
		return
	}

	log.InfoContextf(ctx, "[Heartbeat] 收到任务实例更新，任务数: %d", len(tasks))

	ptrs := make([]*model.TaskInstance, 0, len(tasks))
	for i := range tasks {
		ptrs = append(ptrs, &tasks[i])
	}

	r.taskStore.UpdateTaskInstances(ptrs)
	log.InfoContextf(ctx, "[Heartbeat] 任务实例已更新到内存，总任务数: %d, 当前MD5: %s",
		len(ptrs), r.taskStore.GetCurrentMD5())
}
