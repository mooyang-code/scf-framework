package reporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	retry "github.com/avast/retry-go"
	"github.com/mooyang-code/scf-framework/config"
	"trpc.group/trpc-go/trpc-go"
	"trpc.group/trpc-go/trpc-go/log"
)

// TaskReporter 任务状态上报器
type TaskReporter struct {
	runtime *config.RuntimeState
	client  *http.Client
}

// NewTaskReporter 创建 TaskReporter
func NewTaskReporter(rs *config.RuntimeState) *TaskReporter {
	return &TaskReporter{
		runtime: rs,
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

// reportTaskStatusRequest 上报请求体
type reportTaskStatusRequest struct {
	ID     string `json:"id"`
	NodeID string `json:"node_id"`
	Status int    `json:"status"`
	Result string `json:"result"`
}

// ReportAsync 异步上报任务状态，不阻塞调用方。
// 使用 trpc.CloneContext 创建脱离 deadline 但保留日志字段的 context，
// 避免调用方 context 取消导致上报中断。
func (r *TaskReporter) ReportAsync(ctx context.Context, taskID string, status int, result string) {
	log.InfoContextf(ctx, "[TaskReporter] start async report: taskID=%s, status=%d", taskID, status)
	asyncCtx := trpc.CloneContext(ctx)
	go func() {
		if err := r.Report(asyncCtx, taskID, status, result); err != nil {
			log.ErrorContextf(asyncCtx, "[TaskReporter] async report failed: taskID=%s, status=%d, error=%v", taskID, status, err)
		}
	}()
}

// Report 同步上报任务状态，3 次重试 + 指数退避
func (r *TaskReporter) Report(ctx context.Context, taskID string, status int, result string) error {
	serverIP, serverPort := r.runtime.GetServerInfo()
	if serverIP == "" || serverPort <= 0 {
		log.WarnContextf(ctx, "[TaskReporter] skip report: server info not available (ip=%q, port=%d)", serverIP, serverPort)
		return nil
	}

	nodeID := r.runtime.GetNodeID()
	url := fmt.Sprintf("http://%s:%d/gateway/collectmgr/ReportTaskStatus", serverIP, serverPort)

	reqBody := reportTaskStatusRequest{
		ID:     taskID,
		NodeID: nodeID,
		Status: status,
		Result: result,
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	log.InfoContextf(ctx, "[TaskReporter] reporting: taskID=%s, nodeID=%s, status=%d, url=%s", taskID, nodeID, status, url)

	err = retry.Do(
		func() error {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
			if err != nil {
				return fmt.Errorf("failed to create request: %w", err)
			}
			req.Header.Set("Content-Type", "application/json")

			resp, err := r.client.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
			}

			return nil
		},
		retry.Attempts(3),
		retry.Delay(500*time.Millisecond),
		retry.DelayType(retry.BackOffDelay),
		retry.LastErrorOnly(true),
		retry.OnRetry(func(n uint, err error) {
			log.WarnContextf(ctx, "[TaskReporter] retrying: taskID=%s, attempt=%d, error=%v", taskID, n+1, err)
		}),
		retry.Context(ctx),
	)

	if err != nil {
		log.ErrorContextf(ctx, "[TaskReporter] report failed after retries: taskID=%s, status=%d, error=%v", taskID, status, err)
		return err
	}

	log.InfoContextf(ctx, "[TaskReporter] report success: taskID=%s, status=%d", taskID, status)
	return nil
}
