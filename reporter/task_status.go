package reporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/mooyang-code/scf-framework/config"
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

// ReportAsync 异步上报任务状态，不阻塞调用方
func (r *TaskReporter) ReportAsync(ctx context.Context, taskID string, status int, result string) {
	go func() {
		if err := r.Report(ctx, taskID, status, result); err != nil {
			log.ErrorContextf(ctx, "[TaskReporter] async report failed: taskID=%s, error=%v", taskID, err)
		}
	}()
}

// Report 同步上报任务状态，3次重试+指数退避
func (r *TaskReporter) Report(ctx context.Context, taskID string, status int, result string) error {
	serverIP, serverPort := r.runtime.GetServerInfo()
	if serverIP == "" || serverPort <= 0 {
		log.WarnContextf(ctx, "[TaskReporter] skip report: server info not available")
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

	var lastErr error
	const maxRetries = 3
	backoff := 500 * time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
		if err != nil {
			lastErr = fmt.Errorf("failed to create request: %w", err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := r.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("request failed: %w", err)
			log.WarnContextf(ctx, "[TaskReporter] attempt %d/%d failed: taskID=%s, error=%v",
				attempt+1, maxRetries, taskID, err)
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			log.InfoContextf(ctx, "[TaskReporter] report success: taskID=%s, status=%d", taskID, status)
			return nil
		}

		lastErr = fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
		log.WarnContextf(ctx, "[TaskReporter] attempt %d/%d failed: taskID=%s, status=%d, body=%s",
			attempt+1, maxRetries, taskID, resp.StatusCode, string(body))
	}

	return fmt.Errorf("all %d retries exhausted for taskID=%s: %w", maxRetries, taskID, lastErr)
}
