package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/mooyang-code/scf-framework/config"
	"github.com/mooyang-code/scf-framework/model"
	"trpc.group/trpc-go/trpc-go/log"
)

// Plugin 插件接口
type Plugin interface {
	Name() string
	Init(ctx context.Context, fw Framework) error
	OnTrigger(ctx context.Context, event *model.TriggerEvent) error
}

// Framework 框架接口，插件通过此接口访问框架能力
type Framework interface {
	Config() *config.FrameworkConfig
	Runtime() *config.RuntimeState
	TaskStore() *config.TaskInstanceStore
}

// HeartbeatContributor 可选接口，插件可实现此接口向心跳负载注入额外字段
type HeartbeatContributor interface {
	HeartbeatExtra() map[string]interface{}
}

// DynamicHeartbeatContributor 可选接口，支持每次心跳动态获取额外字段
type DynamicHeartbeatContributor interface {
	HeartbeatExtraFunc() func() map[string]interface{}
}

// ========== HTTPPluginAdapter ==========

// HTTPPluginOption HTTPPluginAdapter 的选项函数
type HTTPPluginOption func(*HTTPPluginAdapter)

// WithReadyTimeout 设置插件就绪探测超时时间
func WithReadyTimeout(d time.Duration) HTTPPluginOption {
	return func(a *HTTPPluginAdapter) {
		a.readyTimeout = d
	}
}

// WithHeartbeatExtra 设置心跳额外字段（静态）
func WithHeartbeatExtra(m map[string]interface{}) HTTPPluginOption {
	return func(a *HTTPPluginAdapter) {
		a.heartbeatExtra = m
	}
}

// WithHeartbeatExtraFunc 设置心跳额外字段获取函数（动态，每次心跳时调用）
func WithHeartbeatExtraFunc(fn func() map[string]interface{}) HTTPPluginOption {
	return func(a *HTTPPluginAdapter) {
		a.heartbeatExtraFunc = fn
	}
}

// HTTPPluginAdapter 通过 HTTP 调用外部插件进程的适配器
type HTTPPluginAdapter struct {
	name               string
	baseURL            string
	client             *http.Client
	readyTimeout       time.Duration
	heartbeatExtra     map[string]interface{}
	heartbeatExtraFunc func() map[string]interface{}
}

// NewHTTPPluginAdapter 创建 HTTPPluginAdapter
func NewHTTPPluginAdapter(name, baseURL string, opts ...HTTPPluginOption) *HTTPPluginAdapter {
	a := &HTTPPluginAdapter{
		name:         name,
		baseURL:      baseURL,
		client:       &http.Client{Timeout: 30 * time.Second},
		readyTimeout: 30 * time.Second,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Name 返回插件名称
func (a *HTTPPluginAdapter) Name() string {
	return a.name
}

// Init 循环探测 GET /health 等待插件进程就绪
func (a *HTTPPluginAdapter) Init(ctx context.Context, _ Framework) error {
	healthURL := fmt.Sprintf("%s/health", a.baseURL)
	deadline := time.Now().Add(a.readyTimeout)

	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
		if err != nil {
			return fmt.Errorf("failed to create health check request: %w", err)
		}

		resp, err := a.client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				log.InfoContextf(ctx, "[HTTPPluginAdapter] plugin %s is ready", a.name)
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}

	return fmt.Errorf("plugin %s not ready after %v", a.name, a.readyTimeout)
}

// OnTrigger POST /on-trigger 发送 TriggerEvent JSON
func (a *HTTPPluginAdapter) OnTrigger(ctx context.Context, event *model.TriggerEvent) error {
	triggerURL := fmt.Sprintf("%s/on-trigger", a.baseURL)

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal trigger event: %w", err)
	}

	// 调试日志：打印发送给插件的 payload 片段
	logData := string(data)
	if len(logData) > 500 {
		logData = logData[:500] + "..."
	}
	log.InfoContextf(ctx, "[HTTPPluginAdapter] sending to plugin: url=%s, body_len=%d, body=%s",
		triggerURL, len(data), logData)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, triggerURL, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create trigger request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send trigger event to plugin %s: %w", a.name, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("plugin %s returned status %d for trigger event", a.name, resp.StatusCode)
	}

	return nil
}

// HeartbeatExtra 返回心跳额外字段（合并静态和动态）
func (a *HTTPPluginAdapter) HeartbeatExtra() map[string]interface{} {
	result := make(map[string]interface{})
	// 静态字段
	for k, v := range a.heartbeatExtra {
		result[k] = v
	}
	// 动态字段（每次心跳时实时获取）
	if a.heartbeatExtraFunc != nil {
		for k, v := range a.heartbeatExtraFunc() {
			result[k] = v
		}
	}
	return result
}

// BaseURL 返回插件基础 URL（供 Gateway 转发使用）
func (a *HTTPPluginAdapter) BaseURL() string {
	return a.baseURL
}
