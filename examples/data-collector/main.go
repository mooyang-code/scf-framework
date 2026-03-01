// Package main 演示如何使用 scf-framework 实现 data-collector 云函数。
//
// data-collector 是一个多源数据采集服务，通过定时器周期性地从交易所（如 Binance）
// 采集 K线数据，写入存储服务。
//
// 架构：
//
//	SCF 入口 (Go, port 9000)
//	  └── scf-framework（心跳、探测、Gateway、定时采集、DNS 解析）
//	        └── DataCollectorPlugin（内置 Go 采集逻辑）
//
// 对应的 config.yaml:
//
//	system:
//	  name: "data-collector"
//	  version: "v0.0.3"
//	  env: "production"
//	heartbeat:
//	  interval: 9
//	triggers:
//	  - name: "scheduled-collect"
//	    type: "timer"
//	    settings:
//	      cron: "0 * * * * *"     # 每分钟触发一次采集
//	dns_proxy:                      # 可选，框架层 DNS 定时解析
//	  dns_servers:
//	    - "8.8.8.8"
//	    - "1.1.1.1"
//	    - "localhost"
//	  dns_timeout: 5
//	  scheduled_domains:
//	    - "api.binance.com"
//	    - "fapi.binance.com"
//	  probe_configs:
//	    - domain: "api.binance.com"
//	      probe_type: "https"
//	      probe_api:
//	        path: "/api/v3/time"
//	        method: "GET"
//	        timeout: 3
//	        expected_status: 200
//
// 对应的 trpc_go.yaml:
//
//	server:
//	  service:
//	    - name: trpc.collector.gateway.stdhttp
//	      network: tcp
//	      protocol: http_no_protocol
//	      ip: 0.0.0.0
//	      port: 9000
//	    - name: trpc.heartbeat.timer
//	      network: tcp
//	      protocol: timer
//	      timeout: 30000
//	      timer:
//	        cron: "*/9 * * * * *"
//	    - name: trpc.dns.timer            # DNS 刷新定时器
//	      network: tcp
//	      protocol: timer
//	      timeout: 30000
//	      timer:
//	        cron: "30 * * * * *"
//	    - name: trpc.timer.minute
//	      network: tcp
//	      protocol: timer
//	      timeout: 60000
//	      timer:
//	        cron: "0 * * * * *"
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	scf "github.com/mooyang-code/scf-framework"
	"github.com/mooyang-code/scf-framework/model"
	"github.com/mooyang-code/scf-framework/plugin"
)

func main() {
	app := scf.New(
		&DataCollectorPlugin{},
		scf.WithConfigPath("./config.yaml"),
		scf.WithGatewayService("trpc.collector.gateway.stdhttp"),
	)

	if err := app.Run(context.Background()); err != nil {
		log.Fatalf("data-collector exited: %v", err)
	}
}

// ============================================================================
// DataCollectorPlugin 数据采集插件
// ============================================================================

// DataCollectorPlugin 实现 plugin.Plugin 和 plugin.HeartbeatContributor
type DataCollectorPlugin struct {
	fw         plugin.Framework
	collectors []string // 支持的采集器类型
}

func (p *DataCollectorPlugin) Name() string { return "data-collector" }

func (p *DataCollectorPlugin) Init(_ context.Context, fw plugin.Framework) error {
	p.fw = fw
	p.collectors = []string{"binance-spot-kline", "binance-swap-kline"}
	return nil
}

// OnTrigger 路由不同触发器事件到对应的处理函数
func (p *DataCollectorPlugin) OnTrigger(ctx context.Context, event *model.TriggerEvent) (*model.TriggerResponse, error) {
	switch event.Name {
	case "scheduled-collect":
		return nil, p.executeScheduledCollect(ctx, event)
	default:
		return nil, fmt.Errorf("unknown trigger: %s", event.Name)
	}
}

// HeartbeatExtra 向心跳注入支持的采集器列表
func (p *DataCollectorPlugin) HeartbeatExtra() map[string]interface{} {
	return map[string]interface{}{
		"supported_collectors": p.collectors,
	}
}

// ============================================================================
// 业务逻辑
// ============================================================================

// triggerPayload 触发器事件携带的负载数据（与框架 TriggerPayload 对应）
type triggerPayload struct {
	Jobs []model.TaskJob `json:"jobs"`
}

// collectTaskParams 采集任务参数（从 TaskInstance.TaskParams JSON 解析）
type collectTaskParams struct {
	DataType   string `json:"data_type"`   // kline
	DataSource string `json:"data_source"` // binance
	InstType   string `json:"inst_type"`   // SPOT / SWAP
	Symbol     string `json:"symbol"`      // BTC-USDT
}

// executeScheduledCollect 定时采集：从 payload.jobs 读取框架已筛选的任务并执行
func (p *DataCollectorPlugin) executeScheduledCollect(ctx context.Context, event *model.TriggerEvent) error {
	var payload triggerPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	if len(payload.Jobs) == 0 {
		fmt.Println("[DataCollector] 无待执行 jobs")
		return nil
	}

	fmt.Printf("[DataCollector] 开始执行采集，jobs 数: %d\n", len(payload.Jobs))

	for _, job := range payload.Jobs {
		var params collectTaskParams
		if err := json.Unmarshal([]byte(job.Task.TaskParams), &params); err != nil {
			fmt.Printf("[DataCollector] 解析任务参数失败: taskID=%s, err=%v\n", job.Task.TaskID, err)
			continue
		}

		// 实际业务中：
		// 1. 调用 Binance API 获取 K线数据
		// 2. 写入 xData 存储: POST {storageURL}/xData/SetData
		// 3. 上报任务执行状态
		fmt.Printf("[DataCollector] 采集: source=%s, type=%s, symbol=%s, interval=%s\n",
			params.DataSource, params.InstType, params.Symbol, job.Interval)
	}

	return nil
}

// 编译期检查接口实现
var _ plugin.Plugin = (*DataCollectorPlugin)(nil)
var _ plugin.HeartbeatContributor = (*DataCollectorPlugin)(nil)
