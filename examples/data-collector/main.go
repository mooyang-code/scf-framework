// Package main 演示如何使用 scf-framework 实现 data-collector 云函数。
//
// data-collector 是一个多源数据采集服务，通过定时器周期性地从交易所（如 Binance）
// 采集 K线数据，写入存储服务。
//
// 架构：
//
//	SCF 入口 (Go, port 9000)
//	  └── scf-framework（心跳、探测、Gateway、定时采集）
//	        └── DataCollectorPlugin（内置 Go 采集逻辑）
//
// 对应的 config.yaml:
//
//	system:
//	  name: "data-collector"
//	  version: "v0.0.3"
//	  env: "production"
//	  storage_url: "http://43.136.59.72:19104"
//	heartbeat:
//	  server_ip: "10.0.0.1"
//	  server_port: 8080
//	  interval: 9
//	triggers:
//	  - name: "scheduled-collect"
//	    type: "timer"
//	    settings:
//	      cron: "0 * * * * *"     # 每分钟触发一次采集
//	  - name: "dns-refresh"
//	    type: "timer"
//	    settings:
//	      cron: "0 */5 * * * *"   # 每5分钟刷新 DNS
//	plugin:
//	  storage_url: "http://43.136.59.72:19104"
//	  dns_servers:
//	    - "8.8.8.8"
//	    - "1.1.1.1"
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
	"strings"
	"time"

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
	storageURL string
	collectors []string // 支持的采集器类型
}

func (p *DataCollectorPlugin) Name() string { return "data-collector" }

func (p *DataCollectorPlugin) Init(_ context.Context, fw plugin.Framework) error {
	p.fw = fw
	p.storageURL = fw.Config().System.StorageURL
	p.collectors = []string{"binance-spot-kline", "binance-swap-kline"}
	return nil
}

// OnTrigger 路由不同触发器事件到对应的处理函数
func (p *DataCollectorPlugin) OnTrigger(ctx context.Context, event *model.TriggerEvent) error {
	switch event.Name {
	case "scheduled-collect":
		return p.executeScheduledCollect(ctx)
	case "dns-refresh":
		return p.refreshDNS(ctx)
	default:
		return fmt.Errorf("unknown trigger: %s", event.Name)
	}
}

// HeartbeatExtra 向心跳注入支持的采集器列表和本地 DNS 记录
func (p *DataCollectorPlugin) HeartbeatExtra() map[string]interface{} {
	return map[string]interface{}{
		"supported_collectors": p.collectors,
		"local_dns_records": map[string][]string{
			"api.binance.com":  {"203.107.43.166", "47.254.55.110"},
			"fapi.binance.com": {"47.254.55.111"},
		},
	}
}

// ============================================================================
// 业务逻辑
// ============================================================================

// executeScheduledCollect 定时采集：获取当前节点的任务实例，逐个执行采集
func (p *DataCollectorPlugin) executeScheduledCollect(ctx context.Context) error {
	nodeID := p.fw.Runtime().GetNodeID()
	tasks := p.fw.TaskStore().GetByNode(nodeID)
	if len(tasks) == 0 {
		fmt.Printf("[DataCollector] 当前节点 %s 无采集任务\n", nodeID)
		return nil
	}

	fmt.Printf("[DataCollector] 开始执行采集，任务数: %d\n", len(tasks))

	now := time.Now()
	for _, task := range tasks {
		// 解析任务参数
		params, err := parseTaskParams(task.TaskParams)
		if err != nil {
			fmt.Printf("[DataCollector] 解析任务参数失败: taskID=%s, err=%v\n", task.TaskID, err)
			continue
		}

		// 检查当前时刻是否应该执行
		for _, interval := range params.Intervals {
			if !shouldExecute(now, interval) {
				continue
			}

			// 实际业务中：
			// 1. 调用 Binance API 获取 K线数据
			// 2. 写入 xData 存储: POST {storageURL}/xData/SetData
			// 3. 上报任务执行状态
			fmt.Printf("[DataCollector] 采集: source=%s, type=%s, symbol=%s, interval=%s\n",
				params.DataSource, params.InstType, params.Symbol, interval)
		}
	}

	return nil
}

// refreshDNS 定期刷新 DNS 解析
func (p *DataCollectorPlugin) refreshDNS(_ context.Context) error {
	// 实际业务中：
	// 1. 使用配置的 DNS 服务器解析域名
	// 2. 对每个 IP 进行延迟探测
	// 3. 更新本地最优 IP 列表
	fmt.Println("[DataCollector] DNS 刷新完成")
	return nil
}

// ============================================================================
// 辅助类型和函数
// ============================================================================

// CollectTaskParams 采集任务参数（从 TaskInstance.TaskParams JSON 解析）
type CollectTaskParams struct {
	DataType   string   `json:"data_type"`   // kline
	DataSource string   `json:"data_source"` // binance
	InstType   string   `json:"inst_type"`   // SPOT / SWAP
	Symbol     string   `json:"symbol"`      // BTC-USDT
	Intervals  []string `json:"intervals"`   // ["1m","5m","1h"]
}

func parseTaskParams(raw string) (*CollectTaskParams, error) {
	var params CollectTaskParams
	if err := json.Unmarshal([]byte(raw), &params); err != nil {
		return nil, err
	}
	return &params, nil
}

// shouldExecute 检查当前时间是否应触发指定周期的采集
// 例如 "5m" → 当前分钟数能被5整除时触发, "1h" → 分钟数为0时触发
func shouldExecute(now time.Time, interval string) bool {
	minute := now.Minute()

	switch {
	case interval == "1m":
		return true
	case strings.HasSuffix(interval, "m"):
		var n int
		fmt.Sscanf(interval, "%dm", &n)
		if n <= 0 {
			return false
		}
		return minute%n == 0
	case interval == "1h":
		return minute == 0
	case strings.HasSuffix(interval, "h"):
		var n int
		fmt.Sscanf(interval, "%dh", &n)
		if n <= 0 {
			return false
		}
		return minute == 0 && now.Hour()%n == 0
	default:
		return false
	}
}

// 编译期检查接口实现
var _ plugin.Plugin = (*DataCollectorPlugin)(nil)
var _ plugin.HeartbeatContributor = (*DataCollectorPlugin)(nil)
