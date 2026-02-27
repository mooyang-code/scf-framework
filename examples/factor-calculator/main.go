// Package main 演示如何使用 scf-framework 实现 factor-calculator 云函数。
//
// factor-calculator 是一个因子计算服务，通过 NATS JetStream 接收 K线数据，
// 调用 Python 计算引擎计算因子值，再写入 xData 存储。
//
// 架构：
//
//	SCF 入口 (Go, port 9000)
//	  ├── scf-framework（心跳、探测、Gateway、NATS 消费）
//	  └── HTTPPluginAdapter → 计算引擎 (Python, port 9001)
//
// 对应的 config.yaml:
//
//	system:
//	  name: "factor-calculator"
//	  version: "v0.1.0"
//	  env: "production"
//	  storage_url: "http://43.136.59.72:19104"
//	heartbeat:
//	  server_ip: "10.0.0.1"
//	  server_port: 8080
//	  interval: 9
//	triggers:
//	  - name: "kline-consumer"
//	    type: "nats"
//	    settings:
//	      url: "nats://127.0.0.1:4222"
//	      stream: "KLINE"
//	      subject: "kline.>"
//	      consumer_name: "factor-calculator"
//	      batch_size: 10
//	      ack_wait: 30
//	      max_deliver: 3
//	      fetch_max_wait: 5
//	plugin:
//	  engine_url: "http://127.0.0.1:9001"
//	  engine_timeout: 30
//
// 对应的 trpc_go.yaml:
//
//	server:
//	  service:
//	    - name: trpc.factor.gateway.stdhttp
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
//	    - name: trpc.timer.second
//	      network: tcp
//	      protocol: timer
//	      timeout: 5000
//	      timer:
//	        cron: "* * * * * *"
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	scf "github.com/mooyang-code/scf-framework"
	"github.com/mooyang-code/scf-framework/model"
	"github.com/mooyang-code/scf-framework/plugin"
	"gopkg.in/yaml.v3"
)

func main() {
	// 方式一：使用 HTTPPluginAdapter（Go 侧仅做框架，计算逻辑在 Python 引擎中）
	// 适用于计算引擎是独立进程的场景
	p := plugin.NewHTTPPluginAdapter(
		"factor-calculator",
		"http://127.0.0.1:9001",
		plugin.WithReadyTimeout(60*time.Second),
		plugin.WithHeartbeatExtra(map[string]interface{}{
			"supported_collectors": []string{"PctChange", "MA", "RSI", "MACD"},
		}),
	)

	app := scf.New(p,
		scf.WithConfigPath("./config.yaml"),
		scf.WithGatewayService("trpc.factor.gateway.stdhttp"),
	)

	if err := app.Run(context.Background()); err != nil {
		log.Fatalf("factor-calculator exited: %v", err)
	}
}

// ============================================================================
// 方式二（备选）：使用原生 Go Plugin 实现全部业务逻辑
// 如果计算逻辑也在 Go 中，可以直接实现 plugin.Plugin 接口
// ============================================================================

// FactorPlugin Go 原生插件实现
type FactorPlugin struct {
	fw        plugin.Framework
	engineURL string
}

func (p *FactorPlugin) Name() string { return "factor-calculator" }

func (p *FactorPlugin) Init(_ context.Context, fw plugin.Framework) error {
	p.fw = fw

	// 从 framework config 的 plugin 节点解析业务配置
	var pluginCfg struct {
		EngineURL     string `yaml:"engine_url"`
		EngineTimeout int    `yaml:"engine_timeout"`
	}
	if err := fw.Config().Plugin.Decode(&pluginCfg); err != nil && fw.Config().Plugin.Kind != 0 {
		return fmt.Errorf("decode plugin config: %w", err)
	}
	if pluginCfg.EngineURL != "" {
		p.engineURL = pluginCfg.EngineURL
	} else {
		p.engineURL = "http://127.0.0.1:9001"
	}
	return nil
}

func (p *FactorPlugin) OnTrigger(ctx context.Context, event *model.TriggerEvent) (*model.TriggerResponse, error) {
	switch event.Type {
	case model.TriggerNATS:
		return nil, p.handleKlineMessage(ctx, event)
	case model.TriggerTimer:
		return nil, p.handleTimerTick(ctx, event)
	default:
		return nil, fmt.Errorf("unknown trigger type: %s", event.Type)
	}
}

// HeartbeatExtra 向心跳注入支持的因子列表
func (p *FactorPlugin) HeartbeatExtra() map[string]interface{} {
	return map[string]interface{}{
		"supported_collectors": []string{"PctChange", "MA", "RSI", "MACD"},
	}
}

// handleKlineMessage 处理 NATS K线消息
func (p *FactorPlugin) handleKlineMessage(ctx context.Context, event *model.TriggerEvent) error {
	var kline KlineMessage
	if err := json.Unmarshal(event.Payload, &kline); err != nil {
		return fmt.Errorf("unmarshal kline: %w", err)
	}

	_ = ctx // 实际业务中：
	// 1. 更新本地 K线缓存
	// 2. 根据 symbol + interval 查询匹配的任务实例
	//    tasks := p.fw.TaskStore().GetByNode(p.fw.Runtime().GetNodeID())
	// 3. 从缓存获取历史 K线
	// 4. 调用计算引擎: POST http://engine/calculate
	// 5. 将结果写入 xData 存储

	fmt.Printf("[FactorPlugin] 收到K线: symbol=%s, interval=%s, close=%.4f\n",
		kline.Symbol, kline.Interval, kline.Close)

	return nil
}

// handleTimerTick 处理定时器事件（可用于定期清理缓存等）
func (p *FactorPlugin) handleTimerTick(_ context.Context, event *model.TriggerEvent) error {
	fmt.Printf("[FactorPlugin] 定时触发: name=%s\n", event.Name)
	return nil
}

// KlineMessage NATS 中的 K线数据消息体
type KlineMessage struct {
	Symbol      string  `json:"symbol"`
	Interval    string  `json:"interval"`
	InstType    string  `json:"inst_type,omitempty"`
	OpenTime    string  `json:"open_time"`
	CloseTime   string  `json:"close_time,omitempty"`
	Open        float64 `json:"open"`
	High        float64 `json:"high"`
	Low         float64 `json:"low"`
	Close       float64 `json:"close"`
	Volume      float64 `json:"volume"`
	QuoteVolume float64 `json:"quote_volume,omitempty"`
}

// 使用原生 Go Plugin 的启动方式（供参考，本示例 main 使用的是 HTTPPluginAdapter）
func runWithNativePlugin() {
	app := scf.New(&FactorPlugin{},
		scf.WithConfigPath("./config.yaml"),
		scf.WithGatewayService("trpc.factor.gateway.stdhttp"),
	)
	if err := app.Run(context.Background()); err != nil {
		log.Fatalf("factor-calculator exited: %v", err)
	}
}

// 确保 yaml 包被使用（Plugin.Decode 需要）
var _ = yaml.Node{}
