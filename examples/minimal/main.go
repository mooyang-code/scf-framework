// Package main 演示 scf-framework 的最小使用示例。
//
// 这是最简单的接入方式：实现 Plugin 接口 + 编写 config.yaml + 启动。
// 适合快速验证或作为新项目的起步模板。
//
// 对应的 config.yaml:
//
//	system:
//	  name: "my-service"
//	  version: "v0.0.1"
//	  env: "development"
//	heartbeat:
//	  server_ip: "10.0.0.1"
//	  server_port: 8080
//	  interval: 30
//	triggers:
//	  - name: "health-check"
//	    type: "timer"
//	    settings:
//	      cron: "0 * * * * *"
//
// 对应的 trpc_go.yaml:
//
//	server:
//	  service:
//	    - name: trpc.myservice.gateway.stdhttp
//	      network: tcp
//	      protocol: http_no_protocol
//	      ip: 0.0.0.0
//	      port: 9000
//	    - name: trpc.heartbeat.timer
//	      network: tcp
//	      protocol: timer
//	      timeout: 30000
//	      timer:
//	        cron: "*/30 * * * * *"
//	    - name: trpc.timer.minute
//	      network: tcp
//	      protocol: timer
//	      timeout: 60000
//	      timer:
//	        cron: "0 * * * * *"
package main

import (
	"context"
	"fmt"
	"log"

	scf "github.com/mooyang-code/scf-framework"
	"github.com/mooyang-code/scf-framework/model"
	"github.com/mooyang-code/scf-framework/plugin"
)

func main() {
	app := scf.New(
		&MyPlugin{},
		scf.WithConfigPath("./config.yaml"),
		scf.WithGatewayService("trpc.myservice.gateway.stdhttp"),
	)

	if err := app.Run(context.Background()); err != nil {
		log.Fatalf("service exited: %v", err)
	}
}

// MyPlugin 最简插件实现
type MyPlugin struct {
	fw plugin.Framework
}

func (p *MyPlugin) Name() string { return "my-service" }

func (p *MyPlugin) Init(_ context.Context, fw plugin.Framework) error {
	p.fw = fw
	fmt.Printf("[MyPlugin] initialized: version=%s, env=%s\n",
		fw.Config().System.Version, fw.Config().System.Env)
	return nil
}

func (p *MyPlugin) OnTrigger(ctx context.Context, event *model.TriggerEvent) error {
	fmt.Printf("[MyPlugin] trigger: type=%s, name=%s\n", event.Type, event.Name)

	// 访问运行时状态
	nodeID, version := p.fw.Runtime().GetNodeInfo()
	fmt.Printf("[MyPlugin]   node=%s, version=%s\n", nodeID, version)

	// 访问任务实例
	tasks := p.fw.TaskStore().GetByNode(nodeID)
	fmt.Printf("[MyPlugin]   tasks=%d, md5=%s\n", len(tasks), p.fw.TaskStore().GetCurrentMD5())

	return nil
}
