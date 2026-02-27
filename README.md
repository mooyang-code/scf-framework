# SCF Framework - 云函数计算框架技术架构说明

## 一、项目概述

SCF Framework 是一个基于 Go 语言的云函数计算框架，运行于腾讯云 SCF（Serverless Cloud Function）Web 函数模式之上。框架封装了云函数运行所需的基础设施——心跳上报、任务分发、触发器调度、HTTP 网关——使业务开发者只需实现 `Plugin` 接口即可完成业务逻辑，无需关心底层调度细节。

框架同时支持 **Go 原生插件** 和 **跨语言插件**（Python 等），后者通过内置的 `HTTPPluginAdapter` 以 HTTP 协议桥接。

---

## 二、项目结构

```
scf-framework/
├── app.go                  # 主应用入口，App 生命周期管理
├── options.go              # App 选项配置（WithConfigPath, WithGatewayService 等）
│
├── config/
│   ├── config.go           # FrameworkConfig YAML 加载
│   ├── runtime.go          # RuntimeState 运行时状态（nodeID、server 信息）
│   └── task_store.go       # TaskInstanceStore 任务实例内存缓存（并发安全、MD5 变更检测）
│
├── plugin/
│   └── plugin.go           # Plugin 接口定义 + HTTPPluginAdapter 实现
│
├── trigger/
│   ├── trigger.go          # Trigger 接口定义
│   ├── manager.go          # TriggerManager 触发器生命周期管理
│   ├── timer.go            # TimerTrigger 基于 cron 的定时触发器
│   └── nats.go             # NATSTrigger NATS JetStream Pull Consumer 触发器
│
├── gateway/
│   ├── gateway.go          # HTTP Gateway（健康检查、探测、catch-all 转发）
│   └── forwarder.go        # HTTP 请求转发器（反向代理到插件进程）
│
├── heartbeat/
│   ├── heartbeat.go        # Reporter 心跳上报器（含服务端响应解析、任务实例更新）
│   └── probe.go            # ProbeHandler 探测请求处理（服务端发现节点）
│
├── reporter/
│   └── task_status.go      # TaskReporter 异步任务状态上报
│
├── model/
│   └── types.go            # 共享数据模型（TriggerEvent, TaskInstance, HeartbeatPayload 等）
│
├── python/
│   ├── scf_log/            # Python CLS 日志模块（腾讯云日志服务集成）
│   └── examples/           # Python 使用示例
│
└── examples/
    ├── minimal/            # 最小化 Go 插件示例
    ├── data-collector/     # 数据采集器示例（HTTPPluginAdapter + Python）
    └── factor-calculator/  # 因子计算示例（NATS 触发器）
```

---

## 三、核心架构

### 3.1 整体架构图

```
┌─────────────────────────────────────────────────────────────┐
│                      Moox Server（服务端）                    │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────────┐  │
│  │ 任务调度中心  │  │ 心跳管理服务  │  │ 版本/配置管理中心  │  │
│  └──────┬───────┘  └──────┬───────┘  └────────┬──────────┘  │
└─────────┼─────────────────┼───────────────────┼─────────────┘
          │                 │                   │
          ▼                 ▼                   ▼
┌─────────────────────────────────────────────────────────────┐
│              SCF 云函数节点（scf-framework 驱动）             │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │                    App（主应用）                        │  │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐  │  │
│  │  │ Config   │ │ Runtime  │ │TaskStore │ │ Gateway  │  │  │
│  │  │ Loader   │ │ State    │ │ (内存)   │ │ (HTTP)   │  │  │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘  │  │
│  │                                                        │  │
│  │  ┌─────────────────────┐  ┌─────────────────────────┐  │  │
│  │  │   Heartbeat Reporter │  │    Trigger Manager      │  │  │
│  │  │  (TRPC Timer 驱动)  │  │  ┌────────┐ ┌────────┐ │  │  │
│  │  │  - 上报节点状态      │  │  │ Timer  │ │ NATS   │ │  │  │
│  │  │  - 接收任务实例      │  │  │Trigger │ │Trigger │ │  │  │
│  │  │  - 版本一致性检查    │  │  └────────┘ └────────┘ │  │  │
│  │  └─────────────────────┘  └────────────┬────────────┘  │  │
│  │                                        │               │  │
│  │                               ┌────────▼────────┐      │  │
│  │                               │     Plugin      │      │  │
│  │                               │  (业务逻辑层)    │      │  │
│  │                               └─────────────────┘      │  │
│  └────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 App 启动流程

`App.Run()` 方法（`app.go:62`）按以下顺序初始化所有子系统：

```
1. LoadFrameworkConfig     → 加载 YAML 配置文件
2. trpc.NewServer()        → 创建 TRPC Server
3. NewRuntimeState         → 初始化运行时状态（从环境变量读取 NodeID）
4. NewTaskInstanceStore    → 初始化任务实例内存缓存
5. plugin.Init()           → 调用插件初始化（Go 插件直接调用；HTTP 插件轮询 /health）
6. Gateway.Register()      → 注册 HTTP 网关（可选）
7. HeartbeatReporter       → 注册心跳定时器（TRPC Timer）
8. TriggerManager.Init()   → 创建并初始化所有触发器
9. RegisterTimerSchedulers → 注册秒/分/时粒度的 TRPC Timer
10. TriggerManager.StartAll → 启动非 Timer 触发器（如 NATS）
11. Signal Listener         → 监听 SIGTERM/SIGINT 信号
12. Server.Serve()          → 启动 TRPC Server（阻塞）
```

---

## 四、核心模块详解

### 4.1 Plugin 插件系统

**文件**: `plugin/plugin.go`

插件系统是框架的核心扩展点。所有业务逻辑均通过实现 `Plugin` 接口接入。

```go
// Plugin 插件接口
type Plugin interface {
    Name() string
    Init(ctx context.Context, fw Framework) error
    OnTrigger(ctx context.Context, event *model.TriggerEvent) (*model.TriggerResponse, error)
}

// Framework 框架接口，插件通过此接口访问框架能力
type Framework interface {
    Config() *config.FrameworkConfig
    Runtime() *config.RuntimeState
    TaskStore() *config.TaskInstanceStore
}
```

| 方法 | 说明 |
|------|------|
| `Name()` | 返回插件名称，用于日志标识 |
| `Init()` | 初始化插件，可通过 `Framework` 接口读取配置、运行时状态 |
| `OnTrigger()` | 触发事件到达时调用，返回任务执行结果（框架自动异步上报） |

**可选接口**：

- `HeartbeatContributor`：注入静态心跳额外字段
- `DynamicHeartbeatContributor`：注入动态心跳额外字段（每次心跳时调用函数）

#### 两种插件模式

| 模式 | 适用语言 | 通信方式 | 实现方式 |
|------|---------|---------|---------|
| Go 原生插件 | Go | 进程内函数调用 | 直接实现 `Plugin` 接口 |
| HTTP 插件适配器 | Python/Node.js/任意语言 | HTTP REST (localhost) | 使用 `HTTPPluginAdapter` |

### 4.2 HTTPPluginAdapter

**文件**: `plugin/plugin.go:67`

`HTTPPluginAdapter` 是框架内置的跨语言桥接层，将 `Plugin` 接口调用转换为 HTTP 请求：

```
Framework                           外部插件进程
   │                                    │
   │── Init() ──► GET /health ─────────►│  (轮询等待就绪，默认 30s 超时)
   │                                    │
   │── OnTrigger() ► POST /on-trigger ─►│  (JSON: TriggerEvent)
   │◄──────────── JSON: TriggerResponse ┤  (含 task_results)
```

**配置选项**：

```go
p := plugin.NewHTTPPluginAdapter(
    "my-plugin",                           // 插件名称
    "http://127.0.0.1:9001",              // 插件进程地址
    plugin.WithReadyTimeout(60*time.Second), // 就绪探测超时
    plugin.WithHeartbeatExtra(map[string]interface{}{...}),  // 静态心跳字段
    plugin.WithHeartbeatExtraFunc(func() map[string]interface{}{...}), // 动态心跳字段
)
```

### 4.3 Trigger 触发器系统

**文件**: `trigger/trigger.go`, `trigger/manager.go`

```go
// Trigger 触发器接口
type Trigger interface {
    Name() string
    Type() model.TriggerType
    Init(ctx context.Context, cfg model.TriggerConfig) error
    Start(ctx context.Context, handler TriggerHandler) error
    Stop(ctx context.Context) error
}

// TriggerHandler 触发事件处理函数
type TriggerHandler func(ctx context.Context, event *model.TriggerEvent) error
```

#### 内置触发器类型

| 类型 | 文件 | 说明 |
|------|------|------|
| `timer` | `trigger/timer.go` | 基于 cron 表达式的定时触发器，由 TRPC Timer 驱动，支持秒/分/时三种粒度 |
| `nats` | `trigger/nats.go` | NATS JetStream Pull Consumer，持续拉取消息并触发处理 |

#### TriggerManager 工作流

`TriggerManager`（`trigger/manager.go`）是所有触发器的统一管理者：

1. **初始化**：根据 YAML 配置创建触发器实例
2. **Handler 包装**：`wrapHandler()` 在调用 `Plugin.OnTrigger()` 前注入：
   - 无 deadline 的新 context（避免 TRPC Timer 超时影响业务）
   - 结构化日志字段（nodeID, version, plugin, trigger）
   - TaskStore 快照（当前所有任务实例 + MD5）
3. **结果上报**：OnTrigger 返回后，异步上报 `TaskResult` 到服务端

#### TimerTrigger 滑动窗口机制

`TimerTrigger`（`trigger/timer.go:67`）采用 **滑动窗口** 避免漏触发：

```
上次 Tick 时间 (lastTick)          当前 Tick 时间 (now)
        │                                │
        ├────── 窗口 (lastTick, now] ─────┤
        │                                │
        │   如果 cron.Next(lastTick) ≤ now → 触发
```

### 4.4 Heartbeat 心跳系统

**文件**: `heartbeat/heartbeat.go`, `heartbeat/probe.go`

心跳系统负责节点与服务端的双向通信：

```
        ┌───────────┐
        │  服务端    │
        └─────┬─────┘
              │
    ┌─────────┼──────────┐
    ▼                     ▼
 POST /probe           POST /ReportHeartbeatInner
 (服务端 → 节点)        (节点 → 服务端)
 探测节点存活/          上报节点状态
 下发 server IP+Port    携带 tasks_md5
                        ↓
                   服务端响应中包含：
                   - package_version（版本一致性检查）
                   - task_instances（MD5 不匹配时下发新任务列表）
```

**心跳上报间隔**：由配置文件 `heartbeat.interval` 控制（通过 TRPC Timer 驱动）。

**版本一致性**：心跳响应中若 `package_version` 与本地版本不一致，框架会 Fatal 终止服务，由 SCF 平台重新拉起新版本。

### 4.5 TaskInstanceStore 任务存储

**文件**: `config/task_store.go`

- 内存级并发安全 Map（`sync.RWMutex`）
- 存储服务端下发的 `TaskInstance` 列表
- 通过 MD5 哈希检测任务列表变更（心跳上报 `tasks_md5`，服务端仅在 MD5 不匹配时才下发新列表）
- 每次触发事件携带完整 TaskStore 快照（由 TriggerManager 注入 `event.Payload`）

### 4.6 Gateway HTTP 网关

**文件**: `gateway/gateway.go`

| 路由 | 方法 | 说明 |
|------|------|------|
| `/health` | GET | 健康检查 |
| `/probe` | POST | 接收服务端探测请求（下发 server IP/Port） |
| `/*` (catch-all) | ANY | 转发到插件进程（HTTPPluginAdapter 模式） |

Gateway 使用 TRPC 的 `http_no_protocol` 模式注册，监听端口由 `trpc_go.yaml` 配置（默认 9000）。

---

## 五、数据流

### 5.1 定时采集完整数据流

```
                                      ┌──────────────────┐
                                      │   Moox Server    │
                                      └────────┬─────────┘
                                               │
                                     ① 心跳响应下发任务实例
                                               │
                                               ▼
┌──────────────────────────────────────────────────────────┐
│                     SCF 云函数节点                         │
│                                                           │
│  ② TRPC Timer Tick ──► TimerTrigger.Tick()                │
│                              │                            │
│                  ③ cron 匹配，生成 TriggerEvent            │
│                              │                            │
│                   ④ TriggerManager.wrapHandler()           │
│                      注入 context + TaskStore 快照         │
│                              │                            │
│                   ⑤ Plugin.OnTrigger(event)               │
│                       ┌──────┴──────┐                     │
│                       │ Go 原生     │ HTTP 适配器          │
│                       │ 直接调用    │ POST /on-trigger     │
│                       └─────────────┘──► 外部插件进程      │
│                              │                            │
│                   ⑥ 返回 TriggerResponse                  │
│                      (TaskResult[])                        │
│                              │                            │
│                   ⑦ TaskReporter.ReportAsync()             │
│                      异步上报每个 TaskResult               │
│                              │                            │
│                              ▼                            │
│                      POST /ReportTaskStatus               │
│                              │                            │
└──────────────────────────────┼────────────────────────────┘
                               │
                               ▼
                        ┌──────────────┐
                        │  Moox Server  │
                        └──────────────┘
```

### 5.2 NATS 消息驱动数据流

```
NATS JetStream ──► NATSTrigger.consumeLoop()
                         │
                    Fetch batch messages
                         │
                    对每条消息生成 TriggerEvent
                    （msg.Data() 作为 Payload）
                         │
                    TriggerManager.wrapHandler()
                    注入 context + TaskStore 快照
                         │
                    Plugin.OnTrigger(event)
                         │
                    成功 → msg.Ack()
                    失败 → msg.Nak()（触发重投递）
```

---

## 六、配置说明

### 6.1 框架配置文件 (config.yaml)

```yaml
system:
  name: "my-function"          # 函数名称
  version: "v1.0.0"           # 版本号（与服务端 package_version 比对）
  env: "production"            # 环境标识
  storage_url: "http://..."    # 存储服务地址

heartbeat:
  server_ip: ""                # 服务端 IP（通常由 probe 动态注入）
  server_port: 0               # 服务端端口（通常由 probe 动态注入）
  interval: 9                  # 心跳间隔（秒），对应 TRPC Timer 配置

triggers:
  - name: "my-timer"           # 触发器名称
    type: "timer"              # 类型：timer | nats
    settings:
      cron: "0 * * * * * *"    # 7 位 cron（秒 分 时 日 月 周 年）

  - name: "my-queue"
    type: "nats"
    settings:
      url: "nats://..."
      stream: "my-stream"
      subject: "my.subject"
      consumer_name: "my-consumer"
      batch_size: 10
      ack_wait: 30
      max_deliver: 3

plugin:                        # 插件自定义配置（yaml.Node，延迟解析）
  cls:                         # 例如 CLS 日志配置
    topic_id: "xxx"
    secret_id: "xxx"
```

### 6.2 TRPC 配置文件 (trpc_go.yaml)

```yaml
server:
  service:
    - name: trpc.myapp.gateway.stdhttp   # HTTP Gateway
      network: tcp
      protocol: http_no_protocol
      ip: 0.0.0.0
      port: 9000
    - name: trpc.heartbeat.timer          # 心跳定时器
      protocol: timer
      timeout: 10000
    - name: trpc.timer.second             # 秒级定时器
      protocol: timer
      timeout: 10000
    - name: trpc.timer.minute             # 分钟级定时器
      protocol: timer
      timeout: 120000
    - name: trpc.timer.hour               # 小时级定时器
      protocol: timer
      timeout: 3700000
```

---

## 七、接入指南

### 7.1 Go 原生插件接入

```go
package main

import (
    "context"
    "log"

    scf "github.com/mooyang-code/scf-framework"
    "github.com/mooyang-code/scf-framework/model"
    "github.com/mooyang-code/scf-framework/plugin"
    "trpc.group/trpc-go/trpc-go"
)

// MyPlugin 实现 plugin.Plugin 接口
type MyPlugin struct{}

func (p *MyPlugin) Name() string { return "my-plugin" }

func (p *MyPlugin) Init(ctx context.Context, fw plugin.Framework) error {
    // 读取配置: fw.Config()
    // 读取运行时状态: fw.Runtime()
    return nil
}

func (p *MyPlugin) OnTrigger(ctx context.Context, event *model.TriggerEvent) (*model.TriggerResponse, error) {
    // 处理触发事件
    // event.Name     → 触发器名称
    // event.Type     → 触发器类型 (timer/nats)
    // event.Payload  → 载荷（包含 TaskStore 快照）
    // event.Metadata → 元数据（nodeID, version 等）

    return &model.TriggerResponse{
        TaskResults: []model.TaskResult{
            {TaskID: "task-1", Status: model.TaskStatusSuccess, Result: ""},
        },
    }, nil
}

func main() {
    app := scf.New(&MyPlugin{},
        scf.WithConfigPath("./config.yaml"),
        scf.WithGatewayService("trpc.myapp.gateway.stdhttp"),
    )
    if err := app.Run(trpc.BackgroundContext()); err != nil {
        log.Fatalf("exited: %v", err)
    }
}
```

### 7.2 Python 插件接入

Python 插件接入采用 **双进程架构**：Go 进程运行 scf-framework（含 HTTPPluginAdapter），Python 进程运行业务逻辑，两者通过 HTTP 通信。

#### 第一步：Go 侧 - 使用 HTTPPluginAdapter

```go
// cmd/serverless/main.go
package main

import (
    "log"
    "time"

    scf "github.com/mooyang-code/scf-framework"
    "github.com/mooyang-code/scf-framework/plugin"
    "trpc.group/trpc-go/trpc-go"
)

func main() {
    p := plugin.NewHTTPPluginAdapter(
        "my-python-plugin",
        "http://127.0.0.1:9001",              // Python HTTP Server 地址
        plugin.WithReadyTimeout(60*time.Second), // 等待 Python 进程就绪
        plugin.WithHeartbeatExtra(map[string]interface{}{
            "capabilities": []string{"kline", "ticker"},
        }),
    )

    app := scf.New(p,
        scf.WithConfigPath("./configs/config.yaml"),
        scf.WithGatewayService("trpc.myapp.gateway.stdhttp"),
    )

    if err := app.Run(trpc.BackgroundContext()); err != nil {
        log.Fatalf("exited: %v", err)
    }
}
```

#### 第二步：Python 侧 - 实现 HTTP Server

Python 进程需实现两个 HTTP 端点：

```python
"""
my_plugin.py - Python 插件 HTTP Server
"""
import json
from http.server import HTTPServer, BaseHTTPRequestHandler


class PluginHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        """GET /health - 健康检查"""
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "ok"}).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        """POST /on-trigger - 接收触发事件"""
        if self.path != "/on-trigger":
            self.send_response(404)
            self.end_headers()
            return

        # 1. 解析触发事件
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)
        event = json.loads(body)

        trigger_name = event.get("name", "")      # 触发器名称
        trigger_type = event.get("type", "")      # "timer" | "nats"
        metadata     = event.get("metadata", {})   # {"nodeID": "...", "version": "..."}

        # 2. 解析 payload（包含 TaskStore 快照）
        payload = event.get("payload", {})
        if isinstance(payload, str):
            payload = json.loads(payload)

        tasks = payload.get("tasks", [])           # 任务实例列表
        tasks_md5 = payload.get("tasks_md5", "")   # 任务列表 MD5

        # 3. 根据触发器名称执行不同逻辑
        task_results = []
        if trigger_name == "my-timer":
            task_results = self.handle_timer(tasks)
        elif trigger_name == "my-queue":
            task_results = self.handle_message(event.get("payload"))

        # 4. 返回执行结果
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        response = {"task_results": task_results}
        self.wfile.write(json.dumps(response).encode())

    def handle_timer(self, tasks):
        """处理定时触发"""
        results = []
        for task in tasks:
            task_id = task.get("task_id", "")
            params = json.loads(task.get("task_params", "{}"))

            try:
                # ... 执行业务逻辑 ...
                results.append({
                    "task_id": task_id,
                    "status": 2,     # 2=成功
                    "result": "",
                })
            except Exception as e:
                results.append({
                    "task_id": task_id,
                    "status": 4,     # 4=失败
                    "result": str(e),
                })
        return results

    def handle_message(self, payload):
        """处理 NATS 消息"""
        # payload 为 NATS msg.Data() 的内容
        # ... 业务逻辑 ...
        return []


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 9001), PluginHandler)
    print("Plugin server running on port 9001")
    server.serve_forever()
```

#### 第三步：启动脚本

```bash
#!/bin/bash
# scf_bootstrap - SCF Web 函数启动脚本
export PORT=9000

# 后台启动 Python 插件进程
python3 -u ./plugin/my_plugin.py &

# 前台启动 Go 主进程
./main --conf ./configs/trpc_go.yaml
```

#### 第四步：构建部署包

```
my-function.zip
├── main                    # Go 二进制（GOOS=linux GOARCH=amd64）
├── scf_bootstrap           # 启动脚本
├── configs/
│   ├── config.yaml         # 框架配置
│   └── trpc_go.yaml        # TRPC 配置
└── plugin/
    ├── my_plugin.py        # Python 插件
    └── scf_log/            # （可选）CLS 日志模块
```

### 7.3 TriggerEvent 数据结构

```json
{
    "type": "timer",
    "name": "scheduled-collect",
    "payload": {                       // JSON bytes，TriggerManager 注入
        "tasks": [                     // TaskInstance 列表
            {
                "id": 1,
                "task_id": "task-001",
                "rule_id": "rule-001",
                "planned_exec_node": "node-abc",
                "task_params": "{\"data_source\":\"binance\",\"symbol\":\"BTC-USDT\"}",
                "invalid": 0
            }
        ],
        "tasks_md5": "a1b2c3d4..."
    },
    "metadata": {
        "nodeID": "node-abc",
        "version": "v1.0.0",
        "granularity": "minute",
        "fire_time": "2025-01-01T00:01:00Z"
    }
}
```

### 7.4 TriggerResponse 数据结构

```json
{
    "task_results": [
        {
            "task_id": "task-001",
            "status": 2,              // 2=成功, 4=失败
            "result": ""              // 失败时为错误原因
        }
    ]
}
```

---

## 八、关键设计决策

### 8.1 为什么使用 HTTP 桥接而不是 gRPC/IPC？

- **简单性**：HTTP + JSON 是最通用的协议，任何语言都有成熟的 HTTP Server 库
- **调试友好**：可用 curl 直接测试插件接口
- **性能足够**：localhost 通信延迟在微秒级，对于秒级/分钟级触发场景不构成瓶颈
- **隔离性**：Go 和 Python 进程完全独立，故障互不影响

### 8.2 为什么任务列表用 MD5 检测变更？

- **带宽优化**：每 9 秒的心跳只需携带一个 32 字符的 MD5，而非完整任务列表
- **服务端友好**：服务端只需比较 MD5 即可判断是否需要下发新任务
- **幂等性**：相同任务集合始终产生相同 MD5

### 8.3 为什么 TriggerManager 要克隆 context？

```go
ctx = trpc.CloneContext(ctx)
```

TRPC Timer 的 context 带有超时限制。如果插件执行时间超过 Timer 超时（例如 HTTP 请求到外部 API），context 会被取消导致插件执行中断。`CloneContext` 创建了一个保留 TRPC metadata 但不继承 deadline 的新 context。

---

## 九、Python CLS 日志模块

**目录**: `python/scf_log/`

框架提供了 Python 版本的腾讯云 CLS（Cloud Log Service）日志集成模块，方便 Python 插件将日志写入 CLS。

```python
from scf_log import setup_cls_logging

handler = setup_cls_logging(
    config_path="./configs/config.yaml",  # 从 YAML plugin.cls 节点读取配置
    level=logging.INFO,
    logger_name="my-plugin",
)

# 设置上下文字段（推荐在收到第一个触发事件后设置）
handler.set_context_fields(nodeID="node-abc", version="v1.0.0")

# 之后所有 logging 调用自动上报 CLS
logger.info("采集完成")
```
