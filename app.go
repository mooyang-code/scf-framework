package scf

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"github.com/mooyang-code/go-commlib/trpc-database/timer"
	"github.com/mooyang-code/scf-framework/config"
	"github.com/mooyang-code/scf-framework/gateway"
	"github.com/mooyang-code/scf-framework/heartbeat"
	"github.com/mooyang-code/scf-framework/model"
	"github.com/mooyang-code/scf-framework/plugin"
	"github.com/mooyang-code/scf-framework/trigger"
	"trpc.group/trpc-go/trpc-go"
	"trpc.group/trpc-go/trpc-go/log"
)

// App SCF 框架主应用
type App struct {
	opts       *options
	cfg        *config.FrameworkConfig
	runtime    *config.RuntimeState
	taskStore  *config.TaskInstanceStore
	plugin     plugin.Plugin
	triggerMgr *trigger.Manager
	gw         *gateway.Gateway
}

// New 创建 App 实例
func New(p plugin.Plugin, opts ...Option) *App {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}
	return &App{
		opts:   o,
		plugin: p,
	}
}

// Config 返回框架配置（实现 plugin.Framework 接口）
func (a *App) Config() *config.FrameworkConfig {
	return a.cfg
}

// Runtime 返回运行时状态（实现 plugin.Framework 接口）
func (a *App) Runtime() *config.RuntimeState {
	return a.runtime
}

// TaskStore 返回任务实例存储（实现 plugin.Framework 接口）
func (a *App) TaskStore() *config.TaskInstanceStore {
	return a.taskStore
}

// Run 启动应用
func (a *App) Run(ctx context.Context) error {
	// 1. 加载配置
	cfg, err := config.LoadFrameworkConfig(a.opts.configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}
	a.cfg = cfg

	// 2. 创建 TRPC Server
	s := trpc.NewServer()

	// 3. 初始化 RuntimeState
	a.runtime = config.NewRuntimeState(cfg)
	a.runtime.InitNodeIDFromEnv()

	// 4. 初始化 TaskInstanceStore
	a.taskStore = config.NewTaskInstanceStore()

	// 5. 调用 plugin.Init
	if err := a.plugin.Init(ctx, a); err != nil {
		return fmt.Errorf("failed to init plugin %q: %w", a.plugin.Name(), err)
	}
	log.InfoContextf(ctx, "plugin %q initialized", a.plugin.Name())

	// 6. 注册 HTTP Gateway（如启用）
	if a.opts.enableGateway {
		probeHandler := heartbeat.NewProbeHandler(a.runtime, a.plugin)
		a.gw = gateway.NewGateway(probeHandler)

		// HTTPPluginAdapter 模式：设置 catch-all 转发
		if adapter, ok := a.plugin.(*plugin.HTTPPluginAdapter); ok {
			u, err := url.Parse(adapter.BaseURL())
			if err == nil {
				host := u.Hostname()
				port := u.Port()
				portNum := 0
				if port != "" {
					fmt.Sscanf(port, "%d", &portNum)
				}
				if portNum > 0 {
					a.gw.SetPluginHandler(gateway.NewForwarder(host, portNum))
				}
			}
		}

		a.gw.Register(s.Service(a.opts.gatewayServiceName))
		log.InfoContextf(ctx, "gateway registered on service %q", a.opts.gatewayServiceName)
	}

	// 7. 注册心跳 TRPC Timer
	hbReporter := heartbeat.NewReporter(a.runtime, a.taskStore, a.plugin)
	timer.RegisterScheduler("heartbeatSchedule", &timer.DefaultScheduler{})
	timer.RegisterHandlerService(s.Service(a.opts.heartbeatServiceName), hbReporter.ScheduledHeartbeat)
	log.InfoContextf(ctx, "heartbeat timer registered on service %q", a.opts.heartbeatServiceName)

	// 8. 初始化 TriggerManager
	a.triggerMgr = trigger.NewManager(a.plugin, a.taskStore)

	// 将框架配置中的 triggers 转换为 model.TriggerConfig
	triggerConfigs := make([]config.TriggerConfig, len(cfg.Triggers))
	copy(triggerConfigs, cfg.Triggers)

	modelTriggerConfigs := toModelTriggerConfigs(triggerConfigs)
	if err := a.triggerMgr.Init(ctx, modelTriggerConfigs); err != nil {
		return fmt.Errorf("failed to init triggers: %w", err)
	}

	// 9. 注册预定义 Timer（秒/分/时）用于驱动 TimerTrigger
	//    始终注册 scheduler，避免 trpc_go.yaml 中声明了 timer service 但未注册 scheduler 导致 "invalid scheduler" 错误。
	//    Tick 内部会自行判断是否有匹配该粒度的触发器。
	timerTrigger := a.triggerMgr.Timer()

	type timerDef struct {
		schedulerName string
		serviceName   string
		granularity   trigger.Granularity
	}
	timerDefs := []timerDef{
		{"timerSecondSchedule", a.opts.timerSecondService, trigger.GranularitySecond},
		{"timerMinuteSchedule", a.opts.timerMinuteService, trigger.GranularityMinute},
		{"timerHourSchedule", a.opts.timerHourService, trigger.GranularityHour},
	}
	for _, td := range timerDefs {
		svc := s.Service(td.serviceName)
		if svc == nil {
			continue
		}
		g := td.granularity
		timer.RegisterScheduler(td.schedulerName, &timer.DefaultScheduler{})
		timer.RegisterHandlerService(svc, func(c context.Context, _ string) error {
			return timerTrigger.Tick(trpc.CloneContext(c), g)
		})
		log.InfoContextf(ctx, "%s timer registered on service %q", td.granularity, td.serviceName)
	}

	// 10. 启动所有非 Timer 触发器（如 NATS）
	if err := a.triggerMgr.StartAll(ctx); err != nil {
		return fmt.Errorf("failed to start triggers: %w", err)
	}

	// 11. 信号监听
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
		sig := <-sigCh
		log.InfoContextf(ctx, "received signal %v, shutting down...", sig)
		a.triggerMgr.StopAll(ctx)
	}()

	// 12. 启动 TRPC Server（阻塞）
	log.InfoContextf(ctx, "scf-framework started with plugin %q", a.plugin.Name())
	if err := s.Serve(); err != nil {
		return fmt.Errorf("server error: %w", err)
	}

	return nil
}

// toModelTriggerConfigs 将 config.TriggerConfig 转换为 model.TriggerConfig
func toModelTriggerConfigs(cfgs []config.TriggerConfig) []model.TriggerConfig {
	result := make([]model.TriggerConfig, len(cfgs))
	for i, c := range cfgs {
		result[i] = model.TriggerConfig{
			Name:     c.Name,
			Type:     c.Type,
			Settings: c.Settings,
		}
	}
	return result
}
