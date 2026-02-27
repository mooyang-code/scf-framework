package trigger

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mooyang-code/scf-framework/config"
	"github.com/mooyang-code/scf-framework/model"
	"github.com/mooyang-code/scf-framework/plugin"
	"trpc.group/trpc-go/trpc-go/log"
)

// Manager 管理所有触发器的生命周期
type Manager struct {
	triggers  []Trigger
	plugin    plugin.Plugin
	timer     *TimerTrigger
	taskStore *config.TaskInstanceStore
}

// NewManager 创建触发器管理器
func NewManager(p plugin.Plugin, ts *config.TaskInstanceStore) *Manager {
	return &Manager{
		plugin:    p,
		timer:     NewTimerTrigger(),
		taskStore: ts,
	}
}

// Init 根据配置创建并初始化触发器实例
func (m *Manager) Init(ctx context.Context, configs []model.TriggerConfig) error {
	handler := m.wrapHandler()

	for _, cfg := range configs {
		switch cfg.Type {
		case string(model.TriggerTimer):
			cronExpr, _ := cfg.Settings["cron"].(string)
			if cronExpr == "" {
				return fmt.Errorf("timer trigger %q missing cron setting", cfg.Name)
			}
			if err := m.timer.AddCron(cfg.Name, cronExpr, handler); err != nil {
				return fmt.Errorf("failed to add cron %q: %w", cfg.Name, err)
			}
			log.InfoContextf(ctx, "[TriggerManager] registered timer trigger: name=%s, cron=%s", cfg.Name, cronExpr)

		case string(model.TriggerNATS):
			t := NewNATSTrigger(cfg.Name)
			if err := t.Init(ctx, cfg); err != nil {
				return fmt.Errorf("failed to init NATS trigger %q: %w", cfg.Name, err)
			}
			m.triggers = append(m.triggers, t)
			log.InfoContextf(ctx, "[TriggerManager] registered NATS trigger: name=%s", cfg.Name)

		default:
			return fmt.Errorf("unknown trigger type %q for trigger %q", cfg.Type, cfg.Name)
		}
	}
	return nil
}

// StartAll 启动所有触发器
func (m *Manager) StartAll(ctx context.Context) error {
	handler := m.wrapHandler()

	for _, t := range m.triggers {
		if err := t.Start(ctx, handler); err != nil {
			return fmt.Errorf("failed to start trigger %q: %w", t.Name(), err)
		}
		log.InfoContextf(ctx, "[TriggerManager] started trigger: name=%s, type=%s", t.Name(), t.Type())
	}
	return nil
}

// StopAll 停止所有触发器
func (m *Manager) StopAll(ctx context.Context) {
	for _, t := range m.triggers {
		if err := t.Stop(ctx); err != nil {
			log.ErrorContextf(ctx, "[TriggerManager] failed to stop trigger %q: %v", t.Name(), err)
		}
	}
}

// Timer 返回内部的 TimerTrigger，供 TRPC Timer handler 调用 Tick
func (m *Manager) Timer() *TimerTrigger {
	return m.timer
}

// wrapHandler 包装 plugin.OnTrigger 并注入结构化日志字段和 TaskStore 快照
func (m *Manager) wrapHandler() TriggerHandler {
	return func(ctx context.Context, event *model.TriggerEvent) error {
		ctx = log.WithContextFields(ctx,
			"plugin", m.plugin.Name(),
			"trigger", event.Name,
			"trigger_type", string(event.Type),
		)

		// 将 TaskStore 快照注入 TriggerEvent.Payload（供 HTTPPluginAdapter 插件使用）
		if m.taskStore != nil && len(event.Payload) == 0 {
			tasks := m.taskStore.GetAll()
			if len(tasks) > 0 {
				snapshot := &TriggerPayload{
					Tasks:    tasks,
					TasksMD5: m.taskStore.GetCurrentMD5(),
				}
				if data, err := json.Marshal(snapshot); err == nil {
					event.Payload = data
				}
			}
		}

		return m.plugin.OnTrigger(ctx, event)
	}
}

// TriggerPayload 触发器事件携带的负载数据
type TriggerPayload struct {
	Tasks    []*model.TaskInstance `json:"tasks"`
	TasksMD5 string                `json:"tasks_md5"`
}
