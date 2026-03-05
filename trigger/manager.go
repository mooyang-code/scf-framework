package trigger

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mooyang-code/scf-framework/config"
	"github.com/mooyang-code/scf-framework/dnsproxy"
	"github.com/mooyang-code/scf-framework/model"
	"github.com/mooyang-code/scf-framework/plugin"
	"github.com/mooyang-code/scf-framework/reporter"
	"github.com/mooyang-code/scf-framework/storage"
	"trpc.group/trpc-go/trpc-go"
	"trpc.group/trpc-go/trpc-go/log"
)

// Manager 管理所有触发器的生命周期
type Manager struct {
	triggers      []Trigger
	plugin        plugin.Plugin
	timer         *TimerTrigger
	taskStore     *config.TaskInstanceStore
	runtime       *config.RuntimeState
	reporter      *reporter.TaskReporter
	dnsResolver   *dnsproxy.Resolver
	storageWriter *storage.RPCWriter
	storageReader *storage.Reader
}

// NewManager 创建触发器管理器
func NewManager(p plugin.Plugin, ts *config.TaskInstanceStore, rs *config.RuntimeState,
	tr *reporter.TaskReporter, dr *dnsproxy.Resolver, sw *storage.RPCWriter, sr *storage.Reader) *Manager {
	return &Manager{
		plugin:        p,
		timer:         NewTimerTrigger(),
		taskStore:     ts,
		runtime:       rs,
		reporter:      tr,
		dnsResolver:   dr,
		storageWriter: sw,
		storageReader: sr,
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
			if m.storageReader != nil {
				t.SetStorageReader(m.storageReader)
			}
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

// wrapHandler 包装 plugin.OnTrigger，注入 metadata/TaskStore 快照，并处理响应
func (m *Manager) wrapHandler() TriggerHandler {
	return func(ctx context.Context, event *model.TriggerEvent) error {
		ctx = trpc.CloneContext(ctx)

		nodeID, version := m.injectMetadata(event)

		ctx = log.WithContextFields(ctx,
			"nodeID", nodeID,
			"version", version,
			"plugin", m.plugin.Name(),
			"trigger", event.Name,
			"trigger_type", string(event.Type),
		)

		if skip := m.injectTaskStore(ctx, event); skip {
			return nil
		}

		log.InfoContextf(ctx, "[TriggerManager] dispatching trigger: name=%s, type=%s",
			event.Name, event.Type)

		resp, err := m.plugin.OnTrigger(ctx, event)
		if err != nil {
			log.ErrorContextf(ctx, "[TriggerManager] trigger %s failed: %v", event.Name, err)
		}

		m.logResponse(ctx, event.Name, resp, err)
		m.reportTaskResults(ctx, resp)
		m.writeResponse(ctx, resp)

		return err
	}
}

// injectMetadata 向 event.Metadata 注入 runtime 信息和 DNS 解析结果，返回 nodeID/version
func (m *Manager) injectMetadata(event *model.TriggerEvent) (nodeID, version string) {
	if event.Metadata == nil {
		event.Metadata = make(map[string]string)
	}

	if m.runtime != nil {
		nodeID, version = m.runtime.GetNodeInfo()
		event.Metadata["nodeID"] = nodeID
		event.Metadata["version"] = version
		event.Metadata["storage_server_url"] = m.runtime.GetStorageServerURL()
	}

	if m.dnsResolver != nil {
		records := m.dnsResolver.GetAllRecords()
		if len(records) > 0 {
			if dnsJSON, err := json.Marshal(records); err == nil {
				event.Metadata["dns_records"] = string(dnsJSON)
			}
		}
	}

	return
}

// injectTaskStore 注入 TaskStore 快照到 event，对 timer 触发器执行调度筛选。
// 返回 true 表示无 jobs 可执行，调用方应跳过后续处理。
func (m *Manager) injectTaskStore(ctx context.Context, event *model.TriggerEvent) (skip bool) {
	if m.taskStore == nil {
		return false
	}

	tasks := m.taskStore.GetAll()
	if tasks == nil {
		tasks = []*model.TaskInstance{}
	}
	tasksMD5 := m.taskStore.GetCurrentMD5()

	event.Tasks = tasks
	event.TasksMD5 = tasksMD5

	// 对 timer 类型触发器执行框架调度筛选
	if event.Type == model.TriggerTimer {
		jobs := FilterTaskJobs(tasks, time.Now().UTC())
		if len(jobs) == 0 {
			log.InfoContextf(ctx, "[TriggerManager] no jobs to execute, skipping trigger %s", event.Name)
			return true
		}
		event.Jobs = jobs
		log.InfoContextf(ctx, "[TriggerManager] scheduled execute: %d jobs for trigger %s", len(jobs), event.Name)

		snapshot := &TriggerPayload{Tasks: tasks, TasksMD5: tasksMD5, Jobs: jobs}
		if data, err := json.Marshal(snapshot); err == nil {
			event.Payload = data
		}
	}
	// NATS 触发器：保留原始 Payload 不覆盖

	log.InfoContextf(ctx, "[TriggerManager] task snapshot injected: tasks=%d, jobs=%d, md5=%s",
		len(tasks), len(event.Jobs), tasksMD5)

	return false
}

// logResponse 记录 OnTrigger 返回摘要
func (m *Manager) logResponse(ctx context.Context, triggerName string, resp *model.TriggerResponse, err error) {
	var taskResults, dataPoints, writeGroups int
	if resp != nil {
		taskResults = len(resp.TaskResults)
		dataPoints = len(resp.DataPoints)
		writeGroups = len(resp.WriteGroups)
	}
	log.InfoContextf(ctx, "[TriggerManager] OnTrigger returned: trigger=%s, hasResp=%v, taskResults=%d, dataPoints=%d, writeGroups=%d, err=%v",
		triggerName, resp != nil, taskResults, dataPoints, writeGroups, err)
}

// reportTaskResults 异步上报任务执行结果
func (m *Manager) reportTaskResults(ctx context.Context, resp *model.TriggerResponse) {
	if resp == nil || len(resp.TaskResults) == 0 || m.reporter == nil {
		return
	}
	log.InfoContextf(ctx, "[TriggerManager] dispatching %d task results to reporter", len(resp.TaskResults))
	for _, tr := range resp.TaskResults {
		log.InfoContextf(ctx, "[TriggerManager] reporting task result: taskID=%s, status=%d, result=%q",
			tr.TaskID, tr.Status, tr.Result)
		m.reporter.ReportAsync(ctx, tr.TaskID, tr.Status, tr.Result)
	}
}

// writeResponse 将 DataPoints 和 WriteGroups 写入 xData
func (m *Manager) writeResponse(ctx context.Context, resp *model.TriggerResponse) {
	if resp == nil || m.storageWriter == nil {
		return
	}

	// 全局 DataPoints（使用默认 storage config）
	if len(resp.DataPoints) > 0 {
		log.InfoContextf(ctx, "[TriggerManager] writing %d data points to xData (global config)",
			len(resp.DataPoints))
		if err := m.storageWriter.SetData(ctx, resp.DataPoints, nil); err != nil {
			log.ErrorContextf(ctx, "[TriggerManager] failed to write data points: %v", err)
		}
	}

	// 按 WriteGroups 分组写入
	for i, wg := range resp.WriteGroups {
		if len(wg.DataPoints) == 0 {
			continue
		}
		override := &storage.WriteOverride{
			WriteMode: wg.WriteMode,
			DatasetID: wg.DatasetID,
			Freq:      wg.Freq,
			AppKey:    wg.AppKey,
		}
		log.InfoContextf(ctx, "[TriggerManager] writing WriteGroup[%d]: mode=%s, points=%d",
			i, wg.WriteMode, len(wg.DataPoints))

		var err error
		switch wg.WriteMode {
		case "upsert_object":
			err = m.storageWriter.UpsertObject(ctx, wg.DataPoints, override)
		default:
			err = m.storageWriter.SetData(ctx, wg.DataPoints, override)
		}
		if err != nil {
			log.ErrorContextf(ctx, "[TriggerManager] failed to write WriteGroup[%d]: %v", i, err)
		}
	}
}

// TriggerPayload 触发器事件携带的负载数据
type TriggerPayload struct {
	Tasks    []*model.TaskInstance `json:"tasks"`
	TasksMD5 string                `json:"tasks_md5"`
	Jobs     []model.TaskJob       `json:"jobs,omitempty"`
}
