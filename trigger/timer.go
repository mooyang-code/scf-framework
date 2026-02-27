package trigger

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/gorhill/cronexpr"
	"github.com/mooyang-code/scf-framework/model"
	"trpc.group/trpc-go/trpc-go/log"
)

// Granularity 定时器粒度
type Granularity string

const (
	GranularitySecond Granularity = "second"
	GranularityMinute Granularity = "minute"
	GranularityHour   Granularity = "hour"
)

// timerEntry 单个定时器条目
type timerEntry struct {
	name        string
	cronExpr    *cronexpr.Expression
	granularity Granularity
	handler     TriggerHandler
}

// TimerTrigger 基于 TRPC Timer 的定时触发器
type TimerTrigger struct {
	entries []*timerEntry
	mu      sync.RWMutex
}

// NewTimerTrigger 创建 TimerTrigger
func NewTimerTrigger() *TimerTrigger {
	return &TimerTrigger{}
}

// AddCron 解析 cron 表达式，推断粒度，添加定时器条目
func (t *TimerTrigger) AddCron(name, cron string, handler TriggerHandler) error {
	expr, err := cronexpr.Parse(cron)
	if err != nil {
		return err
	}

	granularity := inferGranularity(cron)

	t.mu.Lock()
	defer t.mu.Unlock()

	t.entries = append(t.entries, &timerEntry{
		name:        name,
		cronExpr:    expr,
		granularity: granularity,
		handler:     handler,
	})
	return nil
}

// Tick 遍历匹配此粒度的所有条目，检查 cron 匹配，触发 handler
func (t *TimerTrigger) Tick(ctx context.Context, granularity Granularity) error {
	t.mu.RLock()
	entries := make([]*timerEntry, len(t.entries))
	copy(entries, t.entries)
	t.mu.RUnlock()

	now := time.Now()

	for _, entry := range entries {
		if entry.granularity != granularity {
			continue
		}

		// 检查当前时刻是否匹配 cron 表达式
		nextTime := entry.cronExpr.Next(now.Add(-1 * time.Second))
		if nextTime.After(now) {
			continue
		}

		event := &model.TriggerEvent{
			Type: model.TriggerTimer,
			Name: entry.name,
			Metadata: map[string]string{
				"granularity": string(granularity),
				"fire_time":   now.Format(time.RFC3339),
			},
		}

		if err := entry.handler(ctx, event); err != nil {
			log.ErrorContextf(ctx, "[TimerTrigger] handler error for %q: %v", entry.name, err)
		}
	}
	return nil
}

// HasEntries 返回是否有任何定时器条目
func (t *TimerTrigger) HasEntries() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.entries) > 0
}

// HasGranularity 返回是否有指定粒度的条目
func (t *TimerTrigger) HasGranularity(g Granularity) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, entry := range t.entries {
		if entry.granularity == g {
			return true
		}
	}
	return false
}

// inferGranularity 从 cron 表达式推断粒度
// 秒位含 */ 或 , 或 - → second（真正的秒级调度）
// 秒位为固定数字（如 "0"、"30"）→ 视为分钟级（只是偏移）
// 秒位为固定值且分位含 */ 或 , 或 - 或非 "0" → minute
// 否则 → hour
func inferGranularity(cron string) Granularity {
	parts := strings.Fields(cron)
	if len(parts) < 2 {
		return GranularityMinute
	}

	secField := parts[0]
	minField := parts[1]

	// 秒位含 */、逗号、范围 → 真正的秒级调度
	if strings.ContainsAny(secField, "*/,-") {
		return GranularitySecond
	}
	// 秒位是固定数字（含 "0"），按分位判断
	if minField != "0" && !strings.ContainsAny(minField, "*/,-") {
		return GranularityMinute
	}
	if minField != "0" {
		return GranularityMinute
	}
	return GranularityHour
}
