package trigger

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/mooyang-code/scf-framework/model"
)

// ShouldExecute 判断当前时刻是否应执行指定周期的任务。
// 支持: 1m, 5m, 1h, 2h, 1d, 1w, 1M 等。
func ShouldExecute(interval string, now time.Time) bool {
	value, unit, ok := parseInterval(interval)
	if !ok {
		return false
	}

	minute := now.Minute()
	hour := now.Hour()

	switch unit {
	case 'm':
		return minute%value == 0
	case 'h':
		return minute == 0 && hour%value == 0
	case 'd':
		if minute != 0 || hour != 0 {
			return false
		}
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		days := int(now.Sub(epoch).Hours() / 24)
		return days%value == 0
	case 'w':
		if minute != 0 || hour != 0 || now.Weekday() != time.Monday {
			return false
		}
		// 1970-01-05 是第一个周一
		epoch := time.Date(1970, 1, 5, 0, 0, 0, 0, time.UTC)
		weeks := int(now.Sub(epoch).Hours() / 24 / 7)
		return weeks%value == 0
	case 'M':
		return minute == 0 && hour == 0 && now.Day() == 1 && int(now.Month())%value == 0
	default:
		return false
	}
}

// FilterTaskJobs 从任务列表中筛选出当前时刻需要执行的 jobs。
// 1. 跳过 Invalid != 0
// 2. 解析 task_params JSON 提取 "intervals" 数组
// 3. 对每个 interval 调用 ShouldExecute
func FilterTaskJobs(tasks []*model.TaskInstance, now time.Time) []model.TaskJob {
	var jobs []model.TaskJob
	for _, task := range tasks {
		if task.Invalid != 0 {
			continue
		}
		intervals := taskParamsIntervals(task.TaskParams)
		for _, iv := range intervals {
			if ShouldExecute(iv, now) {
				jobs = append(jobs, model.TaskJob{
					Task:     task,
					Interval: iv,
				})
			}
		}
	}
	return jobs
}

// parseInterval 解析周期字符串 "5m" → (5, 'm', true)
func parseInterval(s string) (value int, unit byte, ok bool) {
	if len(s) < 2 {
		return 0, 0, false
	}
	unit = s[len(s)-1]
	v, err := strconv.Atoi(s[:len(s)-1])
	if err != nil || v <= 0 {
		return 0, 0, false
	}
	return v, unit, true
}

// taskParamsIntervals 从 task_params JSON 中提取 "intervals" 字段
func taskParamsIntervals(taskParams string) []string {
	if taskParams == "" {
		return nil
	}
	var parsed struct {
		Intervals []string `json:"intervals"`
	}
	if err := json.Unmarshal([]byte(taskParams), &parsed); err != nil {
		return nil
	}
	return parsed.Intervals
}
