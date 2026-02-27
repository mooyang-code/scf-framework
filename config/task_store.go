package config

import (
	"crypto/md5"
	"encoding/hex"
	"sort"
	"strings"
	"sync"

	"github.com/mooyang-code/scf-framework/model"
	cmap "github.com/orcaman/concurrent-map/v2"
)

// TaskInstanceStore 任务实例内存缓存
type TaskInstanceStore struct {
	store cmap.ConcurrentMap[string, *model.TaskInstance]
	md5   string
	mu    sync.RWMutex
}

// NewTaskInstanceStore 创建新的任务实例存储
func NewTaskInstanceStore() *TaskInstanceStore {
	return &TaskInstanceStore{
		store: cmap.New[*model.TaskInstance](),
		md5:   "empty",
	}
}

// UpdateTaskInstances 清空并重新填充任务实例，计算 MD5
func (s *TaskInstanceStore) UpdateTaskInstances(tasks []*model.TaskInstance) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store.Clear()
	for _, task := range tasks {
		if task != nil && task.TaskID != "" {
			s.store.Set(task.TaskID, task)
		}
	}

	s.md5 = calculateMD5(tasks)
}

// GetByNode 根据节点ID获取任务实例列表
func (s *TaskInstanceStore) GetByNode(nodeID string) []*model.TaskInstance {
	if nodeID == "" {
		return nil
	}

	var result []*model.TaskInstance
	s.store.IterCb(func(_ string, task *model.TaskInstance) {
		if task.NodeID == nodeID && task.Invalid == 0 {
			result = append(result, task)
		}
	})
	return result
}

// GetAll 获取所有任务实例
func (s *TaskInstanceStore) GetAll() []*model.TaskInstance {
	var result []*model.TaskInstance
	s.store.IterCb(func(_ string, task *model.TaskInstance) {
		result = append(result, task)
	})
	return result
}

// GetCurrentMD5 获取当前任务列表的 MD5 值
func (s *TaskInstanceStore) GetCurrentMD5() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.md5
}

// calculateMD5 计算任务列表的 MD5 值
func calculateMD5(tasks []*model.TaskInstance) string {
	if len(tasks) == 0 {
		return "empty"
	}

	taskIDs := make([]string, 0, len(tasks))
	for _, task := range tasks {
		if task.Invalid == 0 {
			taskIDs = append(taskIDs, task.TaskID)
		}
	}

	if len(taskIDs) == 0 {
		return "empty"
	}

	sort.Strings(taskIDs)
	combined := strings.Join(taskIDs, ",")
	hash := md5.Sum([]byte(combined))
	return hex.EncodeToString(hash[:])
}
