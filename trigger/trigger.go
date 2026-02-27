package trigger

import (
	"context"

	"github.com/mooyang-code/scf-framework/model"
)

// TriggerHandler 触发事件处理函数
type TriggerHandler func(ctx context.Context, event *model.TriggerEvent) error

// Trigger 触发器接口
type Trigger interface {
	Name() string
	Type() model.TriggerType
	Init(ctx context.Context, cfg model.TriggerConfig) error
	Start(ctx context.Context, handler TriggerHandler) error
	Stop(ctx context.Context) error
}
