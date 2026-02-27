package scf

// Option App 配置选项
type Option func(*options)

type options struct {
	configPath           string
	gatewayServiceName   string
	heartbeatServiceName string
	timerSecondService   string
	timerMinuteService   string
	timerHourService     string
	enableGateway        bool
}

func defaultOptions() *options {
	return &options{
		configPath:           "./config.yaml",
		heartbeatServiceName: "trpc.heartbeat.timer",
		timerSecondService:   "trpc.timer.second",
		timerMinuteService:   "trpc.timer.minute",
		timerHourService:     "trpc.timer.hour",
	}
}

// WithConfigPath 设置配置文件路径
func WithConfigPath(path string) Option {
	return func(o *options) {
		o.configPath = path
	}
}

// WithGatewayService 启用 HTTP Gateway 并指定 TRPC service name
func WithGatewayService(name string) Option {
	return func(o *options) {
		o.gatewayServiceName = name
		o.enableGateway = true
	}
}

// WithHeartbeatService 设置心跳定时器 service name
func WithHeartbeatService(name string) Option {
	return func(o *options) {
		o.heartbeatServiceName = name
	}
}

// WithTimerServices 设置预定义定时器 service name（秒/分/时）
func WithTimerServices(second, minute, hour string) Option {
	return func(o *options) {
		o.timerSecondService = second
		o.timerMinuteService = minute
		o.timerHourService = hour
	}
}
