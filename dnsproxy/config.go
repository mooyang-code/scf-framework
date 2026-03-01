package dnsproxy

// Config DNS 代理配置（对应 config.yaml 中的 dns_proxy 节点）
type Config struct {
	DNSServers       []string      `yaml:"dns_servers"`
	DNSTimeout       int           `yaml:"dns_timeout"`        // 秒，默认 5
	ScheduledDomains []string      `yaml:"scheduled_domains"`
	ProbeConfigs     []ProbeConfig `yaml:"probe_configs"`
}

// ProbeConfig IP 探测配置
type ProbeConfig struct {
	Domain    string          `yaml:"domain"`
	ProbeType string          `yaml:"probe_type"` // tcp | https
	ProbeAPI  *ProbeAPIConfig `yaml:"probe_api"`
	TCPPort   int             `yaml:"tcp_port"`  // 默认 443
	Timeout   int             `yaml:"timeout"`   // 秒，默认 2
}

// ProbeAPIConfig HTTPS 业务探测配置
type ProbeAPIConfig struct {
	Path           string `yaml:"path"`
	Method         string `yaml:"method"`
	Timeout        int    `yaml:"timeout"`
	ExpectedStatus int    `yaml:"expected_status"`
}
