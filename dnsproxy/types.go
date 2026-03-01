package dnsproxy

import "time"

// IPInfo 单个 IP 的信息
type IPInfo struct {
	IP        string    `json:"ip"`
	Latency   int64     `json:"latency"`   // 微秒
	Available bool      `json:"available"`
	LastPing  time.Time `json:"last_ping"`
}

// DNSRecord DNS 解析记录
type DNSRecord struct {
	Domain    string    `json:"domain"`
	IPList    []*IPInfo `json:"ip_list"`    // 已排序：可用优先，延迟低优先
	ResolveAt time.Time `json:"resolve_at"`
	Success   bool      `json:"success"`
}

// DNSReportItem 心跳上报格式
type DNSReportItem struct {
	Domain    string    `json:"domain"`
	IPList    []string  `json:"ip_list"`    // 仅可用 IP
	ResolveAt time.Time `json:"resolve_at"`
}
