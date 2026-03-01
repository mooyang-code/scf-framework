package dnsproxy

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/avast/retry-go"
	"trpc.group/trpc-go/trpc-go"
	"trpc.group/trpc-go/trpc-go/log"
)

// Resolver DNS 解析器
type Resolver struct {
	cfg             *Config
	mu              sync.RWMutex
	records         map[string]*DNSRecord
	mooxServerURLFn func() string // 获取 Moox Server URL（用于远端降级获取）
}

// NewResolver 创建 DNS 解析器
func NewResolver(cfg *Config, mooxServerURLFn func() string) *Resolver {
	return &Resolver{
		cfg:             cfg,
		records:         make(map[string]*DNSRecord),
		mooxServerURLFn: mooxServerURLFn,
	}
}

// ScheduledResolve TRPC Timer 入口函数（同 heartbeat.ScheduledHeartbeat 模式）
func (r *Resolver) ScheduledResolve(c context.Context, _ string) error {
	ctx := trpc.CloneContext(c)
	log.DebugContext(ctx, "ScheduledResolve Enter")
	if err := r.Resolve(ctx); err != nil {
		log.ErrorContextf(ctx, "scheduled resolve DNS failed: %v", err)
		return err
	}
	log.DebugContext(ctx, "ScheduledResolve Success")
	return nil
}

// Resolve 执行一轮完整 DNS 解析
func (r *Resolver) Resolve(ctx context.Context) error {
	domains := r.cfg.ScheduledDomains
	if len(domains) == 0 {
		log.DebugContext(ctx, "no domains configured for DNS resolution")
		return nil
	}

	log.InfoContextf(ctx, "[DNSProxy] 开始解析 %d 个域名", len(domains))

	allRecords := make([]*DNSRecord, 0, len(domains))
	for _, domain := range domains {
		record := r.resolveSingleDomain(ctx, domain)
		if record != nil {
			allRecords = append(allRecords, record)
		}
	}

	log.InfoContextf(ctx, "[DNSProxy] 解析完成: 总数=%d", len(allRecords))

	r.mu.Lock()
	r.records = make(map[string]*DNSRecord, len(allRecords))
	for _, record := range allRecords {
		r.records[record.Domain] = record
	}
	r.mu.Unlock()

	log.DebugContextf(ctx, "DNS records updated successfully, total: %d", len(allRecords))
	return nil
}

// GetBestIP 获取指定域名的最优 IP（第一个可用 IP）
func (r *Resolver) GetBestIP(domain string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	record, exists := r.records[domain]
	if !exists || record == nil || len(record.IPList) == 0 {
		return ""
	}
	for _, ipInfo := range record.IPList {
		if ipInfo.Available {
			return ipInfo.IP
		}
	}
	return ""
}

// GetAvailableIPs 获取指定域名所有可用的 IP 列表
func (r *Resolver) GetAvailableIPs(domain string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	record, exists := r.records[domain]
	if !exists || record == nil || len(record.IPList) == 0 {
		return nil
	}

	var ips []string
	for _, ipInfo := range record.IPList {
		if ipInfo.Available {
			ips = append(ips, ipInfo.IP)
		}
	}
	return ips
}

// GetAllRecords 获取全部 DNS 记录
func (r *Resolver) GetAllRecords() map[string]*DNSRecord {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[string]*DNSRecord, len(r.records))
	for domain, record := range r.records {
		result[domain] = record
	}
	return result
}

// GetDNSReportItems 获取心跳上报格式的 DNS 记录
func (r *Resolver) GetDNSReportItems() []DNSReportItem {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var items []DNSReportItem
	for domain, record := range r.records {
		var availableIPs []string
		for _, ipInfo := range record.IPList {
			if ipInfo.Available {
				availableIPs = append(availableIPs, ipInfo.IP)
			}
		}
		if len(availableIPs) > 0 {
			items = append(items, DNSReportItem{
				Domain:    domain,
				IPList:    availableIPs,
				ResolveAt: record.ResolveAt,
			})
		}
	}
	return items
}

// resolveSingleDomain 解析单个域名（先本地解析，失败则远端获取）
func (r *Resolver) resolveSingleDomain(ctx context.Context, domain string) *DNSRecord {
	// 1. 尝试本地解析
	log.DebugContextf(ctx, "[DNSProxy] 开始本地解析域名: %s", domain)
	localRecord, err := r.localResolveDomain(ctx, domain)
	if err == nil && localRecord != nil && localRecord.Success {
		log.InfoContextf(ctx, "[DNSProxy] 本地解析成功: domain=%s", domain)
		return localRecord
	}

	// 2. 本地解析失败，降级到远端获取
	log.WarnContextf(ctx, "[DNSProxy] 本地解析失败，降级到远端获取: domain=%s, error=%v", domain, err)
	remoteRecord := r.fetchSingleDomainFromRemote(ctx, domain)
	if remoteRecord != nil {
		log.InfoContextf(ctx, "[DNSProxy] 远端获取成功: domain=%s", domain)
		return remoteRecord
	}

	// 3. 远端获取也失败
	log.ErrorContextf(ctx, "[DNSProxy] 域名解析完全失败（本地+远端）: domain=%s", domain)
	return nil
}

// localResolveDomain 本地解析单个域名
func (r *Resolver) localResolveDomain(ctx context.Context, domain string) (*DNSRecord, error) {
	result := &DNSRecord{
		Domain:    domain,
		ResolveAt: time.Now(),
		Success:   false,
	}

	// 1. 获取 DNS 服务器列表和超时配置
	dnsServers, dnsTimeout := r.getLocalDNSConfig()
	if len(dnsServers) == 0 {
		return result, fmt.Errorf("no DNS servers configured")
	}

	// 2. 使用多个 DNS 服务器并发解析
	ips, err := r.resolveWithMultipleDNS(ctx, domain, dnsServers, dnsTimeout)
	if err != nil {
		log.ErrorContextf(ctx, "[DNSProxy] 本地DNS解析失败: domain=%s, error=%v", domain, err)
		return result, err
	}

	if len(ips) == 0 {
		return result, fmt.Errorf("domain %s has no IP addresses resolved", domain)
	}

	log.InfoContextf(ctx, "[DNSProxy] 本地DNS解析成功: domain=%s, 解析到 %d 个 IP", domain, len(ips))

	// 3. 对解析出的 IP 列表进行探测和排序
	ipInfoList := r.probeAndSort(ctx, domain, ips)

	// 4. 统计可用 IP 数量
	availableCount := 0
	for _, ipInfo := range ipInfoList {
		if ipInfo.Available {
			availableCount++
		}
	}

	log.InfoContextf(ctx, "[DNSProxy] 本地DNS解析 - domain=%s, 可用IP: %d/%d", domain, availableCount, len(ips))

	result.IPList = ipInfoList
	result.Success = true
	return result, nil
}

// resolveWithMultipleDNS 使用多个 DNS 服务器并发解析域名
func (r *Resolver) resolveWithMultipleDNS(ctx context.Context, domain string, dnsServers []string, timeout time.Duration) ([]string, error) {
	ipSet := &sync.Map{}

	var handlers []func() error
	for _, dnsServer := range dnsServers {
		dnsServerCopy := dnsServer
		handlers = append(handlers, func() error {
			return r.resolveWithDNS(ctx, domain, dnsServerCopy, timeout, ipSet)
		})
	}

	if err := trpc.GoAndWait(handlers...); err != nil {
		log.WarnContextf(ctx, "[DNSProxy] 并发DNS解析过程中出现错误: %v", err)
	}

	allIPs := convertMapToSlice(ipSet)
	if len(allIPs) == 0 {
		return nil, fmt.Errorf("all DNS servers failed to resolve domain: %s", domain)
	}

	return allIPs, nil
}

// resolveWithDNS 使用指定 DNS 服务器解析域名并存储结果
func (r *Resolver) resolveWithDNS(ctx context.Context, domain, dnsServer string, timeout time.Duration, ipSet *sync.Map) error {
	resolver := createResolver(dnsServer, timeout)

	ips, err := resolver.LookupIPAddr(ctx, domain)
	if err != nil {
		log.WarnContextf(ctx, "[DNSProxy] DNS服务器 %s 解析域名 %s 失败: %v", dnsServer, domain, err)
		return nil
	}

	for _, ip := range ips {
		ipStr := ip.IP.String()
		ipSet.LoadOrStore(ipStr, struct{}{})
	}

	log.DebugContextf(ctx, "[DNSProxy] DNS服务器 %s 解析域名 %s 成功，得到 %d 个 IP", dnsServer, domain, len(ips))
	return nil
}

// probeAndSort 对 IP 列表进行探测并排序
func (r *Resolver) probeAndSort(ctx context.Context, domain string, ips []string) []*IPInfo {
	var ipInfoList []*IPInfo
	var mu sync.Mutex

	probeConfig := r.getProbeConfig(domain)

	var handlers []func() error
	for _, ip := range ips {
		ipAddr := ip
		handlers = append(handlers, func() error {
			latency, available := r.probeIP(ctx, ipAddr, domain, probeConfig)
			ipInfo := &IPInfo{
				IP:        ipAddr,
				Latency:   latency,
				Available: available,
				LastPing:  time.Now(),
			}

			mu.Lock()
			ipInfoList = append(ipInfoList, ipInfo)
			mu.Unlock()
			return nil
		})
	}
	_ = trpc.GoAndWait(handlers...)

	sort.Slice(ipInfoList, func(i, j int) bool {
		if ipInfoList[i].Available != ipInfoList[j].Available {
			return ipInfoList[i].Available
		}
		return ipInfoList[i].Latency < ipInfoList[j].Latency
	})

	return ipInfoList
}

// probeIP 统一探测入口
func (r *Resolver) probeIP(ctx context.Context, ip, domain string, cfg *ProbeConfig) (latency int64, available bool) {
	if cfg == nil || cfg.ProbeType == "tcp" {
		port := 443
		timeout := 2 * time.Second

		if cfg != nil {
			if cfg.TCPPort > 0 {
				port = cfg.TCPPort
			}
			if cfg.Timeout > 0 {
				timeout = time.Duration(cfg.Timeout) * time.Second
			}
		}

		latency, available := probeTCP(ctx, ip, port, timeout)
		if available {
			log.DebugContextf(ctx, "[DNSProxy] 探测 %s - IP: %s, 类型: TCP(%d), 延迟: %dμs, 状态: 可用",
				domain, ip, port, latency)
		} else {
			log.DebugContextf(ctx, "[DNSProxy] 探测 %s - IP: %s, 类型: TCP(%d), 延迟: 0μs, 状态: 不可用",
				domain, ip, port)
		}
		return latency, available
	}

	if cfg.ProbeType == "https" && cfg.ProbeAPI != nil {
		latency, available := probeHTTPS(ctx, ip, domain, cfg.ProbeAPI)
		if available {
			log.DebugContextf(ctx, "[DNSProxy] 探测 %s - IP: %s, 类型: HTTPS(%s), 延迟: %dμs, 状态: 可用",
				domain, ip, cfg.ProbeAPI.Path, latency)
		} else {
			log.DebugContextf(ctx, "[DNSProxy] 探测 %s - IP: %s, 类型: HTTPS(%s), 延迟: 0μs, 状态: 不可用",
				domain, ip, cfg.ProbeAPI.Path)
		}
		return latency, available
	}

	log.WarnContextf(ctx, "[DNSProxy] 未知探测类型 %s，降级到 TCP 探测", cfg.ProbeType)
	return probeTCP(ctx, ip, 443, 2*time.Second)
}

// probeHTTPS 使用业务 API 进行 HTTPS 探测
func probeHTTPS(ctx context.Context, ip, domain string, cfg *ProbeAPIConfig) (latency int64, available bool) {
	timeout := 3 * time.Second
	if cfg.Timeout > 0 {
		timeout = time.Duration(cfg.Timeout) * time.Second
	}

	method := "GET"
	if cfg.Method != "" {
		method = cfg.Method
	}

	expectedStatus := 200
	if cfg.ExpectedStatus > 0 {
		expectedStatus = cfg.ExpectedStatus
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()

	fullURL := fmt.Sprintf("https://%s%s", domain, cfg.Path)

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			_, port, err := net.SplitHostPort(addr)
			if err != nil {
				port = "443"
			}
			targetAddr := net.JoinHostPort(ip, port)
			dialer := &net.Dialer{Timeout: timeout}
			return dialer.DialContext(ctx, network, targetAddr)
		},
	}

	client := &http.Client{
		Timeout:   timeout,
		Transport: transport,
	}

	req, err := http.NewRequestWithContext(timeoutCtx, method, fullURL, nil)
	if err != nil {
		log.WarnContextf(ctx, "[DNSProxy] 创建 HTTPS 探测请求失败: %v", err)
		return 0, false
	}
	req.Header.Set("User-Agent", "scf-dnsproxy/1.0")

	resp, err := client.Do(req)
	if err != nil {
		log.DebugContextf(ctx, "[DNSProxy] HTTPS 探测请求失败 (IP: %s, URL: %s): %v", ip, fullURL, err)
		return 0, false
	}
	defer resp.Body.Close()

	latency = time.Since(start).Microseconds()

	if resp.StatusCode != expectedStatus {
		log.DebugContextf(ctx, "[DNSProxy] HTTPS 探测状态码不匹配 (IP: %s, 期望: %d, 实际: %d)",
			ip, expectedStatus, resp.StatusCode)
		return 0, false
	}

	return latency, true
}

// probeTCP 使用 TCP 连接测试
func probeTCP(ctx context.Context, ip string, port int, timeout time.Duration) (latency int64, available bool) {
	start := time.Now()
	address := fmt.Sprintf("%s:%d", ip, port)

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(timeoutCtx, "tcp", address)
	if err != nil {
		return 0, false
	}
	defer conn.Close()

	latency = time.Since(start).Microseconds()
	return latency, true
}

// getProbeConfig 根据域名查找探测配置
func (r *Resolver) getProbeConfig(domain string) *ProbeConfig {
	if r.cfg == nil {
		return nil
	}
	for i := range r.cfg.ProbeConfigs {
		cfg := &r.cfg.ProbeConfigs[i]
		if cfg.Domain == domain {
			return cfg
		}
	}
	return nil
}

// getLocalDNSConfig 获取本地 DNS 解析配置
func (r *Resolver) getLocalDNSConfig() (dnsServers []string, dnsTimeout time.Duration) {
	if r.cfg == nil {
		return []string{"localhost"}, 5 * time.Second
	}

	dnsServers = r.cfg.DNSServers
	if len(dnsServers) == 0 {
		dnsServers = []string{"localhost"}
	}

	timeoutSec := r.cfg.DNSTimeout
	if timeoutSec <= 0 {
		timeoutSec = 5
	}
	dnsTimeout = time.Duration(timeoutSec) * time.Second

	return dnsServers, dnsTimeout
}

// ============================================================================
// 远端降级获取
// ============================================================================

// serverResponse 服务端响应结构
type serverResponse struct {
	Code    int               `json:"code"`
	Message string            `json:"message"`
	Data    []serverDNSRecord `json:"data"`
	Total   int               `json:"total"`
}

// serverDNSRecord 服务端 DNS 记录
type serverDNSRecord struct {
	Domain    string    `json:"domain"`
	BestIPs   string    `json:"best_ips"` // "1.2.3.4+5.6.7.8"
	ResolveAt time.Time `json:"resolve_at"`
	Success   bool      `json:"success"`
}

// fetchSingleDomainFromRemote 从远端获取单个域名的 DNS 记录
func (r *Resolver) fetchSingleDomainFromRemote(ctx context.Context, domain string) *DNSRecord {
	if r.mooxServerURLFn == nil {
		return nil
	}
	mooxServerURL := r.mooxServerURLFn()
	if mooxServerURL == "" {
		log.DebugContext(ctx, "no moox server URL configured, skipping remote DNS fetch")
		return nil
	}

	url := mooxServerURL + "/gateway/dnsproxy/GetDNSRecordList"

	respData, err := fetchFromServer(ctx, url)
	if err != nil {
		log.ErrorContextf(ctx, "[DNSProxy] 远端获取失败: %v", err)
		return nil
	}

	serverRecords, err := parseServerResponse(respData)
	if err != nil {
		log.ErrorContextf(ctx, "[DNSProxy] 解析远端响应失败: %v", err)
		return nil
	}

	for _, srvRecord := range serverRecords {
		if srvRecord.Domain == domain {
			ips := parseBestIPs(srvRecord.BestIPs)
			log.DebugContextf(ctx, "[DNSProxy] 远端返回域名 %s 的 %d 个 IP", domain, len(ips))

			ipList := r.probeAndSort(ctx, domain, ips)

			availableCount := 0
			for _, ip := range ipList {
				if ip.Available {
					availableCount++
				}
			}
			log.DebugContextf(ctx, "[DNSProxy] 远端域名 %s: %d/%d IPs available", domain, availableCount, len(ips))

			return &DNSRecord{
				Domain:    srvRecord.Domain,
				IPList:    ipList,
				ResolveAt: srvRecord.ResolveAt,
				Success:   srvRecord.Success,
			}
		}
	}

	log.WarnContextf(ctx, "[DNSProxy] 远端未返回域名 %s 的记录", domain)
	return nil
}

// fetchFromServer 从服务端获取数据（带重试）
func fetchFromServer(ctx context.Context, url string) ([]byte, error) {
	httpClient := &http.Client{Timeout: 5 * time.Second}

	var respData []byte
	err := retry.Do(
		func() error {
			return sendSingleRequest(ctx, url, httpClient, &respData)
		},
		retry.Attempts(3),
		retry.Delay(1*time.Second),
		retry.DelayType(retry.BackOffDelay),
		retry.LastErrorOnly(true),
		retry.OnRetry(func(n uint, err error) {
			log.WarnContextf(ctx, "retrying DNS fetch request, attempt: %d, error: %v", n+1, err)
		}),
		retry.Context(ctx),
	)
	return respData, err
}

// sendSingleRequest 发送单次 HTTP 请求
func sendSingleRequest(ctx context.Context, url string, httpClient *http.Client, respData *[]byte) error {
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer([]byte("{}")))
	if err != nil {
		return fmt.Errorf("failed to create DNS fetch request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyData, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("DNS fetch request failed with status: %d, response: %s", resp.StatusCode, string(bodyData))
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	*respData = data
	return nil
}

// parseServerResponse 解析服务端响应
func parseServerResponse(respData []byte) ([]serverDNSRecord, error) {
	var serverResp serverResponse
	if err := json.Unmarshal(respData, &serverResp); err != nil {
		return nil, fmt.Errorf("failed to parse server response: %w", err)
	}

	if serverResp.Code != 200 {
		return nil, fmt.Errorf("server returned error code: %d, message: %s", serverResp.Code, serverResp.Message)
	}

	return serverResp.Data, nil
}

// parseBestIPs 解析 best_ips 字符串为 IP 列表
func parseBestIPs(bestIPs string) []string {
	if bestIPs == "" {
		return nil
	}

	parts := strings.Split(bestIPs, "+")
	ips := make([]string, 0, len(parts))
	for _, ip := range parts {
		if trimmed := strings.TrimSpace(ip); trimmed != "" {
			ips = append(ips, trimmed)
		}
	}
	return ips
}

// ============================================================================
// 工具函数
// ============================================================================

// createResolver 根据 DNS 服务器地址创建解析器
func createResolver(dnsServer string, timeout time.Duration) *net.Resolver {
	if dnsServer == "localhost" || dnsServer == "" {
		return &net.Resolver{
			PreferGo: true,
		}
	}
	return &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			dialer := net.Dialer{
				Timeout: timeout,
			}
			return dialer.DialContext(ctx, network, dnsServer+":53")
		},
	}
}

// convertMapToSlice 将 sync.Map 转换为 string slice
func convertMapToSlice(ipSet *sync.Map) []string {
	var allIPs []string
	ipSet.Range(func(key, value interface{}) bool {
		if ipStr, ok := key.(string); ok {
			allIPs = append(allIPs, ipStr)
		}
		return true
	})
	return allIPs
}
