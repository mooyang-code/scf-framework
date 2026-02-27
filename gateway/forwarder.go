package gateway

import (
	"bytes"
	"fmt"
	"io"
	"net/http"

	"trpc.group/trpc-go/trpc-go/log"
)

// Forwarder HTTP 请求转发器
type Forwarder struct {
	targetHost string
	targetPort int
	client     *http.Client
}

// NewForwarder 创建请求转发器
func NewForwarder(host string, port int) *Forwarder {
	return &Forwarder{
		targetHost: host,
		targetPort: port,
		client:     &http.Client{},
	}
}

// ServeHTTP 实现 http.Handler 接口，转发请求到目标地址
func (f *Forwarder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.ErrorContextf(ctx, "读取请求body失败: %v", err)
		http.Error(w, "读取请求失败", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	targetURL := fmt.Sprintf("http://%s:%d%s", f.targetHost, f.targetPort, r.URL.RequestURI())

	log.InfoContextf(ctx, "转发请求: %s %s -> %s", r.Method, r.URL.RequestURI(), targetURL)

	forwardReq, err := http.NewRequestWithContext(ctx, r.Method, targetURL, bytes.NewReader(body))
	if err != nil {
		log.ErrorContextf(ctx, "创建转发请求失败: %v", err)
		http.Error(w, "创建转发请求失败", http.StatusInternalServerError)
		return
	}

	// 复制请求头（排除 Host、Content-Length 等特殊头）
	for key, values := range r.Header {
		if key == "Host" || key == "Content-Length" {
			continue
		}
		for _, value := range values {
			forwardReq.Header.Add(key, value)
		}
	}
	forwardReq.Header.Add("gateway-tag", "forward")

	resp, err := f.client.Do(forwardReq)
	if err != nil {
		log.ErrorContextf(ctx, "转发请求失败: %v", err)
		http.Error(w, fmt.Sprintf("转发请求失败: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		log.ErrorContextf(ctx, "读取响应body失败: %v", err)
		http.Error(w, "读取响应失败", http.StatusInternalServerError)
		return
	}

	log.InfoContextf(ctx, "收到后端响应: status=%d, body_size=%d", resp.StatusCode, len(respBody))

	// 复制响应头
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	w.WriteHeader(resp.StatusCode)
	w.Write(respBody)
}
