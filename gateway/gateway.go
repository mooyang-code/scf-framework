package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/mooyang-code/scf-framework/heartbeat"
	"github.com/mooyang-code/scf-framework/model"
	thttp "trpc.group/trpc-go/trpc-go/http"
	"trpc.group/trpc-go/trpc-go/log"
	"trpc.group/trpc-go/trpc-go/server"
)

// Gateway HTTP 网关
type Gateway struct {
	mux          *http.ServeMux
	probeHandler *heartbeat.ProbeHandler
	pluginHandler http.Handler
}

// NewGateway 创建 HTTP Gateway
func NewGateway(probeHandler *heartbeat.ProbeHandler) *Gateway {
	g := &Gateway{
		mux:          http.NewServeMux(),
		probeHandler: probeHandler,
	}
	g.registerRoutes()
	return g
}

// registerRoutes 注册内置路由
func (g *Gateway) registerRoutes() {
	g.mux.HandleFunc("GET /health", g.handleHealth)
	g.mux.HandleFunc("POST /probe", g.handleProbe)
	// catch-all 转发（必须放最后）
	g.mux.HandleFunc("/", g.handleCatchAll)
}

// SetPluginHandler 设置 catch-all 转发处理器（HTTPPluginAdapter 模式）
func (g *Gateway) SetPluginHandler(h http.Handler) {
	g.pluginHandler = h
}

// Register 注册到 TRPC Server 的指定 service
func (g *Gateway) Register(svc server.Service) {
	thttp.RegisterNoProtocolServiceMux(svc, g.mux)
}

// handleHealth 健康检查
func (g *Gateway) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "healthy",
	})
}

// handleProbe 探测请求处理
func (g *Gateway) handleProbe(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.ErrorContextf(ctx, "读取探测请求body失败: %v", err)
		writeJSON(w, http.StatusBadRequest, &model.Response{
			Success: false,
			Message: fmt.Sprintf("读取请求失败: %v", err),
		})
		return
	}
	defer r.Body.Close()

	var event model.CloudFunctionEvent
	if err := json.Unmarshal(body, &event); err != nil {
		log.ErrorContextf(ctx, "解析探测请求失败: %v", err)
		writeJSON(w, http.StatusBadRequest, &model.Response{
			Success: false,
			Message: fmt.Sprintf("解析请求失败: %v", err),
		})
		return
	}

	log.InfoContextf(ctx, "收到探测请求: source=%s, serverIP=%s, serverPort=%d",
		event.Source, event.ServerIP, event.ServerPort)

	resp, err := g.probeHandler.ProcessProbe(ctx, event)
	if err != nil {
		log.ErrorContextf(ctx, "处理探测请求失败: %v", err)
		writeJSON(w, http.StatusInternalServerError, &model.Response{
			Success: false,
			Message: fmt.Sprintf("处理探测失败: %v", err),
		})
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

// handleCatchAll 转发到插件处理器或返回 404
func (g *Gateway) handleCatchAll(w http.ResponseWriter, r *http.Request) {
	if g.pluginHandler != nil {
		g.pluginHandler.ServeHTTP(w, r)
		return
	}
	http.NotFound(w, r)
}

// writeJSON 写入 JSON 响应
func writeJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}
