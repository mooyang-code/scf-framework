package trigger

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mooyang-code/scf-framework/cache"
	"github.com/mooyang-code/scf-framework/model"
	"github.com/mooyang-code/scf-framework/storage"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"trpc.group/trpc-go/trpc-go/log"
)

// NATSConfig NATS 触发器配置
type NATSConfig struct {
	URL          string
	Stream       string
	Subject      string
	ConsumerName string
	BatchSize    int
	AckWait      int
	MaxDeliver   int
	FetchMaxWait int
	// 缓存相关
	CacheEnabled  bool
	CacheKeyPrefix string
	CacheMaxItems int
	CacheTTL      int64 // 秒
	// 回源相关
	BackfillEnabled   bool
	BackfillDatasetID int
	BackfillFieldKeys []string
}

// NATSTrigger NATS JetStream Pull Consumer 触发器
type NATSTrigger struct {
	name          string
	config        NATSConfig
	conn          *nats.Conn
	js            jetstream.JetStream
	consumer      jetstream.Consumer
	handler       TriggerHandler
	cancel        context.CancelFunc
	storageReader *storage.Reader
	backfillMu    sync.Mutex
}

// NewNATSTrigger 创建 NATSTrigger
func NewNATSTrigger(name string) *NATSTrigger {
	return &NATSTrigger{name: name}
}

// SetStorageReader 设置回源读取器
func (t *NATSTrigger) SetStorageReader(r *storage.Reader) {
	t.storageReader = r
}

// Name 返回触发器名称
func (t *NATSTrigger) Name() string {
	return t.name
}

// Type 返回触发器类型
func (t *NATSTrigger) Type() model.TriggerType {
	return model.TriggerNATS
}

// Init 从 TriggerConfig.Settings 解析 NATSConfig
func (t *NATSTrigger) Init(_ context.Context, cfg model.TriggerConfig) error {
	s := cfg.Settings

	t.config.URL, _ = s["url"].(string)
	if t.config.URL == "" {
		return fmt.Errorf("NATS trigger %q missing url setting", t.name)
	}

	t.config.Stream, _ = s["stream"].(string)
	t.config.Subject, _ = s["subject"].(string)
	t.config.ConsumerName, _ = s["consumer_name"].(string)

	t.config.BatchSize = getIntSetting(s, "batch_size", 10)
	t.config.AckWait = getIntSetting(s, "ack_wait", 30)
	t.config.MaxDeliver = getIntSetting(s, "max_deliver", 3)
	t.config.FetchMaxWait = getIntSetting(s, "fetch_max_wait", 5)

	// 缓存配置
	if v, ok := s["cache_enabled"].(bool); ok {
		t.config.CacheEnabled = v
	}
	t.config.CacheKeyPrefix, _ = s["cache_key_prefix"].(string)
	if t.config.CacheKeyPrefix == "" {
		t.config.CacheKeyPrefix = "kline"
	}
	t.config.CacheMaxItems = getIntSetting(s, "cache_max_items", 2000)
	t.config.CacheTTL = int64(getIntSetting(s, "cache_ttl", 36000))

	// 回源配置
	if v, ok := s["backfill_enabled"].(bool); ok {
		t.config.BackfillEnabled = v
	}
	t.config.BackfillDatasetID = getIntSetting(s, "backfill_dataset_id", 0)
	if v, ok := s["backfill_field_keys"].([]interface{}); ok {
		for _, item := range v {
			if str, ok := item.(string); ok {
				t.config.BackfillFieldKeys = append(t.config.BackfillFieldKeys, str)
			}
		}
	}

	return nil
}

// getIntSetting 从 settings map 中提取 int 值（兼容 int / float64）
func getIntSetting(s map[string]interface{}, key string, defaultVal int) int {
	if v, ok := s[key].(int); ok {
		return v
	}
	if v, ok := s[key].(float64); ok {
		return int(v)
	}
	return defaultVal
}

// Start 连接 NATS，创建 JetStream Pull Consumer，启动 consumeLoop
func (t *NATSTrigger) Start(ctx context.Context, handler TriggerHandler) error {
	t.handler = handler

	nc, err := nats.Connect(t.config.URL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.WarnContextf(ctx, "[NATSTrigger] %s disconnected: %v", t.name, err)
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			log.InfoContextf(ctx, "[NATSTrigger] %s reconnected", t.name)
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to connect NATS for trigger %q: %w", t.name, err)
	}
	t.conn = nc

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return fmt.Errorf("failed to create JetStream for trigger %q: %w", t.name, err)
	}
	t.js = js

	consumerCfg := jetstream.ConsumerConfig{
		Durable:       t.config.ConsumerName,
		FilterSubject: t.config.Subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       time.Duration(t.config.AckWait) * time.Second,
		MaxDeliver:    t.config.MaxDeliver,
		DeliverPolicy: jetstream.DeliverNewPolicy,
	}

	cons, err := js.CreateOrUpdateConsumer(ctx, t.config.Stream, consumerCfg)
	if err != nil {
		nc.Close()
		return fmt.Errorf("failed to create NATS consumer for trigger %q: %w", t.name, err)
	}
	t.consumer = cons

	loopCtx, cancel := context.WithCancel(ctx)
	t.cancel = cancel

	go t.consumeLoop(loopCtx)

	log.InfoContextf(ctx, "[NATSTrigger] %s started: stream=%s, subject=%s, consumer=%s, cache=%v, backfill=%v",
		t.name, t.config.Stream, t.config.Subject, t.config.ConsumerName,
		t.config.CacheEnabled, t.config.BackfillEnabled)
	return nil
}

// Stop 停止消费循环并关闭连接
func (t *NATSTrigger) Stop(_ context.Context) error {
	if t.cancel != nil {
		t.cancel()
	}
	if t.conn != nil {
		t.conn.Close()
	}
	return nil
}

// consumeLoop 持续拉取并处理 NATS 消息
func (t *NATSTrigger) consumeLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.InfoContextf(ctx, "[NATSTrigger] %s consume loop exiting", t.name)
			return
		default:
		}

		msgs, err := t.consumer.Fetch(t.config.BatchSize,
			jetstream.FetchMaxWait(time.Duration(t.config.FetchMaxWait)*time.Second),
		)
		if err != nil {
			log.WarnContextf(ctx, "[NATSTrigger] %s fetch failed: %v", t.name, err)
			time.Sleep(1 * time.Second)
			continue
		}

		for msg := range msgs.Messages() {
			event := &model.TriggerEvent{
				Type:    model.TriggerNATS,
				Name:    t.name,
				Payload: msg.Data(),
				Metadata: map[string]string{
					"subject": msg.Subject(),
				},
			}

			// 缓存层：自动缓存 K线 + 回源 + 注入完整序列
			if t.config.CacheEnabled {
				t.processKlineCache(ctx, event, msg.Subject())
			}

			if err := t.handler(ctx, event); err != nil {
				log.ErrorContextf(ctx, "[NATSTrigger] %s handler error: %v", t.name, err)
				msg.Nak()
				continue
			}
			msg.Ack()
		}

		if msgs.Error() != nil {
			log.WarnContextf(ctx, "[NATSTrigger] %s message iteration error: %v", t.name, msgs.Error())
		}
	}
}

// klineMessage NATS K线消息的通用结构
type klineMessage struct {
	Symbol   string          `json:"symbol"`
	Interval string          `json:"interval"`
	Kline    json.RawMessage `json:"kline,omitempty"`   // 单条 K线
	Klines   json.RawMessage `json:"klines,omitempty"`  // K线数组
}

// processKlineCache 处理 K线缓存逻辑：
// 1. 从 NATS 消息/subject 提取 symbol + interval
// 2. 追加到缓存（滑动窗口）
// 3. 冷启动时从 xData 回填
// 4. 将完整 K线序列注入 event.Payload
func (t *NATSTrigger) processKlineCache(ctx context.Context, event *model.TriggerEvent, subject string) {
	// 解析 NATS 消息提取 symbol/interval
	var msg klineMessage
	if err := json.Unmarshal(event.Payload, &msg); err != nil {
		log.WarnContextf(ctx, "[NATSTrigger] %s failed to parse kline message: %v", t.name, err)
		return
	}

	// 如果消息中没有 symbol/interval，尝试从 subject 提取
	// subject 格式: kline.{symbol}.{interval}
	if msg.Symbol == "" || msg.Interval == "" {
		parts := strings.Split(subject, ".")
		if len(parts) >= 3 {
			if msg.Symbol == "" {
				msg.Symbol = parts[1]
			}
			if msg.Interval == "" {
				msg.Interval = parts[2]
			}
		}
	}

	if msg.Symbol == "" || msg.Interval == "" {
		log.WarnContextf(ctx, "[NATSTrigger] %s cannot determine symbol/interval from message", t.name)
		return
	}

	cacheKey := fmt.Sprintf("%s:%s:%s", t.config.CacheKeyPrefix, msg.Symbol, msg.Interval)

	// 解析新到的 K线数据
	var newKlines []json.RawMessage
	if msg.Kline != nil {
		newKlines = append(newKlines, msg.Kline)
	}
	if msg.Klines != nil {
		var klines []json.RawMessage
		if err := json.Unmarshal(msg.Klines, &klines); err == nil {
			newKlines = append(newKlines, klines...)
		}
	}
	// 如果没有解析出独立的 kline/klines，整条消息可能本身就是一条 K线
	if len(newKlines) == 0 {
		newKlines = append(newKlines, event.Payload)
	}

	// 从缓存获取已有 K线序列
	cached, exists := cache.Get(cacheKey)
	var klineList []json.RawMessage

	if exists {
		if list, ok := cached.([]json.RawMessage); ok {
			klineList = list
		}
	}

	// 冷启动回源
	if !exists && t.config.BackfillEnabled && t.storageReader != nil {
		t.backfillMu.Lock()
		// 双重检查
		cached2, exists2 := cache.Get(cacheKey)
		if !exists2 {
			log.InfoContextf(ctx, "[NATSTrigger] %s cold start backfill: symbol=%s, interval=%s", t.name, msg.Symbol, msg.Interval)
			backfilled := t.backfillFromStorage(ctx, msg.Symbol, msg.Interval)
			if len(backfilled) > 0 {
				klineList = backfilled
				log.InfoContextf(ctx, "[NATSTrigger] %s backfilled %d klines for %s:%s", t.name, len(backfilled), msg.Symbol, msg.Interval)
			}
		} else if list, ok := cached2.([]json.RawMessage); ok {
			klineList = list
		}
		t.backfillMu.Unlock()
	}

	// 追加新 K线
	klineList = append(klineList, newKlines...)

	// 滑动窗口裁剪
	maxItems := t.config.CacheMaxItems
	if maxItems > 0 && len(klineList) > maxItems {
		klineList = klineList[len(klineList)-maxItems:]
	}

	// 写回缓存
	cache.Set(cacheKey, klineList, t.config.CacheTTL)

	// 构建增强的 Payload：包含完整 K线历史 + 原始消息元信息
	enrichedPayload := map[string]interface{}{
		"symbol":   msg.Symbol,
		"interval": msg.Interval,
		"klines":   klineList,
	}
	if data, err := json.Marshal(enrichedPayload); err == nil {
		event.Payload = data
		log.InfoContextf(ctx, "[NATSTrigger] %s enriched payload: symbol=%s, interval=%s, klines=%d",
			t.name, msg.Symbol, msg.Interval, len(klineList))
	}
}

// backfillFromStorage 从 xData 回填历史 K线
func (t *NATSTrigger) backfillFromStorage(ctx context.Context, symbol, interval string) []json.RawMessage {
	if t.storageReader == nil {
		return nil
	}

	cfg := storage.ReadConfig{
		DatasetID: t.config.BackfillDatasetID,
		ObjectIDs: []string{fmt.Sprintf("%s:%s", symbol, interval)},
		FieldKeys: t.config.BackfillFieldKeys,
		Limit:     t.config.CacheMaxItems,
	}

	points, err := t.storageReader.GetData(ctx, cfg)
	if err != nil {
		log.ErrorContextf(ctx, "[NATSTrigger] %s backfill failed: %v", t.name, err)
		return nil
	}

	// 将 DataPoint 转为 K线 JSON
	var klines []json.RawMessage
	for _, p := range points {
		kline := map[string]interface{}{
			"open_time": p.Times,
			"symbol":    symbol,
			"interval":  interval,
		}
		for k, v := range p.Fields {
			kline[k] = v
		}
		if data, err := json.Marshal(kline); err == nil {
			klines = append(klines, data)
		}
	}

	return klines
}
