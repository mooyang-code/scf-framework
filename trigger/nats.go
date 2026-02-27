package trigger

import (
	"context"
	"fmt"
	"time"

	"github.com/mooyang-code/scf-framework/model"
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
}

// NATSTrigger NATS JetStream Pull Consumer 触发器
type NATSTrigger struct {
	name     string
	config   NATSConfig
	conn     *nats.Conn
	js       jetstream.JetStream
	consumer jetstream.Consumer
	handler  TriggerHandler
	cancel   context.CancelFunc
}

// NewNATSTrigger 创建 NATSTrigger
func NewNATSTrigger(name string) *NATSTrigger {
	return &NATSTrigger{name: name}
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

	if v, ok := s["batch_size"].(int); ok {
		t.config.BatchSize = v
	} else if v, ok := s["batch_size"].(float64); ok {
		t.config.BatchSize = int(v)
	}
	if t.config.BatchSize <= 0 {
		t.config.BatchSize = 10
	}

	if v, ok := s["ack_wait"].(int); ok {
		t.config.AckWait = v
	} else if v, ok := s["ack_wait"].(float64); ok {
		t.config.AckWait = int(v)
	}
	if t.config.AckWait <= 0 {
		t.config.AckWait = 30
	}

	if v, ok := s["max_deliver"].(int); ok {
		t.config.MaxDeliver = v
	} else if v, ok := s["max_deliver"].(float64); ok {
		t.config.MaxDeliver = int(v)
	}
	if t.config.MaxDeliver <= 0 {
		t.config.MaxDeliver = 3
	}

	if v, ok := s["fetch_max_wait"].(int); ok {
		t.config.FetchMaxWait = v
	} else if v, ok := s["fetch_max_wait"].(float64); ok {
		t.config.FetchMaxWait = int(v)
	}
	if t.config.FetchMaxWait <= 0 {
		t.config.FetchMaxWait = 5
	}

	return nil
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

	log.InfoContextf(ctx, "[NATSTrigger] %s started: stream=%s, subject=%s, consumer=%s",
		t.name, t.config.Stream, t.config.Subject, t.config.ConsumerName)
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
