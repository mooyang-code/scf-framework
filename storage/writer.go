package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/avast/retry-go"
	"github.com/mooyang-code/scf-framework/model"
	"trpc.group/trpc-go/trpc-go/log"
)

// Writer xData 写入器
type Writer struct {
	storageURL string
	client     *http.Client
}

// NewWriter 创建 xData Writer
func NewWriter(storageURL string) *Writer {
	return &Writer{
		storageURL: storageURL,
		client:     &http.Client{Timeout: 30 * time.Second},
	}
}

// WriteConfig 写入配置
type WriteConfig struct {
	DatasetID int    `json:"dataset_id"`
	WriteMode string `json:"write_mode,omitempty"` // "upsert" 或 "append"
}

// setDataRequest xData SetData 请求体
type setDataRequest struct {
	DatasetID int           `json:"dataset_id"`
	WriteMode string        `json:"write_mode,omitempty"`
	Data      []setDataItem `json:"data"`
}

// setDataItem 单条写入数据
type setDataItem struct {
	Times    string                      `json:"times"`
	ObjectID string                      `json:"object_id"`
	Fields   map[string]model.FieldValue `json:"fields"`
}

// Write 将数据点写入 xData（带重试）
func (w *Writer) Write(ctx context.Context, cfg WriteConfig, points []model.DataPoint) error {
	if len(points) == 0 {
		return nil
	}

	if w.storageURL == "" {
		log.WarnContextf(ctx, "[StorageWriter] skip write: storage URL not available")
		return nil
	}

	url := w.storageURL + "/xdata/SetData"

	items := make([]setDataItem, len(points))
	for i, p := range points {
		items[i] = setDataItem{
			Times:    p.Times,
			ObjectID: p.ObjectID,
			Fields:   p.Fields,
		}
	}

	reqBody := setDataRequest{
		DatasetID: cfg.DatasetID,
		WriteMode: cfg.WriteMode,
		Data:      items,
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal SetData request: %w", err)
	}

	log.InfoContextf(ctx, "[StorageWriter] writing %d points to dataset %d, body_len=%d", len(points), cfg.DatasetID, len(data))

	err = retry.Do(
		func() error {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
			if err != nil {
				return fmt.Errorf("failed to create request: %w", err)
			}
			req.Header.Set("Content-Type", "application/json")

			resp, err := w.client.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("SetData returned status %d: %s", resp.StatusCode, string(body))
			}
			return nil
		},
		retry.Attempts(3),
		retry.Delay(500*time.Millisecond),
		retry.DelayType(retry.BackOffDelay),
		retry.LastErrorOnly(true),
		retry.OnRetry(func(n uint, err error) {
			log.WarnContextf(ctx, "[StorageWriter] retrying SetData: attempt=%d, error=%v", n+1, err)
		}),
		retry.Context(ctx),
	)

	if err != nil {
		log.ErrorContextf(ctx, "[StorageWriter] SetData failed after retries: %v", err)
		return err
	}

	log.InfoContextf(ctx, "[StorageWriter] SetData success: %d points written", len(points))
	return nil
}

// UpdateURL 更新存储服务地址（运行时由心跳下发更新）
func (w *Writer) UpdateURL(url string) {
	if url != "" {
		w.storageURL = url
	}
}
