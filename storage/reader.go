package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/mooyang-code/scf-framework/model"
	"trpc.group/trpc-go/trpc-go/log"
)

// Reader xData 读取器
type Reader struct {
	storageURL string
	client     *http.Client
}

// NewReader 创建 xData Reader
func NewReader(storageURL string) *Reader {
	return &Reader{
		storageURL: storageURL,
		client:     &http.Client{Timeout: 30 * time.Second},
	}
}

// ReadConfig 读取配置
type ReadConfig struct {
	DatasetID int      `json:"dataset_id"`
	ObjectIDs []string `json:"object_ids,omitempty"`
	FieldKeys []string `json:"field_keys,omitempty"`
	StartTime string   `json:"start_time,omitempty"`
	EndTime   string   `json:"end_time,omitempty"`
	Limit     int      `json:"limit,omitempty"`
}

// getDataRequest xData GetData 请求体
type getDataRequest struct {
	DatasetID int      `json:"dataset_id"`
	ObjectIDs []string `json:"object_ids,omitempty"`
	FieldKeys []string `json:"field_keys,omitempty"`
	StartTime string   `json:"start_time,omitempty"`
	EndTime   string   `json:"end_time,omitempty"`
	Limit     int      `json:"limit,omitempty"`
}

// getDataResponse xData GetData 响应体
type getDataResponse struct {
	Code    int              `json:"code"`
	Message string           `json:"message"`
	Data    []model.DataPoint `json:"data"`
}

// Read 从 xData 读取数据
func (r *Reader) Read(ctx context.Context, cfg ReadConfig) ([]model.DataPoint, error) {
	if r.storageURL == "" {
		log.WarnContextf(ctx, "[StorageReader] skip read: storage URL not available")
		return nil, nil
	}

	url := r.storageURL + "/xdata/GetData"

	reqBody := getDataRequest{
		DatasetID: cfg.DatasetID,
		ObjectIDs: cfg.ObjectIDs,
		FieldKeys: cfg.FieldKeys,
		StartTime: cfg.StartTime,
		EndTime:   cfg.EndTime,
		Limit:     cfg.Limit,
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal GetData request: %w", err)
	}

	log.InfoContextf(ctx, "[StorageReader] reading from dataset %d, body_len=%d", cfg.DatasetID, len(data))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GetData request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("GetData returned status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read GetData response: %w", err)
	}

	var result getDataResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse GetData response: %w", err)
	}

	log.InfoContextf(ctx, "[StorageReader] GetData success: %d points read from dataset %d", len(result.Data), cfg.DatasetID)
	return result.Data, nil
}

// UpdateURL 更新存储服务地址
func (r *Reader) UpdateURL(url string) {
	if url != "" {
		r.storageURL = url
	}
}
