package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/mooyang-code/scf-framework/config"
	"github.com/mooyang-code/scf-framework/model"
	pb "github.com/mooyang-code/xData-mini/storage/proto"
	"trpc.group/trpc-go/trpc-go/client"
	"trpc.group/trpc-go/trpc-go/log"
)

// Reader xData 读取器（RPC 方式）
type Reader struct {
	target     string
	storageCfg *config.StorageConfig
}

// NewReader 创建 xData Reader
func NewReader(target string, storageCfg *config.StorageConfig) *Reader {
	return &Reader{
		target:     target,
		storageCfg: storageCfg,
	}
}

// ReadConfig 读取配置（调用方按需指定覆盖参数）
type ReadConfig struct {
	DatasetID int      `json:"dataset_id"`
	ObjectIDs []string `json:"object_ids,omitempty"`
	FieldKeys []string `json:"field_keys,omitempty"`
	StartTime string   `json:"start_time,omitempty"`
	EndTime   string   `json:"end_time,omitempty"`
	Limit     int      `json:"limit,omitempty"`
}

// GetData 从 xData 通过 RPC 读取数据
func (r *Reader) GetData(ctx context.Context, cfg ReadConfig) ([]model.DataPoint, error) {
	if r.target == "" {
		log.WarnContextf(ctx, "[StorageReader] skip read: target not available")
		return nil, nil
	}

	datasetID, projectID, appID, appKey := r.resolveConfig(cfg)

	req := &pb.GetDataReq{
		AuthInfo:   &pb.AuthInfo{AppId: appID, AppKey: appKey},
		DataParams: buildGetDataParams(cfg, projectID, datasetID),
	}

	log.InfoContextf(ctx, "[StorageReader] reading from dataset %d via RPC, target=%s", datasetID, r.target)

	c := pb.NewAccessClientProxy(client.WithTarget(r.target))
	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	rsp, err := c.GetData(callCtx, req)
	if err != nil {
		return nil, fmt.Errorf("GetData RPC failed: %w", err)
	}
	if rsp.GetRetInfo().GetCode() != 0 {
		return nil, fmt.Errorf("GetData returned error: code=%d, msg=%s",
			rsp.GetRetInfo().GetCode(), rsp.GetRetInfo().GetMsg())
	}

	points := convertDataList(rsp.GetDataList())

	log.InfoContextf(ctx, "[StorageReader] GetData success: %d points read from dataset %d", len(points), datasetID)
	return points, nil
}

// UpdateURL 更新存储服务地址
func (r *Reader) UpdateURL(target string) {
	if target != "" {
		r.target = target
	}
}

// resolveConfig 合并 ReadConfig 与全局 storageCfg，返回最终的 datasetID/projectID/appID/appKey
func (r *Reader) resolveConfig(cfg ReadConfig) (datasetID, projectID int, appID, appKey string) {
	datasetID = cfg.DatasetID
	if r.storageCfg != nil {
		if datasetID == 0 {
			datasetID = r.storageCfg.DatasetID
		}
		projectID = r.storageCfg.ProjectID
		appID = r.storageCfg.AuthInfo.AppID
		appKey = r.storageCfg.AuthInfo.AppKey
	}
	return
}

// buildGetDataParams 根据 ReadConfig 构建 GetDataParams 列表
func buildGetDataParams(cfg ReadConfig, projectID, datasetID int) []*pb.GetDataParams {
	objectIDs := cfg.ObjectIDs
	if len(objectIDs) == 0 {
		objectIDs = []string{""} // 无 objectID 时查一次（objectID 为空串）
	}

	params := make([]*pb.GetDataParams, 0, len(objectIDs))
	for _, objectID := range objectIDs {
		p := &pb.GetDataParams{
			DataKey: &pb.DataKey{
				ProjectId: int32(projectID),
				DatasetId: int32(datasetID),
				ObjectId:  objectID,
			},
			FieldKeys: cfg.FieldKeys,
		}
		if cfg.Limit > 0 {
			p.MaxLimit = uint32(cfg.Limit)
		}
		if cfg.StartTime != "" || cfg.EndTime != "" {
			p.TimeRange = &pb.TimeRange{Start: cfg.StartTime}
			if cfg.EndTime != "" {
				p.TimeRange.RangeType = &pb.TimeRange_End{End: cfg.EndTime}
			}
		}
		params = append(params, p)
	}
	return params
}

// convertDataList 将 RPC 返回的 DataList map 转为 []model.DataPoint
func convertDataList(dataLists map[string]*pb.DataList) []model.DataPoint {
	var points []model.DataPoint
	for _, dl := range dataLists {
		objectID := ""
		if dl.GetDataKey() != nil {
			objectID = dl.GetDataKey().GetObjectId()
		}
		for _, row := range dl.GetDataRows() {
			points = append(points, model.DataPoint{
				Times:    row.GetTimes(),
				ObjectID: objectID,
				Fields:   extractFields(row.GetFields()),
			})
		}
	}
	return points
}

// extractFields 将 pb.FieldValue map 转为 map[string]interface{}
func extractFields(pbFields map[string]*pb.FieldValue) map[string]interface{} {
	if len(pbFields) == 0 {
		return nil
	}
	fields := make(map[string]interface{}, len(pbFields))
	for k, fv := range pbFields {
		fields[k] = extractFieldValue(fv)
	}
	return fields
}

// extractFieldValue 将 pb.FieldValue 转为原始类型值
func extractFieldValue(fv *pb.FieldValue) interface{} {
	if fv == nil {
		return nil
	}
	if fv.GetFieldType() == pb.EnumFieldType_MAP_KV_FIELD && fv.GetMapValue() != nil {
		m := make(map[string]interface{}, len(fv.GetMapValue().GetEntries()))
		for k, entry := range fv.GetMapValue().GetEntries() {
			m[k] = extractSimpleValue(entry.GetValue())
		}
		return m
	}
	return extractSimpleValue(fv.GetSimpleValue())
}

// extractSimpleValue 将 pb.SimpleValue 转为原始类型值
func extractSimpleValue(sv *pb.SimpleValue) interface{} {
	if sv == nil {
		return nil
	}
	switch v := sv.GetValue().(type) {
	case *pb.SimpleValue_Str:
		return v.Str
	case *pb.SimpleValue_Int:
		return v.Int
	case *pb.SimpleValue_Float:
		return v.Float
	case *pb.SimpleValue_Time:
		return v.Time
	default:
		return nil
	}
}
