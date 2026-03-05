package storage

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/avast/retry-go"
	"github.com/mooyang-code/scf-framework/config"
	"github.com/mooyang-code/scf-framework/model"
	pb "github.com/mooyang-code/xData-mini/storage/proto"
	"trpc.group/trpc-go/trpc-go/client"
	"trpc.group/trpc-go/trpc-go/log"
)

// WriteOverride 写入时的覆盖配置（用于 WriteGroups 场景）
type WriteOverride struct {
	WriteMode string
	DatasetID *int
	Freq      string
	AppKey    string
}

// RPCWriter 通过 tRPC client 写入 xData
type RPCWriter struct {
	target     string // RPC 目标地址 "ip://host:port"
	storageCfg *config.StorageConfig
}

// NewRPCWriter 创建 xData RPCWriter
func NewRPCWriter(target string, storageCfg *config.StorageConfig) *RPCWriter {
	return &RPCWriter{
		target:     target,
		storageCfg: storageCfg,
	}
}

// UpdateURL 更新存储服务地址（运行时由心跳下发更新）
func (w *RPCWriter) UpdateURL(target string) {
	if target != "" {
		w.target = target
	}
}

// SetData 将数据点通过 SetData RPC 写入 xData
func (w *RPCWriter) SetData(ctx context.Context, points []model.DataPoint, override *WriteOverride) error {
	if len(points) == 0 {
		return nil
	}
	if w.target == "" {
		log.WarnContextf(ctx, "[RPCWriter] skip write: target not available")
		return nil
	}
	if w.storageCfg == nil {
		log.WarnContextf(ctx, "[RPCWriter] skip write: storage config not available")
		return nil
	}

	// 确定参数
	datasetID := w.storageCfg.DatasetID
	freq := w.storageCfg.Freq
	appKey := w.storageCfg.AuthInfo.AppKey
	if override != nil {
		if override.DatasetID != nil {
			datasetID = *override.DatasetID
		}
		if override.Freq != "" {
			freq = override.Freq
		}
		if override.AppKey != "" {
			appKey = override.AppKey
		}
	}

	// 按 object_id 分组
	grouped := make(map[string][]model.DataPoint)
	for _, p := range points {
		grouped[p.ObjectID] = append(grouped[p.ObjectID], p)
	}

	// 构建 pb.SetDataReq
	var dataList []*pb.UpdateDataList
	for objectID, pts := range grouped {
		rows := make([]*pb.UpdateDataRow, 0, len(pts))
		for _, p := range pts {
			pbFields, err := buildUpdateFields(p.Fields)
			if err != nil {
				log.WarnContextf(ctx, "[RPCWriter] skip row: objectID=%s, times=%s, err=%v", objectID, p.Times, err)
				continue
			}
			rows = append(rows, &pb.UpdateDataRow{
				Times:  p.Times,
				RowId:  objectID,
				Fields: pbFields,
			})
		}
		if len(rows) == 0 {
			continue
		}
		dataList = append(dataList, &pb.UpdateDataList{
			DataKey: &pb.DataKey{
				ProjectId: int32(w.storageCfg.ProjectID),
				DatasetId: int32(datasetID),
				ObjectId:  objectID,
				Freq:      freq,
			},
			DataRows: rows,
		})
	}

	if len(dataList) == 0 {
		return nil
	}

	req := &pb.SetDataReq{
		AuthInfo: &pb.AuthInfo{
			AppId:  w.storageCfg.AuthInfo.AppID,
			AppKey: appKey,
		},
		DataList: dataList,
	}

	log.InfoContextf(ctx, "[RPCWriter] SetData: %d points, dataset=%d, target=%s", len(points), datasetID, w.target)
	return w.doSetData(ctx, req, len(points), datasetID)
}

// UpsertObject 将数据点通过 UpsertObject RPC 写入 xData
func (w *RPCWriter) UpsertObject(ctx context.Context, points []model.DataPoint, override *WriteOverride) error {
	if len(points) == 0 {
		return nil
	}
	if w.target == "" {
		log.WarnContextf(ctx, "[RPCWriter] skip upsert: target not available")
		return nil
	}
	if w.storageCfg == nil {
		log.WarnContextf(ctx, "[RPCWriter] skip upsert: storage config not available")
		return nil
	}

	// 确定参数
	datasetID := w.storageCfg.DatasetID
	appKey := w.storageCfg.AuthInfo.AppKey
	if override != nil {
		if override.DatasetID != nil {
			datasetID = *override.DatasetID
		}
		if override.AppKey != "" {
			appKey = override.AppKey
		}
	}

	// 构建 ObjectRows
	objectRows := make([]*pb.UpdateObjectRow, 0, len(points))
	for _, p := range points {
		pbFields, err := buildUpdateFields(p.Fields)
		if err != nil {
			log.WarnContextf(ctx, "[RPCWriter] skip upsert row: objectID=%s, err=%v", p.ObjectID, err)
			continue
		}
		objectRows = append(objectRows, &pb.UpdateObjectRow{
			ObjectId: p.ObjectID,
			Fields:   pbFields,
		})
	}

	if len(objectRows) == 0 {
		return nil
	}

	req := &pb.UpsertObjectReq{
		AuthInfo: &pb.AuthInfo{
			AppId:  w.storageCfg.AuthInfo.AppID,
			AppKey: appKey,
		},
		ProjectId:  int32(w.storageCfg.ProjectID),
		DatasetId:  int32(datasetID),
		ObjectRows: objectRows,
	}

	log.InfoContextf(ctx, "[RPCWriter] UpsertObject: %d points, dataset=%d, target=%s", len(points), datasetID, w.target)
	return w.doUpsertObject(ctx, req, len(points), datasetID)
}

// doSetData 发送 SetData RPC，带重试
func (w *RPCWriter) doSetData(ctx context.Context, req *pb.SetDataReq, pointCount int, datasetID int) error {
	err := retry.Do(
		func() error {
			c := pb.NewAccessClientProxy(client.WithTarget(w.target))
			callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			rsp, err := c.SetData(callCtx, req)
			if err != nil {
				return fmt.Errorf("SetData RPC failed: %w", err)
			}
			if rsp.GetRetInfo().GetCode() != 0 {
				return fmt.Errorf("SetData returned error: code=%d, msg=%s",
					rsp.GetRetInfo().GetCode(), rsp.GetRetInfo().GetMsg())
			}
			if len(rsp.GetFailedList()) > 0 {
				log.WarnContextf(ctx, "[RPCWriter] SetData partial failure: %d failed groups", len(rsp.GetFailedList()))
			}
			return nil
		},
		retry.Attempts(3),
		retry.Delay(500*time.Millisecond),
		retry.DelayType(retry.BackOffDelay),
		retry.LastErrorOnly(true),
		retry.OnRetry(func(n uint, err error) {
			log.WarnContextf(ctx, "[RPCWriter] retrying SetData: attempt=%d, error=%v", n+1, err)
		}),
		retry.Context(ctx),
	)

	if err != nil {
		log.ErrorContextf(ctx, "[RPCWriter] SetData failed after retries: %v", err)
		return err
	}
	log.InfoContextf(ctx, "[RPCWriter] SetData success: %d points written to dataset %d", pointCount, datasetID)
	return nil
}

// doUpsertObject 发送 UpsertObject RPC，带重试
func (w *RPCWriter) doUpsertObject(ctx context.Context, req *pb.UpsertObjectReq, pointCount int, datasetID int) error {
	err := retry.Do(
		func() error {
			c := pb.NewAccessClientProxy(client.WithTarget(w.target))
			callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			rsp, err := c.UpsertObject(callCtx, req)
			if err != nil {
				return fmt.Errorf("UpsertObject RPC failed: %w", err)
			}
			if rsp.GetRetInfo().GetCode() != 0 {
				return fmt.Errorf("UpsertObject returned error: code=%d, msg=%s",
					rsp.GetRetInfo().GetCode(), rsp.GetRetInfo().GetMsg())
			}
			if len(rsp.GetFailedRows()) > 0 {
				log.WarnContextf(ctx, "[RPCWriter] UpsertObject partial failure: %d failed rows", len(rsp.GetFailedRows()))
			}
			return nil
		},
		retry.Attempts(3),
		retry.Delay(500*time.Millisecond),
		retry.DelayType(retry.BackOffDelay),
		retry.LastErrorOnly(true),
		retry.OnRetry(func(n uint, err error) {
			log.WarnContextf(ctx, "[RPCWriter] retrying UpsertObject: attempt=%d, error=%v", n+1, err)
		}),
		retry.Context(ctx),
	)

	if err != nil {
		log.ErrorContextf(ctx, "[RPCWriter] UpsertObject failed after retries: %v", err)
		return err
	}
	log.InfoContextf(ctx, "[RPCWriter] UpsertObject success: %d points written to dataset %d", pointCount, datasetID)
	return nil
}

// ========== 类型推断 ==========

// buildUpdateFields 将 map[string]interface{} 转为 map[string]*pb.UpdateField
func buildUpdateFields(fields map[string]interface{}) (map[string]*pb.UpdateField, error) {
	result := make(map[string]*pb.UpdateField, len(fields))
	for key, val := range fields {
		field, err := inferAndBuildUpdateField(key, val)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", key, err)
		}
		result[key] = field
	}
	return result, nil
}

// inferAndBuildUpdateField 根据 interface{} 值推断 xData 字段类型并构建 pb.UpdateField
func inferAndBuildUpdateField(fieldKey string, value interface{}) (*pb.UpdateField, error) {
	field := &pb.UpdateField{
		FieldKey:   fieldKey,
		UpdateType: pb.EnumUpdateType_SET_UPDATE,
	}

	switch v := value.(type) {
	case string:
		field.FieldType = pb.EnumFieldType_STR_FIELD
		field.SimpleValue = &pb.SimpleValue{
			Value: &pb.SimpleValue_Str{Str: v},
		}

	case int:
		field.FieldType = pb.EnumFieldType_INT_FIELD
		field.SimpleValue = &pb.SimpleValue{
			Value: &pb.SimpleValue_Int{Int: int64(v)},
		}

	case int64:
		field.FieldType = pb.EnumFieldType_INT_FIELD
		field.SimpleValue = &pb.SimpleValue{
			Value: &pb.SimpleValue_Int{Int: v},
		}

	case float64:
		if v == math.Trunc(v) && !math.IsInf(v, 0) && !math.IsNaN(v) {
			// 整数值的 float64 → IntField
			field.FieldType = pb.EnumFieldType_INT_FIELD
			field.SimpleValue = &pb.SimpleValue{
				Value: &pb.SimpleValue_Int{Int: int64(v)},
			}
		} else {
			field.FieldType = pb.EnumFieldType_FLOAT_FIELD
			field.SimpleValue = &pb.SimpleValue{
				Value: &pb.SimpleValue_Float{Float: v},
			}
		}

	case map[string]interface{}:
		field.FieldType = pb.EnumFieldType_MAP_KV_FIELD
		mapContainer, err := buildMapContainer(v)
		if err != nil {
			return nil, fmt.Errorf("MapKV field %q: %w", fieldKey, err)
		}
		field.MapValue = mapContainer

	default:
		return nil, fmt.Errorf("unsupported value type %T for field %q", value, fieldKey)
	}
	return field, nil
}

// buildMapContainer 将 map[string]interface{} 转为 pb.MapContainer
func buildMapContainer(m map[string]interface{}) (*pb.MapContainer, error) {
	entries := make(map[string]*pb.KeyValueEntry, len(m))
	for k, v := range m {
		entry, err := buildKeyValueEntry(v)
		if err != nil {
			return nil, fmt.Errorf("map key %q: %w", k, err)
		}
		entries[k] = entry
	}
	return &pb.MapContainer{Entries: entries}, nil
}

// buildKeyValueEntry 将单个值转为 pb.KeyValueEntry
func buildKeyValueEntry(value interface{}) (*pb.KeyValueEntry, error) {
	entry := &pb.KeyValueEntry{}

	switch v := value.(type) {
	case string:
		entry.Type = pb.EnumFieldType_STR_FIELD
		entry.Value = &pb.SimpleValue{
			Value: &pb.SimpleValue_Str{Str: v},
		}

	case int:
		entry.Type = pb.EnumFieldType_INT_FIELD
		entry.Value = &pb.SimpleValue{
			Value: &pb.SimpleValue_Int{Int: int64(v)},
		}

	case int64:
		entry.Type = pb.EnumFieldType_INT_FIELD
		entry.Value = &pb.SimpleValue{
			Value: &pb.SimpleValue_Int{Int: v},
		}

	case float64:
		entry.Type = pb.EnumFieldType_FLOAT_FIELD
		entry.Value = &pb.SimpleValue{
			Value: &pb.SimpleValue_Float{Float: v},
		}

	default:
		return nil, fmt.Errorf("unsupported MapKV value type %T", value)
	}
	return entry, nil
}
