package cache

import (
	"context"

	"trpc.group/trpc-go/trpc-database/localcache"
)

// Get 从本地缓存获取值
func Get(key string) (interface{}, bool) {
	return localcache.Get(key)
}

// Set 设置缓存值，ttl 为秒数
func Set(key string, val interface{}, ttl int64) bool {
	return localcache.Set(key, val, ttl)
}

// Del 删除缓存条目
func Del(key string) {
	localcache.Del(key)
}

// GetWithLoad 获取缓存值，未命中时调用 loadFunc 加载并缓存
func GetWithLoad(ctx context.Context, key string,
	loadFunc func(context.Context, string) (interface{}, error),
	ttl int64) (interface{}, error) {
	return localcache.GetWithLoad(ctx, key, loadFunc, ttl)
}
