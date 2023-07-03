package beeorm

func clearByIDs(engine *engineImplementation, entity Entity, ids ...uint64) {
	schema := initIfNeeded(engine.registry, entity).tableSchema
	cacheKeys := make([]string, len(ids))
	for i, id := range ids {
		cacheKeys[i] = engine.getCacheKey(schema, id)
	}
	localCache, has := schema.GetLocalCache(engine)
	if !has && engine.hasRequestCache {
		has = true
		localCache = engine.GetLocalCache(requestCacheKey)
	}
	if has {
		localCache.Remove(cacheKeys...)
	}
	redisCache, has := schema.GetRedisCache(engine)
	if has {
		redisCache.Del(cacheKeys...)
	}
}
