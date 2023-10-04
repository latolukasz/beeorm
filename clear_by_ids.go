package beeorm

func ClearCacheByIDs[E any](c Context, ids ...uint64) {
	schema := GetEntitySchema[E](c)
	localPool, has := schema.GetLocalCache()
	if has {
		for _, id := range ids {
			localPool.removeEntity(c, id)
		}
	}
	redisPool, has := schema.GetRedisCache()
	if has {
		redisPool.hDelUints(c, schema.GetCacheKey(), ids...)
	}
}
