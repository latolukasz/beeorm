package beeorm

func ClearCacheByIDs[E Entity](c Context, ids ...uint64) {
	schema := GetEntitySchema[E](c)
	localPool, has := schema.GetLocalCache()
	if has {
		for _, id := range ids {
			localPool.Remove(c, id)
		}
	}
	redisPool, has := schema.GetRedisCache()
	if has {
		redisPool.hDelUints(c, schema.GetCacheKey(), ids...)
	}
}
