package beeorm

func clearByIDs(engine *engineImplementation, entity Entity, ids ...uint64) {
	schema := initIfNeeded(engine.registry, entity).entitySchema
	localPool, has := schema.GetLocalCache(engine)
	if has {
		for _, id := range ids {
			localPool.Remove(id)
		}
	}
	redisPool, has := schema.GetRedisCache(engine)
	if has {
		redisPool.hDelUints(schema.cachePrefix, ids...)
	}
}
