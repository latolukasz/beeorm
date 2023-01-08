package beeorm

type EntityCacheFlush struct {
	*EntitySQLFlush
	RedisDeletes      map[string][]string
	LocalCacheDeletes map[string][]string
}

func (el *EntityCacheFlush) PublishToStream(stream string, event interface{}) {
	//TODO
}

func (el *EntityCacheFlush) DeleteInRedis(pool string, key ...string) {
	if len(key) > 0 {
		deletes, has := el.RedisDeletes[pool]
		if !has {
			el.RedisDeletes[pool] = key
		} else {
			el.RedisDeletes[pool] = append(deletes, key...)
		}
	}

}

func (el *EntityCacheFlush) DeleteInLocalCache(pool string, key ...string) {
	if len(key) > 0 {
		deletes, has := el.LocalCacheDeletes[pool]
		if !has {
			el.LocalCacheDeletes[pool] = key
		} else {
			el.LocalCacheDeletes[pool] = append(deletes, key...)
		}
	}
}

func (el *EntityCacheFlush) AddInLocalCache(pool, key string, value interface{}) {
	// TODO
}
