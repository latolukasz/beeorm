package beeorm

import (
	"fmt"
	"hash/maphash"
	"sync"

	"github.com/puzpuzpuz/xsync/v2"
)

type LocalCacheConfig interface {
	GetCode() string
	GetLimit() int
}

type localCacheConfig struct {
	code  string
	limit int
}

func (c *localCacheConfig) GetCode() string {
	return c.code
}

func (c *localCacheConfig) GetLimit() int {
	return c.limit
}

type LocalCache interface {
	Set(orm ORM, key string, value any)
	Remove(orm ORM, key string)
	GetConfig() LocalCacheConfig
	Get(orm ORM, key string) (value any, ok bool)
	Clear(orm ORM)
	GetObjectsCount() int
	getEntity(orm ORM, id uint64) (value any, ok bool)
	setEntity(orm ORM, id uint64, value any)
	removeEntity(orm ORM, id uint64)
	getReference(orm ORM, reference string, id uint64) (value any, ok bool)
	setReference(orm ORM, reference string, id uint64, value any)
	removeReference(orm ORM, reference string, id uint64)
}

type localCache struct {
	config          *localCacheConfig
	cache           *xsync.Map
	cacheEntities   *xsync.MapOf[uint64, any]
	cacheReferences map[string]*xsync.MapOf[uint64, any]
	mutex           sync.Mutex
}

func newLocalCache(code string, limit int, schema *entitySchema) *localCache {
	c := &localCache{config: &localCacheConfig{code: code, limit: limit}}
	c.cache = xsync.NewMap()
	if schema != nil && schema.hasLocalCache {
		c.cacheEntities = xsync.NewTypedMapOf[uint64, any](func(seed maphash.Seed, u uint64) uint64 {
			return u
		})
		if len(schema.cachedReferences) > 0 || schema.cacheAll {
			c.cacheReferences = make(map[string]*xsync.MapOf[uint64, any])
			for reference := range schema.cachedReferences {
				c.cacheReferences[reference] = xsync.NewTypedMapOf[uint64, any](func(seed maphash.Seed, u uint64) uint64 {
					return u
				})
			}
			if schema.cacheAll {
				c.cacheReferences[cacheAllFakeReferenceKey] = xsync.NewTypedMapOf[uint64, any](func(seed maphash.Seed, u uint64) uint64 {
					return u
				})
			}
		}
	}
	return c
}

func (lc *localCache) GetConfig() LocalCacheConfig {
	return lc.config
}

func (lc *localCache) Get(orm ORM, key string) (value any, ok bool) {
	value, ok = lc.cache.Load(key)
	hasLog, _ := orm.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(orm, "GET", fmt.Sprintf("GET %v", key), !ok)
	}
	return
}

func (lc *localCache) getEntity(orm ORM, id uint64) (value any, ok bool) {
	value, ok = lc.cacheEntities.Load(id)
	hasLog, _ := orm.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(orm, "GET", fmt.Sprintf("GET ENTITY %d", id), ok)
	}
	return
}

func (lc *localCache) getReference(orm ORM, reference string, id uint64) (value any, ok bool) {
	value, ok = lc.cacheReferences[reference].Load(id)
	hasLog, _ := orm.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(orm, "GET", fmt.Sprintf("GET REFERENCE %s %d", reference, id), ok)
	}
	return
}

func (lc *localCache) Set(orm ORM, key string, value any) {
	lc.cache.Store(key, value)
	if lc.config.limit > 0 && lc.cache.Size() > lc.config.limit {
		lc.makeSpace(lc.cache, key)
	}
	hasLog, _ := orm.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(orm, "SET ENTITY", fmt.Sprintf("SET %s %v", key, value), false)
	}
}

func (lc *localCache) setEntity(orm ORM, id uint64, value any) {
	lc.cacheEntities.Store(id, value)
	if lc.config.limit > 0 && lc.cacheEntities.Size() > lc.config.limit {
		lc.makeSpaceUint(lc.cacheEntities, id)
	}
	hasLog, _ := orm.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(orm, "SET", fmt.Sprintf("SET ENTITY %d [entity value]", id), false)
	}
}

func (lc *localCache) makeSpace(cache *xsync.Map, addedKey string) {
	cache.Range(func(key string, value any) bool {
		if key != addedKey {
			cache.Delete(key)
			return false
		}
		return true
	})
}

func (lc *localCache) makeSpaceUint(cache *xsync.MapOf[uint64, any], addedKey uint64) {
	cache.Range(func(key uint64, value any) bool {
		if key != addedKey {
			cache.Delete(key)
			return false
		}
		return true
	})
}

func (lc *localCache) setReference(orm ORM, reference string, id uint64, value any) {
	lc.cacheReferences[reference].Store(id, value)
	hasLog, _ := orm.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(orm, "SET", fmt.Sprintf("SET REFERENCE %s %d %v", reference, id, value), false)
	}
}

func (lc *localCache) Remove(orm ORM, key string) {
	lc.cache.Delete(key)
	hasLog, _ := orm.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(orm, "REMOVE", fmt.Sprintf("REMOVE %s", key), false)
	}
}

func (lc *localCache) removeEntity(orm ORM, id uint64) {
	lc.cacheEntities.Delete(id)
	hasLog, _ := orm.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(orm, "REMOVE", fmt.Sprintf("REMOVE ENTITY %d", id), false)
	}
}

func (lc *localCache) removeReference(orm ORM, reference string, id uint64) {
	lc.cacheReferences[reference].Delete(id)
	hasLog, _ := orm.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(orm, "REMOVE", fmt.Sprintf("REMOVE REFERENCE %s %d", reference, id), false)
	}
}

func (lc *localCache) Clear(orm ORM) {
	lc.cache.Clear()
	if lc.cacheEntities != nil {
		lc.cacheEntities.Clear()
	}
	if lc.cacheReferences != nil {
		for _, cache := range lc.cacheReferences {
			cache.Clear()
		}
	}
	hasLog, _ := orm.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(orm, "CLEAR", "CLEAR", false)
	}
}

func (lc *localCache) GetObjectsCount() int {
	total := lc.cache.Size()
	if lc.cacheEntities != nil {
		total += lc.cacheEntities.Size()
	}
	if lc.cacheReferences != nil {
		for _, cache := range lc.cacheReferences {
			total += cache.Size()
		}
	}
	return total
}

func (lc *localCache) fillLogFields(orm ORM, operation, query string, cacheMiss bool) {
	_, loggers := orm.getLocalCacheLoggers()
	fillLogFields(orm, loggers, lc.config.code, sourceLocalCache, operation, query, nil, cacheMiss, nil)
}
