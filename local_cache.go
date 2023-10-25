package beeorm

import (
	"fmt"
	"hash/maphash"

	"github.com/puzpuzpuz/xsync/v2"
)

type LocalCache interface {
	Set(c Context, key string, value interface{})
	Remove(c Context, key string)
	GetCode() string
	Get(c Context, key string) (value interface{}, ok bool)
	getEntity(c Context, id uint64) (value any, ok bool)
	setEntity(c Context, id uint64, value any)
	removeEntity(c Context, id uint64)
	getReference(c Context, reference string, id uint64) (value any, ok bool)
	setReference(c Context, reference string, id uint64, value any)
	removeReference(c Context, reference string, id uint64)
	Clear(c Context)
	GetObjectsCount() int
}

type localCache struct {
	code            string
	cache           *xsync.Map
	limit           int
	cacheEntities   *xsync.MapOf[uint64, any]
	cacheReferences map[string]*xsync.MapOf[uint64, any]
}

func newLocalCache(code string, limit int, schema *entitySchema) *localCache {
	c := &localCache{code: code, limit: limit}
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

func (lc *localCache) GetCode() string {
	return lc.code
}

func (lc *localCache) Get(c Context, key string) (value interface{}, ok bool) {
	value, ok = lc.cache.Load(key)
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "GET", fmt.Sprintf("GET %v", key), !ok)
	}
	return
}

func (lc *localCache) getEntity(c Context, id uint64) (value any, ok bool) {
	value, ok = lc.cacheEntities.Load(id)
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "GET", fmt.Sprintf("GET ENTITY %d", id), ok)
	}
	return
}

func (lc *localCache) getReference(c Context, reference string, id uint64) (value any, ok bool) {
	value, ok = lc.cacheReferences[reference].Load(id)
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "GET", fmt.Sprintf("GET REFERENCE %s %d", reference, id), ok)
	}
	return
}

func (lc *localCache) Set(c Context, key string, value interface{}) {
	lc.cache.Store(key, value)
	if lc.limit > 0 && lc.cache.Size() > lc.limit {
		lc.makeSpace(lc.cache, key)
	}
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "SET ENTITY", fmt.Sprintf("SET %s %v", key, value), false)
	}
}

func (lc *localCache) setEntity(c Context, id uint64, value any) {
	lc.cacheEntities.Store(id, value)
	if lc.limit > 0 && lc.cacheEntities.Size() > lc.limit {
		lc.makeSpaceUint(lc.cacheEntities, id)
	}
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "SET", fmt.Sprintf("SET ENTITY %d [entity value]", id), false)
	}
}

func (lc *localCache) makeSpace(cache *xsync.Map, addedKey string) {
	cache.Range(func(key string, value interface{}) bool {
		if key != addedKey {
			cache.Delete(key)
			return false
		}
		return true
	})
}

func (lc *localCache) makeSpaceUint(cache *xsync.MapOf[uint64, any], addedKey uint64) {
	cache.Range(func(key uint64, value interface{}) bool {
		if key != addedKey {
			cache.Delete(key)
			return false
		}
		return true
	})
}

func (lc *localCache) setReference(c Context, reference string, id uint64, value any) {
	lc.cacheReferences[reference].Store(id, value)
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "SET", fmt.Sprintf("SET REFERENCE %s %d %v", reference, id, value), false)
	}
}

func (lc *localCache) Remove(c Context, key string) {
	lc.cache.Delete(key)
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "REMOVE", fmt.Sprintf("REMOVE %s", key), false)
	}
}

func (lc *localCache) removeEntity(c Context, id uint64) {
	lc.cacheEntities.Delete(id)
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "REMOVE", fmt.Sprintf("REMOVE ENTITY %d", id), false)
	}
}

func (lc *localCache) removeReference(c Context, reference string, id uint64) {
	lc.cacheReferences[reference].Delete(id)
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "REMOVE", fmt.Sprintf("REMOVE REFERENCE %s %d", reference, id), false)
	}
}

func (lc *localCache) Clear(c Context) {
	lc.cache.Clear()
	if lc.cacheEntities != nil {
		lc.cacheEntities.Clear()
	}
	if lc.cacheReferences != nil {
		for _, cache := range lc.cacheReferences {
			cache.Clear()
		}
	}
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "CLEAR", "CLEAR", false)
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

func (lc *localCache) fillLogFields(c Context, operation, query string, cacheMiss bool) {
	_, loggers := c.getLocalCacheLoggers()
	fillLogFields(c, loggers, lc.code, sourceLocalCache, operation, query, nil, cacheMiss, nil)
}
