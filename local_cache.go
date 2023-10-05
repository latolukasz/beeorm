package beeorm

import (
	"container/list"
	"fmt"
	"github.com/puzpuzpuz/xsync/v2"
	"hash/maphash"
)

type LocalCachePoolConfig interface {
	GetCode() string
	GetLimit() int
}

type localCachePoolConfig struct {
	code  string
	limit int
}

func (p *localCachePoolConfig) GetCode() string {
	return p.code
}

func (p *localCachePoolConfig) GetLimit() int {
	return p.limit
}

type LocalCache interface {
	Set(c Context, key string, value interface{})
	Remove(c Context, key string)
	GetPoolConfig() LocalCachePoolConfig
	Get(c Context, key string) (value interface{}, ok bool)
	getEntity(c Context, id uint64) (value any, ok bool)
	setEntity(c Context, id uint64, value any)
	removeEntity(c Context, id uint64)
	getReference(c Context, id uint64) (value any, ok bool)
	setReference(c Context, id uint64, value any)
	removeReference(c Context, id uint64)
	Clear(c Context)
	GetObjectsCount() int
}

type localCache struct {
	config          *localCachePoolConfig
	ll              *list.List
	cache           *xsync.Map
	cacheEntities   *xsync.MapOf[uint64, any]
	cacheReferences *xsync.MapOf[uint64, any]
	storeEntities   bool
}

func newLocalCache(dbCode string, limit int, storeEntities, storeReferences bool) *localCache {
	c := &localCache{config: &localCachePoolConfig{code: dbCode, limit: limit}, storeEntities: storeEntities}
	if limit > 0 {
		c.ll = list.New()
	}
	c.cache = xsync.NewMap()
	if storeEntities {
		c.cacheEntities = xsync.NewTypedMapOf[uint64, any](func(seed maphash.Seed, u uint64) uint64 {
			return u
		})
	}
	if storeReferences {
		c.cacheReferences = xsync.NewTypedMapOf[uint64, any](func(seed maphash.Seed, u uint64) uint64 {
			return u
		})
	}
	return c
}

func (lc *localCache) GetPoolConfig() LocalCachePoolConfig {
	return lc.config
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

func (lc *localCache) getReference(c Context, id uint64) (value any, ok bool) {
	value, ok = lc.cacheReferences.Load(id)
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "GET", fmt.Sprintf("GET REFERENCE %d", id), ok)
	}
	return
}

func (lc *localCache) Set(c Context, key string, value interface{}) {
	lc.cache.Store(key, value)
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "SET ENTITY", fmt.Sprintf("SET %s %v", key, value), false)
	}
}

func (lc *localCache) setEntity(c Context, id uint64, value any) {
	lc.cacheEntities.Store(id, value)
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "SET", fmt.Sprintf("SET ENTITY %d [entity value]", id), false)
	}
}

func (lc *localCache) setReference(c Context, id uint64, value any) {
	lc.cacheReferences.Store(id, value)
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "SET", fmt.Sprintf("SET REFERENCE %d [entity value]", id), false)
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

func (lc *localCache) removeReference(c Context, id uint64) {
	lc.cacheReferences.Delete(id)
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "REMOVE", fmt.Sprintf("REMOVE REFERENCE ENTITY %d", id), false)
	}
}

func (lc *localCache) Clear(c Context) {
	lc.cache.Clear()
	if lc.storeEntities {
		lc.cacheEntities.Clear()
	}
	if lc.config.limit > 0 {
		lc.ll = list.New()
	}
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "CLEAR", "CLEAR", false)
	}
}

func (lc *localCache) GetObjectsCount() int {
	total := lc.cache.Size()
	if lc.storeEntities {
		total += lc.cacheEntities.Size()
	}
	return total
}

func (lc *localCache) fillLogFields(c Context, operation, query string, cacheMiss bool) {
	_, loggers := c.getLocalCacheLoggers()
	fillLogFields(c, loggers, lc.config.GetCode(), sourceLocalCache, operation, query, nil, cacheMiss, nil)
}
