package beeorm

import (
	"container/list"
	"fmt"
	"hash/maphash"
	"sync"
	"sync/atomic"

	"github.com/puzpuzpuz/xsync/v2"
)

type LocalCacheConfig interface {
	GetCode() string
	GetLimit() int
	GetSchema() EntitySchema
}

type localCacheConfig struct {
	code   string
	limit  int
	schema EntitySchema
}

func (c *localCacheConfig) GetCode() string {
	return c.code
}

func (c *localCacheConfig) GetLimit() int {
	return c.limit
}

func (c *localCacheConfig) GetSchema() EntitySchema {
	return c.schema
}

type LocalCacheUsage struct {
	Type      string
	Limit     uint64
	Used      uint64
	Evictions uint64
}

type LocalCache interface {
	Set(orm ORM, key string, value any)
	Remove(orm ORM, key string)
	GetConfig() LocalCacheConfig
	Get(orm ORM, key string) (value any, ok bool)
	Clear(orm ORM)
	GetUsage() []LocalCacheUsage
	getEntity(orm ORM, id uint64) (value any, ok bool)
	setEntity(orm ORM, id uint64, value any)
	removeEntity(orm ORM, id uint64)
	getReference(orm ORM, reference string, id uint64) (value any, ok bool)
	setReference(orm ORM, reference string, id uint64, value any)
	removeReference(orm ORM, reference string, id uint64)
}

type localCache struct {
	config              *localCacheConfig
	cache               *xsync.Map
	cacheEntities       *xsync.MapOf[uint64, any]
	cacheEntitiesLRU    *list.List
	cacheReferences     map[string]*xsync.MapOf[uint64, any]
	cacheReferencesLRU  map[string]*list.List
	mutex               sync.Mutex
	evictions           uint64
	evictionsEntities   uint64
	evictionsReferences map[string]*uint64
}

func newLocalCache(code string, limit int, schema *entitySchema) *localCache {
	c := &localCache{config: &localCacheConfig{code: code, limit: limit, schema: schema}}
	c.cache = xsync.NewMap()
	if schema != nil && schema.hasLocalCache {
		c.cacheEntities = xsync.NewTypedMapOf[uint64, any](func(seed maphash.Seed, u uint64) uint64 {
			return u
		})
		if limit > 0 {
			c.cacheEntitiesLRU = list.New()
		}
		if len(schema.cachedReferences) > 0 || schema.cacheAll {
			c.cacheReferences = make(map[string]*xsync.MapOf[uint64, any])
			c.cacheReferencesLRU = make(map[string]*list.List)
			c.evictionsReferences = make(map[string]*uint64)
			for reference := range schema.cachedReferences {
				c.cacheReferences[reference] = xsync.NewTypedMapOf[uint64, any](func(seed maphash.Seed, u uint64) uint64 {
					return u
				})
				evictions := uint64(0)
				c.evictionsReferences[reference] = &evictions
				c.cacheReferencesLRU[reference] = list.New()
			}
			if schema.cacheAll {
				c.cacheReferences[cacheAllFakeReferenceKey] = xsync.NewTypedMapOf[uint64, any](func(seed maphash.Seed, u uint64) uint64 {
					return u
				})
				evictions := uint64(0)
				c.evictionsReferences[cacheAllFakeReferenceKey] = &evictions
				c.cacheReferencesLRU[cacheAllFakeReferenceKey] = list.New()
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
		atomic.AddUint64(&lc.evictions, 1)
		lc.makeSpace(lc.cache, key)
	}
	hasLog, _ := orm.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(orm, "SET ENTITY", fmt.Sprintf("SET %s %v", key, value), false)
	}
}

func (lc *localCache) setEntity(orm ORM, id uint64, value any) {
	lc.cacheEntities.Store(id, value)
	if lc.config.limit > 0 {
		lc.cacheEntitiesLRU.MoveToFront(&list.Element{Value: id})
		if lc.cacheEntities.Size() > lc.config.limit {
			toRemove := lc.cacheEntitiesLRU.Back()
			if toRemove != nil {
				lc.cacheEntities.Delete(toRemove.Value.(uint64))
				atomic.AddUint64(&lc.evictionsEntities, 1)
			}
		}
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

func (lc *localCache) setReference(orm ORM, reference string, id uint64, value any) {
	c := lc.cacheReferences[reference]
	c.Store(id, value)
	if lc.config.limit > 0 {
		lru := lc.cacheReferencesLRU[reference]
		lru.MoveToFront(&list.Element{Value: id})
		if c.Size() > lc.config.limit {
			toRemove := lru.Back()
			if toRemove != nil {
				c.Delete(toRemove.Value.(uint64))
				atomic.AddUint64(lc.evictionsReferences[reference], 1)
			}
		}
	}
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

func (lc *localCache) GetUsage() []LocalCacheUsage {
	if lc.cacheEntities == nil {
		return []LocalCacheUsage{{Type: "Global", Used: uint64(lc.cache.Size()), Limit: uint64(lc.config.limit), Evictions: lc.evictions}}
	}
	usage := make([]LocalCacheUsage, len(lc.cacheReferences)+1)
	usage[0] = LocalCacheUsage{Type: "Entities " + lc.config.schema.GetType().String(), Used: uint64(lc.cacheEntities.Size()), Limit: uint64(lc.config.limit), Evictions: lc.evictionsEntities}
	i := 1
	for refName, references := range lc.cacheReferences {
		usage[i] = LocalCacheUsage{Type: "Reference " + refName + " of " + lc.config.schema.GetType().String(), Used: uint64(references.Size()), Limit: uint64(lc.config.limit), Evictions: *lc.evictionsReferences[refName]}
		i++
	}
	return usage
}

func (lc *localCache) fillLogFields(orm ORM, operation, query string, cacheMiss bool) {
	_, loggers := orm.getLocalCacheLoggers()
	fillLogFields(orm, loggers, lc.config.code, sourceLocalCache, operation, query, nil, cacheMiss, nil)
}
