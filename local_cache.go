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

type localCacheElement struct {
	value      any
	lruElement *list.Element
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
	getList(orm ORM, key string, id uint64) (value any, ok bool)
	setList(orm ORM, key string, id uint64, value any)
	removeList(orm ORM, key string, id uint64)
}

type localCache struct {
	config               *localCacheConfig
	cacheNoLimit         *xsync.Map
	cacheLimit           *xsync.MapOf[string, *localCacheElement]
	cacheLRU             *list.List
	cacheEntitiesNoLimit *xsync.MapOf[uint64, any]
	cacheEntitiesLimit   *xsync.MapOf[uint64, *localCacheElement]
	cacheEntitiesLRU     *list.List
	cacheListNoLimit     map[string]*xsync.MapOf[uint64, any]
	cacheListLimit       map[string]*xsync.MapOf[uint64, *localCacheElement]
	cacheListLRU         map[string]*list.List
	mutex                sync.Mutex
	evictions            uint64
	evictionsEntities    uint64
	evictionsList        map[string]*uint64
}

func newLocalCache(code string, limit int, schema *entitySchema) *localCache {
	c := &localCache{config: &localCacheConfig{code: code, limit: limit, schema: schema}}
	if limit > 0 {
		c.cacheLimit = xsync.NewMapOf[*localCacheElement]()
		c.cacheLRU = list.New()
	} else {
		c.cacheNoLimit = xsync.NewMap()
	}

	if schema != nil && schema.hasLocalCache {
		if limit > 0 {
			c.cacheEntitiesLimit = xsync.NewTypedMapOf[uint64, *localCacheElement](func(seed maphash.Seed, u uint64) uint64 {
				return u
			})
		} else {
			c.cacheEntitiesNoLimit = xsync.NewTypedMapOf[uint64, any](func(seed maphash.Seed, u uint64) uint64 {
				return u
			})
		}

		if limit > 0 {
			c.cacheEntitiesLRU = list.New()
		}
		if len(schema.cachedReferences) > 0 || len(schema.cachedIndexes) > 0 || schema.cacheAll {
			if limit > 0 {
				c.cacheListLimit = make(map[string]*xsync.MapOf[uint64, *localCacheElement])
				c.cacheListLRU = make(map[string]*list.List)
				c.evictionsList = make(map[string]*uint64)
			} else {
				c.cacheListNoLimit = make(map[string]*xsync.MapOf[uint64, any])
			}
			for reference := range schema.cachedReferences {
				initListCache(limit, c, reference)
			}
			for index := range schema.cachedIndexes {
				initListCache(limit, c, index)
			}
			if schema.cacheAll {
				if limit > 0 {
					c.cacheListLimit[cacheAllFakeReferenceKey] = xsync.NewTypedMapOf[uint64, *localCacheElement](func(seed maphash.Seed, u uint64) uint64 {
						return u
					})

					evictions := uint64(0)
					c.evictionsList[cacheAllFakeReferenceKey] = &evictions
					c.cacheListLRU[cacheAllFakeReferenceKey] = list.New()
				} else {
					c.cacheListNoLimit[cacheAllFakeReferenceKey] = xsync.NewTypedMapOf[uint64, any](func(seed maphash.Seed, u uint64) uint64 {
						return u
					})
				}
			}
		}
	}
	return c
}

func initListCache(limit int, c *localCache, reference string) {
	if limit > 0 {
		c.cacheListLimit[reference] = xsync.NewTypedMapOf[uint64, *localCacheElement](func(seed maphash.Seed, u uint64) uint64 {
			return u
		})
		evictions := uint64(0)
		c.evictionsList[reference] = &evictions
		c.cacheListLRU[reference] = list.New()
	} else {
		c.cacheListNoLimit[reference] = xsync.NewTypedMapOf[uint64, any](func(seed maphash.Seed, u uint64) uint64 {
			return u
		})
	}
}

func (lc *localCache) GetConfig() LocalCacheConfig {
	return lc.config
}

func (lc *localCache) Get(orm ORM, key string) (value any, ok bool) {
	if lc.config.limit > 0 {
		val, has := lc.cacheLimit.Load(key)
		hasLog, _ := orm.getLocalCacheLoggers()
		if hasLog {
			lc.fillLogFields(orm, "GET", fmt.Sprintf("GET %v", key), !has)
		}
		if has {
			if lc.cacheLimit.Size() >= lc.config.limit {
				lc.cacheLRU.MoveToFront(val.lruElement)
			}
			return val.value, true
		}
		return nil, false
	}
	value, ok = lc.cacheNoLimit.Load(key)
	hasLog, _ := orm.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(orm, "GET", fmt.Sprintf("GET %v", key), !ok)
	}
	return
}

func (lc *localCache) getEntity(orm ORM, id uint64) (value any, ok bool) {
	if lc.config.limit > 0 {
		val, has := lc.cacheEntitiesLimit.Load(id)
		hasLog, _ := orm.getLocalCacheLoggers()
		if hasLog {
			lc.fillLogFields(orm, "GET", fmt.Sprintf("GET ENTITY %d", id), !has)
		}
		if has {
			if lc.cacheEntitiesLimit.Size() >= lc.config.limit {
				lc.cacheEntitiesLRU.MoveToFront(val.lruElement)
			}
			return val.value, true
		}
		return nil, false
	}
	value, ok = lc.cacheEntitiesNoLimit.Load(id)
	hasLog, _ := orm.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(orm, "GET", fmt.Sprintf("GET ENTITY %d", id), !ok)
	}
	return
}

func (lc *localCache) getList(orm ORM, key string, id uint64) (value any, ok bool) {
	if lc.config.limit > 0 {
		c := lc.cacheListLimit[key]
		val, has := c.Load(id)
		hasLog, _ := orm.getLocalCacheLoggers()
		if hasLog {
			lc.fillLogFields(orm, "GET", fmt.Sprintf("GET LIST %s %d", key, id), !has)
		}
		if has {
			if c.Size() >= lc.config.limit {
				lc.cacheListLRU[key].MoveToFront(val.lruElement)
			}
			return val.value, true
		}
		return nil, false
	}
	value, ok = lc.cacheListNoLimit[key].Load(id)
	hasLog, _ := orm.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(orm, "GET", fmt.Sprintf("GET LIST %s %d", key, id), !ok)
	}
	return
}

func (lc *localCache) Set(orm ORM, key string, value any) {
	if lc.config.limit > 0 {
		element := lc.cacheLRU.PushFront(key)
		lc.cacheLimit.Store(key, &localCacheElement{lruElement: element, value: value})
		if lc.cacheLimit.Size() > lc.config.limit {
			toRemove := lc.cacheLRU.Back()
			if toRemove != nil {
				lc.cacheLimit.Delete(lc.cacheLRU.Remove(toRemove).(string))
				atomic.AddUint64(&lc.evictions, 1)
			}
		}
		hasLog, _ := orm.getLocalCacheLoggers()
		if hasLog {
			lc.fillLogFields(orm, "SET", fmt.Sprintf("SET %s %v", key, value), false)
		}
		return
	}
	lc.cacheNoLimit.Store(key, value)
	hasLog, _ := orm.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(orm, "SET", fmt.Sprintf("SET %s %v", key, value), false)
	}
}

func (lc *localCache) setEntity(orm ORM, id uint64, value any) {
	if lc.config.limit > 0 {
		element := lc.cacheEntitiesLRU.PushFront(id)
		lc.cacheEntitiesLimit.Store(id, &localCacheElement{lruElement: element, value: value})
		if lc.cacheEntitiesLimit.Size() > lc.config.limit {
			toRemove := lc.cacheEntitiesLRU.Back()
			if toRemove != nil {
				lc.cacheEntitiesLimit.Delete(lc.cacheEntitiesLRU.Remove(toRemove).(uint64))
				atomic.AddUint64(&lc.evictionsEntities, 1)
			}
		}
		hasLog, _ := orm.getLocalCacheLoggers()
		if hasLog {
			lc.fillLogFields(orm, "SET", fmt.Sprintf("SET ENTITY %d %v", id, value), false)
		}
		return
	}
	lc.cacheEntitiesNoLimit.Store(id, value)
	hasLog, _ := orm.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(orm, "SET", fmt.Sprintf("SET ENTITY %d [entity value]", id), false)
	}
}

func (lc *localCache) setList(orm ORM, key string, id uint64, value any) {
	if lc.config.limit > 0 {
		element := lc.cacheEntitiesLRU.PushFront(id)
		c := lc.cacheListLimit[key]
		c.Store(id, &localCacheElement{lruElement: element, value: value})
		lru := lc.cacheListLRU[key]
		if c.Size() > lc.config.limit {
			toRemove := lru.Back()
			if toRemove != nil {
				c.Delete(lc.cacheEntitiesLRU.Remove(toRemove).(uint64))
				atomic.AddUint64(lc.evictionsList[key], 1)
			}
		}
		hasLog, _ := orm.getLocalCacheLoggers()
		if hasLog {
			lc.fillLogFields(orm, "SET", fmt.Sprintf("SET LIST %d %v", id, value), false)
		}
		return
	}
	lc.cacheListNoLimit[key].Store(id, value)
	hasLog, _ := orm.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(orm, "SET", fmt.Sprintf("SET LIST %s %d %v", key, id, value), false)
	}
}

func (lc *localCache) Remove(orm ORM, key string) {
	if lc.config.limit > 0 {
		val, loaded := lc.cacheLimit.LoadAndDelete(key)
		if loaded {
			lc.cacheLRU.Remove(val.lruElement)
		}
	} else {
		lc.cacheNoLimit.Delete(key)
	}
	hasLog, _ := orm.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(orm, "REMOVE", fmt.Sprintf("REMOVE %s", key), false)
	}
}

func (lc *localCache) removeEntity(orm ORM, id uint64) {
	if lc.config.limit > 0 {
		val, loaded := lc.cacheEntitiesLimit.LoadAndDelete(id)
		if loaded {
			lc.cacheEntitiesLRU.Remove(val.lruElement)
		}
	} else {
		lc.cacheEntitiesNoLimit.Delete(id)
	}
	hasLog, _ := orm.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(orm, "REMOVE", fmt.Sprintf("REMOVE ENTITY %d", id), false)
	}
}

func (lc *localCache) removeList(orm ORM, key string, id uint64) {
	if lc.config.limit > 0 {
		val, loaded := lc.cacheListLimit[key].LoadAndDelete(id)
		if loaded {
			lc.cacheListLRU[key].Remove(val.lruElement)
		}
	} else {
		lc.cacheListNoLimit[key].Delete(id)
	}
	hasLog, _ := orm.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(orm, "REMOVE", fmt.Sprintf("REMOVE LIST %s %d", key, id), false)
	}
}

func (lc *localCache) Clear(orm ORM) {
	if lc.config.limit > 0 {
		lc.cacheLimit.Clear()
		lc.cacheLRU.Init()
		if lc.cacheEntitiesLimit != nil {
			lc.cacheEntitiesLimit.Clear()
			lc.cacheEntitiesLRU.Init()
		}
		if lc.cacheListLimit != nil {
			for name, cache := range lc.cacheListLimit {
				cache.Clear()
				lc.cacheListLRU[name].Init()
			}
		}

	} else {
		lc.cacheNoLimit.Clear()
		if lc.cacheEntitiesNoLimit != nil {
			lc.cacheEntitiesNoLimit.Clear()
		}
		if lc.cacheListNoLimit != nil {
			for _, cache := range lc.cacheListNoLimit {
				cache.Clear()
			}
		}
	}

	hasLog, _ := orm.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(orm, "CLEAR", "CLEAR", false)
	}
}

func (lc *localCache) GetUsage() []LocalCacheUsage {
	if lc.config.limit > 0 {
		if lc.cacheEntitiesLimit == nil {
			return []LocalCacheUsage{{Type: "Global", Used: uint64(lc.cacheLimit.Size()), Limit: uint64(lc.config.limit), Evictions: lc.evictions}}
		}
		usage := make([]LocalCacheUsage, len(lc.cacheListLimit)+1)
		usage[0] = LocalCacheUsage{Type: "Entities " + lc.config.schema.GetType().String(), Used: uint64(lc.cacheEntitiesLimit.Size()), Limit: uint64(lc.config.limit), Evictions: lc.evictionsEntities}
		i := 1
		for key, l := range lc.cacheListLimit {
			usage[i] = LocalCacheUsage{Type: "List " + key + " of " + lc.config.schema.GetType().String(), Used: uint64(l.Size()), Limit: uint64(lc.config.limit), Evictions: *lc.evictionsList[key]}
			i++
		}
		return usage
	}
	if lc.cacheEntitiesNoLimit == nil {
		return []LocalCacheUsage{{Type: "Global", Used: uint64(lc.cacheNoLimit.Size()), Limit: 0, Evictions: 0}}
	}
	usage := make([]LocalCacheUsage, len(lc.cacheListNoLimit)+1)
	usage[0] = LocalCacheUsage{Type: "Entities " + lc.config.schema.GetType().String(), Used: uint64(lc.cacheEntitiesNoLimit.Size()), Limit: 0, Evictions: 0}
	i := 1
	for refName, l := range lc.cacheListNoLimit {
		usage[i] = LocalCacheUsage{Type: "List " + refName + " of " + lc.config.schema.GetType().String(), Used: uint64(l.Size()), Limit: 0, Evictions: 0}
		i++
	}
	return usage
}

func (lc *localCache) fillLogFields(orm ORM, operation, query string, cacheMiss bool) {
	_, loggers := orm.getLocalCacheLoggers()
	fillLogFields(orm, loggers, lc.config.code, sourceLocalCache, operation, query, nil, cacheMiss, nil)
}
