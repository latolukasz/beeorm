package beeorm

import (
	"container/list"
	"fmt"
	"github.com/puzpuzpuz/xsync/v2"
	"hash/maphash"
	"reflect"
)

var emptyReflect reflect.Value

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

type LocalCacheSetter interface {
	Set(c Context, key string, value interface{})
	Remove(c Context, key string)
	setEntity(c Context, id uint64, value reflect.Value)
	removeEntity(c Context, id uint64)
}

type LocalCache interface {
	LocalCacheSetter
	GetPoolConfig() LocalCachePoolConfig
	Get(c Context, key string) (value interface{}, ok bool)
	getEntity(c Context, id uint64) (value reflect.Value, ok bool)
	Clear(c Context)
	GetObjectsCount() int
}

type localCache struct {
	config        *localCachePoolConfig
	ll            *list.List
	cache         *xsync.Map
	cacheEntities *xsync.MapOf[uint64, reflect.Value]
	storeEntities bool
}

type localCacheSetter struct {
	engine            Engine
	code              string
	setKeys           []string
	setEntities       []uint64
	setValues         []interface{}
	setValuesEntities []reflect.Value
	removes           []string
	removesEntities   []uint64
}

func newLocalCache(dbCode string, limit int, storeEntities bool) *localCache {
	c := &localCache{config: &localCachePoolConfig{code: dbCode, limit: limit}, storeEntities: storeEntities}
	if limit > 0 {
		c.ll = list.New()
	}
	c.cache = xsync.NewMap()
	if storeEntities {
		c.cacheEntities = xsync.NewTypedMapOf[uint64, reflect.Value](func(seed maphash.Seed, u uint64) uint64 {
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

func (lc *localCache) getEntity(c Context, id uint64) (value reflect.Value, ok bool) {
	value, ok = lc.cacheEntities.Load(id)
	return
}

func (lc *localCache) Set(c Context, key string, value interface{}) {
	lc.cache.Store(key, value)
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "SET", fmt.Sprintf("SET %s %v", key, value), false)
	}
}

func (lc *localCache) setEntity(c Context, id uint64, value reflect.Value) {
	lc.cacheEntities.Store(id, value)
}

func (lc *localCacheSetter) Set(_ Context, key string, value interface{}) {
	lc.setKeys = append(lc.setKeys, key)
	lc.setValues = append(lc.setValues, value)
}

func (lc *localCacheSetter) setEntity(_ Context, id uint64, value reflect.Value) {
	lc.setEntities = append(lc.setEntities, id)
	lc.setValuesEntities = append(lc.setValuesEntities, value)
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
}

func (lc *localCacheSetter) Remove(_ Context, key string) {
	lc.removes = append(lc.removes, key)
}

func (lc *localCacheSetter) removeEntity(_ Context, id uint64) {
	lc.removesEntities = append(lc.removesEntities, id)
}

func (lc *localCacheSetter) flush(c Context) {
	cache := lc.engine.LocalCache(lc.code)
	for i, key := range lc.setKeys {
		cache.Set(c, key, lc.setValues[i])
	}
	for i, key := range lc.setEntities {
		cache.setEntity(c, key, lc.setValuesEntities[i])
	}
	for _, key := range lc.removes {
		cache.Remove(c, key)
	}
	for _, key := range lc.removesEntities {
		cache.removeEntity(c, key)
	}
	lc.setKeys = nil
	lc.setValues = nil
	lc.setEntities = nil
	lc.setValuesEntities = nil
	lc.removes = nil
	lc.removesEntities = nil
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
