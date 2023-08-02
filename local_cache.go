package beeorm

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/groupcache/lru"
)

type LocalCachePoolConfig interface {
	GetCode() string
	GetLimit() int
}

type localCachePoolConfig struct {
	code  string
	limit int
	lru   *lru.Cache
}

func (p *localCachePoolConfig) GetCode() string {
	return p.code
}

func (p *localCachePoolConfig) GetLimit() int {
	return p.limit
}

type LocalCacheSetter interface {
	Set(c Context, key interface{}, value interface{})
	MSet(c Context, pairs ...interface{})
	Remove(c Context, keys ...interface{})
}

type LocalCache interface {
	LocalCacheSetter
	GetPoolConfig() LocalCachePoolConfig
	GetSet(c Context, key interface{}, ttl time.Duration, provider func() interface{}) interface{}
	Get(c Context, key interface{}) (value interface{}, ok bool)
	Clear(c Context)
	GetObjectsCount() int
}

type localCache struct {
	engine Engine
	config *localCachePoolConfig
	mutex  sync.Mutex
}

type localCacheSetter struct {
	engine    Engine
	code      string
	setKeys   []interface{}
	setValues []interface{}
	removes   []interface{}
}

func newLocalCacheConfig(dbCode string, limit int) *localCachePoolConfig {
	return &localCachePoolConfig{code: dbCode, limit: limit, lru: lru.New(limit)}
}

type ttlValue struct {
	value interface{}
	time  int64
}

func (lc *localCache) GetPoolConfig() LocalCachePoolConfig {
	return lc.config
}

func (lc *localCache) GetSet(c Context, key interface{}, ttl time.Duration, provider func() interface{}) interface{} {
	val, has := lc.Get(c, key)
	if has {
		ttlVal := val.(ttlValue)
		seconds := int64(ttl.Seconds())
		if seconds == 0 || time.Now().Unix()-ttlVal.time <= seconds {
			return ttlVal.value
		}
	}
	userVal := provider()
	val = ttlValue{value: userVal, time: time.Now().Unix()}
	lc.Set(c, key, val)
	return userVal
}

func (lc *localCache) Get(c Context, key interface{}) (value interface{}, ok bool) {
	func() {
		lc.mutex.Lock()
		defer lc.mutex.Unlock()
		value, ok = lc.config.lru.Get(key)
	}()
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "GET", fmt.Sprintf("GET %v", key), !ok)
	}
	return
}

func (lc *localCache) Set(c Context, key interface{}, value interface{}) {
	func() {
		lc.mutex.Lock()
		defer lc.mutex.Unlock()
		lc.config.lru.Add(key, value)
	}()
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "SET", fmt.Sprintf("SET %s %v", key, value), false)
	}
}

func (lc *localCacheSetter) Set(_ Context, key interface{}, value interface{}) {
	lc.setKeys = append(lc.setKeys, key)
	lc.setValues = append(lc.setValues, value)
}

func (lc *localCache) MSet(c Context, pairs ...interface{}) {
	for i := 0; i < len(pairs); i += 2 {
		lc.Set(c, pairs[i], pairs[i+1])
	}
}

func (lc *localCacheSetter) MSet(_ Context, pairs ...interface{}) {
	for i := 0; i < len(pairs); i += 2 {
		lc.setKeys = append(lc.setKeys, pairs[i])
		lc.setValues = append(lc.setValues, pairs[i+1])
	}
}

func (lc *localCache) Remove(c Context, keys ...interface{}) {
	for _, v := range keys {
		func() {
			lc.mutex.Lock()
			defer lc.mutex.Unlock()
			lc.config.lru.Remove(v)
		}()
	}
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "REMOVE", fmt.Sprintf("REMOVE %v", keys), false)
	}
}

func (lc *localCacheSetter) Remove(_ Context, keys ...interface{}) {
	lc.removes = append(lc.removes, keys...)
}

func (lc *localCacheSetter) flush(c Context) {
	if lc.setKeys == nil && lc.removes == nil {
		return
	}
	cache := lc.engine.LocalCache(lc.code)
	for i, key := range lc.setKeys {
		cache.Set(c, key, lc.setValues[i])
	}
	if lc.removes != nil {
		cache.Remove(c, lc.removes...)
	}
	lc.setKeys = nil
	lc.setValues = nil
	lc.removes = nil
}

func (lc *localCache) Clear(c Context) {
	lc.mutex.Lock()
	defer lc.mutex.Unlock()
	lc.config.lru.Clear()
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "CLEAR", "CLEAR", false)
	}
}

func (lc *localCache) GetObjectsCount() int {
	lc.mutex.Lock()
	defer lc.mutex.Unlock()
	return lc.config.lru.Len()
}

func (lc *localCache) fillLogFields(c Context, operation, query string, cacheMiss bool) {
	_, loggers := c.getLocalCacheLoggers()
	fillLogFields(c, loggers, lc.config.GetCode(), sourceLocalCache, operation, query, nil, cacheMiss, nil)
}
