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
	Set(key interface{}, value interface{})
	MSet(pairs ...interface{})
	Remove(keys ...interface{})
}

type LocalCache interface {
	LocalCacheSetter
	GetPoolConfig() LocalCachePoolConfig
	GetSet(key interface{}, ttl time.Duration, provider func() interface{}) interface{}
	Get(key interface{}) (value interface{}, ok bool)
	Clear()
	GetObjectsCount() int
}

type localCache struct {
	engine *engineImplementation
	config *localCachePoolConfig
	mutex  sync.Mutex
}

type localCacheSetter struct {
	engine    *engineImplementation
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

func (c *localCache) GetPoolConfig() LocalCachePoolConfig {
	return c.config
}

func (c *localCache) GetSet(key interface{}, ttl time.Duration, provider func() interface{}) interface{} {
	val, has := c.Get(key)
	if has {
		ttlVal := val.(ttlValue)
		seconds := int64(ttl.Seconds())
		if seconds == 0 || time.Now().Unix()-ttlVal.time <= seconds {
			return ttlVal.value
		}
	}
	userVal := provider()
	val = ttlValue{value: userVal, time: time.Now().Unix()}
	c.Set(key, val)
	return userVal
}

func (c *localCache) Get(key interface{}) (value interface{}, ok bool) {
	func() {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		value, ok = c.config.lru.Get(key)
	}()
	if c.engine.hasLocalCacheLogger {
		c.fillLogFields("GET", fmt.Sprintf("GET %v", key), !ok)
	}
	return
}

func (c *localCache) Set(key interface{}, value interface{}) {
	func() {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		c.config.lru.Add(key, value)
	}()
	if c.engine.hasLocalCacheLogger {
		c.fillLogFields("SET", fmt.Sprintf("SET %s %v", key, value), false)
	}
}

func (c *localCacheSetter) Set(key interface{}, value interface{}) {
	c.setKeys = append(c.setKeys, key)
	c.setValues = append(c.setValues, value)
}

func (c *localCache) MSet(pairs ...interface{}) {
	for i := 0; i < len(pairs); i += 2 {
		c.Set(pairs[i], pairs[i+1])
	}
}

func (c *localCacheSetter) MSet(pairs ...interface{}) {
	for i := 0; i < len(pairs); i += 2 {
		c.setKeys = append(c.setKeys, pairs[i])
		c.setValues = append(c.setValues, pairs[i+1])
	}
}

func (c *localCache) Remove(keys ...interface{}) {
	for _, v := range keys {
		func() {
			c.mutex.Lock()
			defer c.mutex.Unlock()
			c.config.lru.Remove(v)
		}()
	}
	if c.engine.hasLocalCacheLogger {
		c.fillLogFields("REMOVE", fmt.Sprintf("REMOVE %v", keys), false)
	}
}

func (c *localCacheSetter) Remove(keys ...interface{}) {
	c.removes = append(c.removes, keys...)
}

func (c *localCacheSetter) flush() {
	if c.setKeys == nil && c.removes == nil {
		return
	}
	cache := c.engine.GetLocalCache(c.code)
	for i, key := range c.setKeys {
		cache.Set(key, c.setValues[i])
	}
	if c.removes != nil {
		cache.Remove(c.removes...)
	}
	c.setKeys = nil
	c.setValues = nil
	c.removes = nil
}

func (c *localCache) Clear() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.config.lru.Clear()
	if c.engine.hasLocalCacheLogger {
		c.fillLogFields("CLEAR", "CLEAR", false)
	}
}

func (c *localCache) GetObjectsCount() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.config.lru.Len()
}

func (c *localCache) fillLogFields(operation, query string, cacheMiss bool) {
	fillLogFields(c.engine, c.engine.queryLoggersLocalCache, c.config.GetCode(), sourceLocalCache, operation, query, nil, cacheMiss, nil)
}
