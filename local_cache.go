package beeorm

import (
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"

	"github.com/golang/groupcache/lru"
)

const requestCacheKey = "_request"
const localCachePools = 100

type LocalCachePoolConfig interface {
	GetCode() string
	GetLimit() int
}

type localCacheLruMutex struct {
	Lru *lru.Cache
	M   sync.Mutex
}

type localCachePoolConfig struct {
	code  string
	limit int
	lru   []*localCacheLruMutex
}

func (p *localCachePoolConfig) GetCode() string {
	return p.code
}

func (p *localCachePoolConfig) GetLimit() int {
	return p.limit
}

type LocalCacheSetter interface {
	Set(key string, value interface{})
	MSet(pairs ...interface{})
	Remove(keys ...string)
}

type LocalCache interface {
	LocalCacheSetter
	GetPoolConfig() LocalCachePoolConfig
	GetSet(key string, ttl time.Duration, provider func() interface{}) interface{}
	Get(key string) (value interface{}, ok bool)
	MGet(keys ...string) []interface{}
	Clear()
	GetObjectsCount() int
}

type localCache struct {
	engine *engineImplementation
	config *localCachePoolConfig
}

type localCacheSetter struct {
	engine    *engineImplementation
	code      string
	setKeys   []string
	setValues []interface{}
	removes   []string
}

func newLocalCacheConfig(dbCode string, limit int) *localCachePoolConfig {
	pools := make([]*localCacheLruMutex, localCachePools)
	for i := 0; i < localCachePools; i++ {
		pools[i] = &localCacheLruMutex{Lru: lru.New(limit)}
	}
	return &localCachePoolConfig{code: dbCode, limit: limit, lru: pools}
}

type ttlValue struct {
	value interface{}
	time  int64
}

func (c *localCache) GetPoolConfig() LocalCachePoolConfig {
	return c.config
}

func (c *localCache) GetSet(key string, ttl time.Duration, provider func() interface{}) interface{} {
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

func (c *localCache) Get(key string) (value interface{}, ok bool) {
	mut := c.getLruMutex(key)
	func() {
		mut.M.Lock()
		defer mut.M.Unlock()
		value, ok = mut.Lru.Get(key)
	}()
	if c.engine.hasLocalCacheLogger {
		c.fillLogFields("GET", "GET "+key, !ok)
	}
	return
}

func (c *localCache) MGet(keys ...string) []interface{} {
	results := make([]interface{}, len(keys))
	misses := 0
	for i, key := range keys {
		value, ok := c.Get(key)
		if !ok {
			misses++
			value = nil
		}
		results[i] = value
	}
	return results
}

func (c *localCache) Set(key string, value interface{}) {
	mut := c.getLruMutex(key)
	func() {
		mut.M.Lock()
		defer mut.M.Unlock()
		mut.Lru.Add(key, value)
	}()
	if c.engine.hasLocalCacheLogger {
		c.fillLogFields("SET", fmt.Sprintf("SET %s %v", key, value), false)
	}
}

func (c *localCacheSetter) Set(key string, value interface{}) {
	c.setKeys = append(c.setKeys, key)
	c.setValues = append(c.setValues, value)
}

func (c *localCache) MSet(pairs ...interface{}) {
	for i := 0; i < len(pairs); i += 2 {
		c.Set(pairs[i].(string), pairs[i+1])
	}
}

func (c *localCacheSetter) MSet(pairs ...interface{}) {
	for i := 0; i < len(pairs); i += 2 {
		c.setKeys = append(c.setKeys, pairs[i].(string))
		c.setValues = append(c.setValues, pairs[i+1])
	}
}

func (c *localCache) Remove(keys ...string) {
	for _, v := range keys {
		mut := c.getLruMutex(v)
		func() {
			mut.M.Lock()
			defer mut.M.Unlock()
			mut.Lru.Remove(v)
		}()
	}
	if c.engine.hasLocalCacheLogger {
		c.fillLogFields("REMOVE", "REMOVE "+strings.Join(keys, " "), false)
	}
}

func (c *localCacheSetter) Remove(keys ...string) {
	c.removes = append(c.removes, keys...)
}

func (c *localCacheSetter) flush() {
	if c.setKeys == nil && c.removes == nil {
		return
	}
	cache := c.engine.GetLocalCache(c.code)
	for i, key := range c.removes {
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
	for _, mut := range c.config.lru {
		func() {
			mut.M.Lock()
			defer mut.M.Unlock()
			mut.Lru.Clear()
		}()
	}
	if c.engine.hasLocalCacheLogger {
		c.fillLogFields("CLEAR", "CLEAR", false)
	}
}

func (c *localCache) GetObjectsCount() int {
	total := 0
	for _, mut := range c.config.lru {
		func() {
			mut.M.Lock()
			defer mut.M.Unlock()
			total += mut.Lru.Len()
		}()
	}
	return total
}

func (c *localCache) getLruMutex(s string) *localCacheLruMutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	modulo := h.Sum32() % localCachePools
	return c.config.lru[modulo]
}

func (c *localCache) fillLogFields(operation, query string, cacheMiss bool) {
	fillLogFields(c.engine.queryLoggersLocalCache, c.config.GetCode(), sourceLocalCache, operation, query, nil, cacheMiss, nil)
}
