package beeorm

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/golang/groupcache/lru"
)

const requestCacheKey = "_request"

type LocalCachePoolConfig interface {
	GetCode() string
	GetLimit() int
}

type localCachePoolConfig struct {
	code  string
	limit int
	lru   *lru.Cache
	m     sync.Mutex
}

func (p *localCachePoolConfig) GetCode() string {
	return p.code
}

func (p *localCachePoolConfig) GetLimit() int {
	return p.limit
}

type LocalCache struct {
	engine *Engine
	config *localCachePoolConfig
}

type ttlValue struct {
	value interface{}
	time  int64
}

func (c *LocalCache) GetPoolConfig() LocalCachePoolConfig {
	return c.config
}

func (c *LocalCache) GetSet(key string, ttl time.Duration, provider func() interface{}) interface{} {
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

func (c *LocalCache) Get(key string) (value interface{}, ok bool) {
	func() {
		c.config.m.Lock()
		defer c.config.m.Unlock()
		value, ok = c.config.lru.Get(key)
	}()
	if c.engine.hasLocalCacheLogger {
		c.fillLogFields("GET", "GET "+key, !ok)
	}
	return
}

func (c *LocalCache) MGet(keys ...string) []interface{} {
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

func (c *LocalCache) Set(key string, value interface{}) {
	func() {
		c.config.m.Lock()
		defer c.config.m.Unlock()
		c.config.lru.Add(key, value)
	}()
	if c.engine.hasLocalCacheLogger {
		c.fillLogFields("SET", fmt.Sprintf("SET %s %v", key, value), false)
	}
}

func (c *LocalCache) MSet(pairs ...interface{}) {
	max := len(pairs)
	for i := 0; i < max; i += 2 {
		c.Set(pairs[i].(string), pairs[i+1])
	}
}

func (c *LocalCache) Remove(keys ...string) {
	for _, v := range keys {
		func() {
			c.config.m.Lock()
			defer c.config.m.Unlock()
			c.config.lru.Remove(v)
		}()
	}
	if c.engine.hasLocalCacheLogger {
		c.fillLogFields("REMOVE", "REMOVE "+strings.Join(keys, " "), false)
	}
}

func (c *LocalCache) Clear() {
	func() {
		c.config.m.Lock()
		defer c.config.m.Unlock()
		c.config.lru.Clear()
	}()
	if c.engine.hasLocalCacheLogger {
		c.fillLogFields("CLEAR", "CLEAR", false)
	}
}

func (c *LocalCache) GetObjectsCount() int {
	c.config.m.Lock()
	defer c.config.m.Unlock()
	return c.config.lru.Len()
}

func (c *LocalCache) fillLogFields(operation, query string, cacheMiss bool) {
	fillLogFields(c.engine.queryLoggersLocalCache, c.config.GetCode(), sourceLocalCache, operation, query, nil, cacheMiss, nil)
}
