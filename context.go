package beeorm

import (
	"context"
	"strings"
	"sync"
)

type Meta map[string]string

func (m Meta) Get(key string) string {
	return m[key]
}

type Context interface {
	Ctx() context.Context
	Clone() Context
	Engine() Engine
	Flush(lazy bool) error
	ClearFlush()
	RedisPipeLine(pool string) *RedisPipeLine
	RegisterQueryLogger(handler LogHandler, mysql, redis, local bool)
	EnableQueryDebug()
	EnableQueryDebugCustom(mysql, redis, local bool)
	SetPluginOption(plugin, key string, value interface{})
	GetPluginOption(plugin, key string) interface{}
	SetMetaData(key, value string)
	GetMetaData() Meta
	getDBLoggers() (bool, []LogHandler)
	getLocalCacheLoggers() (bool, []LogHandler)
	getRedisLoggers() (bool, []LogHandler)
	getStringBuilder() *strings.Builder
	getStringBuilder2() *strings.Builder
}

type contextImplementation struct {
	parent                 context.Context
	engine                 *engineImplementation
	trackedEntities        []EntityFlush
	queryLoggersDB         []LogHandler
	queryLoggersRedis      []LogHandler
	queryLoggersLocalCache []LogHandler
	hasRedisLogger         bool
	hasDBLogger            bool
	hasLocalCacheLogger    bool
	options                map[string]map[string]interface{}
	meta                   Meta
	stringBuilder          *strings.Builder
	stringBuilder2         *strings.Builder
	redisPipeLines         map[string]*RedisPipeLine
	flushDBActions         map[string][]func(db db)
	flushPostActions       []func()
	sync.Mutex
}

func (c *contextImplementation) getStringBuilder() *strings.Builder {
	if c.stringBuilder == nil {
		c.stringBuilder = &strings.Builder{}
	} else {
		c.stringBuilder.Reset()
	}
	return c.stringBuilder
}

func (c *contextImplementation) getStringBuilder2() *strings.Builder {
	if c.stringBuilder2 == nil {
		c.stringBuilder2 = &strings.Builder{}
	} else {
		c.stringBuilder2.Reset()
	}
	return c.stringBuilder2
}

func (c *contextImplementation) Ctx() context.Context {
	return c.parent
}

func (c *contextImplementation) Clone() Context {
	return &contextImplementation{
		parent:                 c.parent,
		engine:                 c.engine,
		queryLoggersDB:         c.queryLoggersDB,
		queryLoggersRedis:      c.queryLoggersRedis,
		queryLoggersLocalCache: c.queryLoggersLocalCache,
		hasRedisLogger:         c.hasRedisLogger,
		hasDBLogger:            c.hasDBLogger,
		hasLocalCacheLogger:    c.hasLocalCacheLogger,
		meta:                   c.meta,
		options:                c.options,
	}
}

func (c *contextImplementation) RedisPipeLine(pool string) *RedisPipeLine {
	if c.redisPipeLines == nil {
		c.redisPipeLines = make(map[string]*RedisPipeLine)
	}
	pipeline, has := c.redisPipeLines[pool]
	if !has {
		r := c.engine.Redis(pool).(*redisCache)
		pipeline = &RedisPipeLine{pool: pool, r: r, pipeLine: r.client.Pipeline()}
		c.redisPipeLines[pool] = pipeline
	}
	return pipeline
}

func (c *contextImplementation) SetPluginOption(plugin, key string, value interface{}) {
	if c.options == nil {
		c.options = map[string]map[string]interface{}{plugin: {key: value}}
	} else {
		before, has := c.options[plugin]
		if !has {
			c.options[plugin] = map[string]interface{}{key: value}
		} else {
			before[key] = value
		}
	}
}

func (c *contextImplementation) GetPluginOption(plugin, key string) interface{} {
	if c.options == nil {
		return nil
	}
	values, has := c.options[plugin]
	if !has {
		return nil
	}
	return values[key]
}

func (c *contextImplementation) SetMetaData(key, value string) {
	if c.meta == nil {
		c.meta = Meta{key: value}
		return
	}
	c.meta[key] = value
}

func (c *contextImplementation) GetMetaData() Meta {
	return c.meta
}

func (c *contextImplementation) Engine() Engine {
	return c.engine
}

func (c *contextImplementation) getRedisLoggers() (bool, []LogHandler) {
	if c.hasRedisLogger {
		return true, c.queryLoggersRedis
	}
	return false, nil
}

func (c *contextImplementation) getDBLoggers() (bool, []LogHandler) {
	if c.hasDBLogger {
		return true, c.queryLoggersDB
	}
	return false, nil
}

func (c *contextImplementation) getLocalCacheLoggers() (bool, []LogHandler) {
	if c.hasLocalCacheLogger {
		return true, c.queryLoggersLocalCache
	}
	return false, nil
}
