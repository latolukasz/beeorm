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
	CloneWithContext(ctx context.Context) Context
	Engine() Engine
	Flush() error
	FlushAsync() error
	ClearFlush()
	RedisPipeLine(pool string) *RedisPipeLine
	RegisterQueryLogger(handler LogHandler, mysql, redis, local bool)
	EnableQueryDebug()
	EnableQueryDebugCustom(mysql, redis, local bool)
	SetMetaData(key, value string)
	GetMetaData() Meta
	getDBLoggers() (bool, []LogHandler)
	getLocalCacheLoggers() (bool, []LogHandler)
	getRedisLoggers() (bool, []LogHandler)
	trackEntity(e EntityFlush)
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
	meta                   Meta
	stringBuilder          *strings.Builder
	stringBuilder2         *strings.Builder
	redisPipeLines         map[string]*RedisPipeLine
	flushDBActions         map[string][]func(db DBBase)
	flushPostActions       []func()
	mutexFlush             sync.Mutex
	mutexData              sync.Mutex
}

func (c *contextImplementation) Ctx() context.Context {
	return c.parent
}

func (c *contextImplementation) CloneWithContext(ctx context.Context) Context {
	return &contextImplementation{
		parent:                 ctx,
		engine:                 c.engine,
		queryLoggersDB:         c.queryLoggersDB,
		queryLoggersRedis:      c.queryLoggersRedis,
		queryLoggersLocalCache: c.queryLoggersLocalCache,
		hasRedisLogger:         c.hasRedisLogger,
		hasDBLogger:            c.hasDBLogger,
		hasLocalCacheLogger:    c.hasLocalCacheLogger,
		meta:                   c.meta,
	}
}

func (c *contextImplementation) Clone() Context {
	return c.CloneWithContext(c.parent)
}

func (c *contextImplementation) RedisPipeLine(pool string) *RedisPipeLine {
	if c.redisPipeLines != nil {
		pipeline, has := c.redisPipeLines[pool]
		if has {
			return pipeline
		}
	}
	c.mutexData.Lock()
	defer c.mutexData.Unlock()
	if c.redisPipeLines == nil {
		c.redisPipeLines = make(map[string]*RedisPipeLine)
	}
	r := c.engine.Redis(pool).(*redisCache)
	pipeline := &RedisPipeLine{c: c, pool: pool, r: r, pipeLine: r.client.Pipeline()}
	c.redisPipeLines[pool] = pipeline
	return pipeline
}

func (c *contextImplementation) SetMetaData(key, value string) {
	c.mutexData.Lock()
	defer c.mutexData.Unlock()
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

func (c *contextImplementation) trackEntity(e EntityFlush) {
	c.mutexFlush.Lock()
	defer c.mutexFlush.Unlock()
	c.trackedEntities = append(c.trackedEntities, e)
}
