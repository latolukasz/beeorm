package beeorm

import (
	"bytes"
	"context"
	"sync"
)

type Context interface {
	Ctx() context.Context
	Clone() Context
	Flusher() Flusher
	Engine() Engine
	EventBroker() EventBroker
	RegisterQueryLogger(handler LogHandler, mysql, redis, local bool)
	EnableQueryDebug()
	EnableQueryDebugCustom(mysql, redis, local bool)
	SetPluginOption(plugin, key string, value interface{})
	GetPluginOption(plugin, key string) interface{}
	SetMetaData(key, value string)
	GetMetaData() Meta
	HasRedisLogger() (bool, []LogHandler)
	getSerializer() *serializer
}

type contextImplementation struct {
	parent                 context.Context
	engine                 *engineImplementation
	flusher                Flusher
	queryLoggersDB         []LogHandler
	queryLoggersRedis      []LogHandler
	queryLoggersLocalCache []LogHandler
	hasRedisLogger         bool
	hasDBLogger            bool
	hasLocalCacheLogger    bool
	eventBroker            *eventBroker
	options                map[string]map[string]interface{}
	meta                   Meta
	serializer             *serializer
	sync.Mutex
}

func CreateContext(parent context.Context) Context {
	c := &contextImplementation{parent: parent}
	return c
}

func (c *contextImplementation) getSerializer() *serializer {
	if c.serializer == nil {
		c.serializer = &serializer{buffer: new(bytes.Buffer)}
	} else {
		c.serializer.buffer.Reset()
	}
	return c.serializer
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

func (c *contextImplementation) Flusher() Flusher {
	if c.flusher == nil {
		c.flusher = &flusher{c: c}
	}
	return c.flusher
}

func (c *contextImplementation) HasRedisLogger() (bool, []LogHandler) {
	if c.hasRedisLogger {
		return true, c.queryLoggersRedis
	}
	return false, nil
}
