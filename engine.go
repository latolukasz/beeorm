package beeorm

import (
	"context"
	"reflect"
)

type Engine interface {
	NewContext(parent context.Context) Context
	GetEntitySchema(entity any) EntitySchema
	Registry() *Registry
	GetEnum(code string) Enum
	GetRedisStreams() map[string]map[string][]string
	GetMySQL(code string) *DB
	GetMySQLPools() map[string]*DB
	GetLocalCache(code string) LocalCache
	GetLocalCachePools() map[string]LocalCache
	GetRedis(code string) RedisCache
	GetRedisPools() map[string]RedisCache
	GetEntities() map[string]reflect.Type
	GetPlugins() []string
	GetPlugin(code string) Plugin
	getDefaultQueryLogger() LogHandler
	getRedisStreamsForGroup(group string) []string
}

type engineImplementation struct {
	registry           *Registry
	entitySchemas      map[reflect.Type]*entitySchema
	entities           map[string]reflect.Type
	localCacheServers  map[string]LocalCache
	mySQLServers       map[string]*DB
	redisServers       map[string]RedisCache
	redisStreamGroups  map[string]map[string]map[string]bool
	redisStreamPools   map[string]string
	enums              map[string]Enum
	plugins            []Plugin
	defaultQueryLogger *defaultLogLogger
}

func (e *engineImplementation) NewContext(parent context.Context) Context {
	return &contextImplementation{parent: parent, engine: e}
}

func (e *engineImplementation) Registry() *Registry {
	return e.registry
}

func (e *engineImplementation) GetEntities() map[string]reflect.Type {
	return e.entities
}

func (e *engineImplementation) GetPlugins() []string {
	codes := make([]string, len(e.plugins))
	for i, plugin := range e.plugins {
		codes[i] = plugin.GetCode()
	}
	return codes
}

func (e *engineImplementation) GetPlugin(code string) Plugin {
	for _, plugin := range e.plugins {
		if plugin.GetCode() == code {
			return plugin
		}
	}
	return nil
}

func (e *engineImplementation) GetRedisStreams() map[string]map[string][]string {
	res := make(map[string]map[string][]string)
	for redisPool, row := range e.redisStreamGroups {
		res[redisPool] = make(map[string][]string)
		for stream, groups := range row {
			res[redisPool][stream] = make([]string, len(groups))
			i := 0
			for group := range groups {
				res[redisPool][stream][i] = group
				i++
			}
		}
	}
	return res
}

func (e *engineImplementation) getRedisStreamsForGroup(group string) []string {
	streams := make([]string, 0)
	for _, row := range e.redisStreamGroups {
		for stream, groups := range row {
			_, has := groups[group]
			if has {
				streams = append(streams, stream)
			}
		}
	}
	return streams
}

func (e *engineImplementation) GetMySQL(code string) *DB {
	return e.mySQLServers[code]
}

func (e *engineImplementation) GetMySQLPools() map[string]*DB {
	return e.mySQLServers
}

func (e *engineImplementation) GetLocalCache(code string) LocalCache {
	return e.localCacheServers[code]
}

func (e *engineImplementation) GetLocalCachePools() map[string]LocalCache {
	return e.localCacheServers
}

func (e *engineImplementation) GetRedis(code string) RedisCache {
	return e.redisServers[code]
}

func (e *engineImplementation) GetRedisPools() map[string]RedisCache {
	return e.redisServers
}

func (e *engineImplementation) GetEntitySchema(entity any) EntitySchema {
	switch entity.(type) {
	case string:
		t, has := e.entities[entity.(string)]
		if !has {
			return nil
		}
		return e.entitySchemas[t]
	case Entity:
		return e.entitySchemas[reflect.TypeOf(entity).Elem()]
	case reflect.Type:
		return e.entitySchemas[entity.(reflect.Type)]
	}
	return nil
}

func (e *engineImplementation) GetEnum(code string) Enum {
	return e.enums[code]
}

func (e *engineImplementation) getDefaultQueryLogger() LogHandler {
	return e.defaultQueryLogger
}
