package beeorm

import (
	"context"
	"reflect"
)

const DefaultPoolCode = "default"

type RedisStream struct {
	Name      string
	Pool      string
	Consumers []string
}

type EngineRegistry interface {
	EntitySchema(entity any) EntitySchema
	Enum(code string) Enum
	RedisStreams() []RedisStream
	DBPools() map[string]DB
	LocalCachePools() map[string]LocalCache
	RedisPools() map[string]RedisCache
	Entities() map[string]reflect.Type
	Plugins() []string
	Plugin(code string) Plugin
	DefaultDBCollate() string
	DefaultDBEncoding() string
	getDefaultQueryLogger() LogHandler
	getRedisStreamsForGroup(group string) []string
	getRedisPoolForStream(stream string) string
	getDBTables() map[string]map[string]bool
}

type Engine interface {
	NewContext(parent context.Context) Context
	DB(code string) DB
	LocalCache(code string) LocalCache
	Redis(code string) RedisCache
	Registry() EngineRegistry
}

type engineRegistryImplementation struct {
	engine             *engineImplementation
	entities           map[string]reflect.Type
	entitySchemas      map[reflect.Type]*entitySchema
	plugins            []Plugin
	enums              map[string]Enum
	defaultQueryLogger *defaultLogLogger
	redisStreamGroups  map[string]map[string]map[string]bool
	redisStreamPools   map[string]string
	defaultDBEncoding  string
	defaultDBCollate   string
	dbTables           map[string]map[string]bool
}

type engineImplementation struct {
	registry          *engineRegistryImplementation
	localCacheServers map[string]LocalCache
	dbServers         map[string]DB
	redisServers      map[string]RedisCache
}

func (e *engineImplementation) NewContext(parent context.Context) Context {
	return &contextImplementation{parent: parent, engine: e}
}

func (e *engineImplementation) Registry() EngineRegistry {
	return e.registry
}

func (e *engineImplementation) DB(code string) DB {
	return e.dbServers[code]
}

func (e *engineImplementation) LocalCache(code string) LocalCache {
	return e.localCacheServers[code]
}

func (e *engineImplementation) Redis(code string) RedisCache {
	return e.redisServers[code]
}

func (er *engineRegistryImplementation) RedisPools() map[string]RedisCache {
	return er.engine.redisServers
}

func (er *engineRegistryImplementation) LocalCachePools() map[string]LocalCache {
	return er.engine.localCacheServers
}

func (er *engineRegistryImplementation) DBPools() map[string]DB {
	return er.engine.dbServers
}

func (er *engineRegistryImplementation) EntitySchema(entity any) EntitySchema {
	switch entity.(type) {
	case Entity:
		return er.entitySchemas[reflect.TypeOf(entity).Elem()]
	case reflect.Type:
		return er.entitySchemas[entity.(reflect.Type)]
	case string:
		t, has := er.entities[entity.(string)]
		if !has {
			return nil
		}
		return er.entitySchemas[t]
	}
	return nil
}

func (er *engineRegistryImplementation) Plugins() []string {
	codes := make([]string, len(er.plugins))
	for i, plugin := range er.plugins {
		codes[i] = plugin.GetCode()
	}
	return codes
}

func (er *engineRegistryImplementation) Plugin(code string) Plugin {
	for _, plugin := range er.plugins {
		if plugin.GetCode() == code {
			return plugin
		}
	}
	return nil
}

func (er *engineRegistryImplementation) RedisStreams() []RedisStream {
	res := make([]RedisStream, 0)
	for redisPool, row := range er.redisStreamGroups {
		for stream, groups := range row {
			redisStream := RedisStream{Name: stream, Pool: redisPool, Consumers: []string{}}
			for group := range groups {
				redisStream.Consumers = append(redisStream.Consumers, group)
			}
			res = append(res, redisStream)
		}
	}
	return res
}

func (er *engineRegistryImplementation) getRedisStreamsForGroup(group string) []string {
	streams := make([]string, 0)
	for _, row := range er.redisStreamGroups {
		for stream, groups := range row {
			_, has := groups[group]
			if has {
				streams = append(streams, stream)
			}
		}
	}
	return streams
}

func (er *engineRegistryImplementation) getDBTables() map[string]map[string]bool {
	return er.dbTables
}

func (er *engineRegistryImplementation) getRedisPoolForStream(stream string) string {
	return er.redisStreamPools[stream]
}

func (er *engineRegistryImplementation) Entities() map[string]reflect.Type {
	return er.entities
}

func (er *engineRegistryImplementation) DefaultDBCollate() string {
	return er.defaultDBCollate
}

func (er *engineRegistryImplementation) DefaultDBEncoding() string {
	return er.defaultDBEncoding
}

func (er *engineRegistryImplementation) Enum(code string) Enum {
	return er.enums[code]
}

func (er *engineRegistryImplementation) getDefaultQueryLogger() LogHandler {
	return er.defaultQueryLogger
}
