package beeorm

import (
	"context"
	"reflect"
	"strings"
	"time"
)

const DefaultPoolCode = "default"

type EngineRegistry interface {
	EntitySchema(entity any) EntitySchema
	DBPools() map[string]DB
	LocalCachePools() map[string]LocalCache
	RedisPools() map[string]RedisCache
	Entities() map[string]reflect.Type
	Option(key string) any
	Enums() map[string][]string
	getDefaultQueryLogger() LogHandler
	getDBTables() map[string]map[string]bool
}

type EngineSetter interface {
	SetOption(key string, value any)
}

type Engine interface {
	NewContext(parent context.Context) Context
	DB(code string) DB
	LocalCache(code string) LocalCache
	Redis(code string) RedisCache
	Registry() EngineRegistry
	Option(key string) any
}

type engineRegistryImplementation struct {
	engine                 *engineImplementation
	entities               map[string]reflect.Type
	entitySchemas          map[reflect.Type]*entitySchema
	entitySchemasQuickMap  map[reflect.Type]*entitySchema
	entityLogSchemas       map[reflect.Type]*entitySchema
	defaultQueryLogger     *defaultLogLogger
	dbTables               map[string]map[string]bool
	options                map[string]any
	enums                  map[string][]string
	asyncConsumerBlockTime time.Duration
}

type engineImplementation struct {
	registry                     *engineRegistryImplementation
	localCacheServers            map[string]LocalCache
	dbServers                    map[string]DB
	redisServers                 map[string]RedisCache
	options                      map[string]any
	pluginFlush                  []PluginInterfaceEntityFlush
	asyncTemporaryIsQueueRunning bool
}

func (e *engineImplementation) NewContext(parent context.Context) Context {
	return &contextImplementation{parent: parent, engine: e}
}

func (e *engineImplementation) Registry() EngineRegistry {
	return e.registry
}

func (e *engineImplementation) Option(key string) any {
	return e.options[key]
}

func (e *engineImplementation) SetOption(key string, value any) {
	e.options[key] = value
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
	case reflect.Type:
		return er.entitySchemas[entity.(reflect.Type)]
	case string:
		name := entity.(string)
		if strings.HasPrefix(name, "*") {
			name = name[1:]
		}
		t, has := er.entities[name]
		if !has {
			return nil
		}
		return er.entitySchemas[t]
	default:
		t := reflect.TypeOf(entity)
		if t.Kind() == reflect.Ptr {
			return er.entitySchemas[t.Elem()]
		}
		return er.entitySchemas[t]
	}
}

func (er *engineRegistryImplementation) getDBTables() map[string]map[string]bool {
	return er.dbTables
}

func (er *engineRegistryImplementation) Entities() map[string]reflect.Type {
	return er.entities
}

func (er *engineRegistryImplementation) Option(key string) any {
	return er.options[key]
}

func (er *engineRegistryImplementation) Enums() map[string][]string {
	return er.enums
}

func (er *engineRegistryImplementation) getDefaultQueryLogger() LogHandler {
	return er.defaultQueryLogger
}
