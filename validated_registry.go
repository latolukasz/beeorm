package beeorm

import (
	"fmt"
	"reflect"
)

type ValidatedRegistry interface {
	CreateEngine() Engine
	GetEntitySchema(entityName string) EntitySchema
	GetEntitySchemaForEntity(entity Entity) EntitySchema
	GetEntitySchemaForCachePrefix(cachePrefix string) EntitySchema
	GetSourceRegistry() *Registry
	GetEnum(code string) Enum
	GetRedisStreams() map[string]map[string][]string
	GetMySQLPools() map[string]MySQLPoolConfig
	GetLocalCachePools() map[string]LocalCachePoolConfig
	GetRedisPools() map[string]RedisPoolConfig
	GetEntities() map[string]reflect.Type
	GetPlugins() []string
}

type validatedRegistry struct {
	registry           *Registry
	entitySchemas      map[reflect.Type]*entitySchema
	entities           map[string]reflect.Type
	localCacheServers  map[string]LocalCachePoolConfig
	mySQLServers       map[string]MySQLPoolConfig
	redisServers       map[string]RedisPoolConfig
	redisStreamGroups  map[string]map[string]map[string]bool
	redisStreamPools   map[string]string
	enums              map[string]Enum
	plugins            []Plugin
	timeOffset         int64
	defaultQueryLogger *defaultLogLogger
}

func (r *validatedRegistry) GetSourceRegistry() *Registry {
	return r.registry
}

func (r *validatedRegistry) GetEntities() map[string]reflect.Type {
	return r.entities
}

func (r *validatedRegistry) GetPlugins() []string {
	codes := make([]string, len(r.plugins))
	for i, plugin := range r.plugins {
		codes[i] = plugin.GetCode()
	}
	return codes
}

func (r *validatedRegistry) GetRedisStreams() map[string]map[string][]string {
	res := make(map[string]map[string][]string)
	for redisPool, row := range r.redisStreamGroups {
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

func (r *validatedRegistry) getRedisStreamsForGroup(group string) []string {
	streams := make([]string, 0)
	for _, row := range r.redisStreamGroups {
		for stream, groups := range row {
			_, has := groups[group]
			if has {
				streams = append(streams, stream)
			}
		}
	}
	return streams
}

func (r *validatedRegistry) GetMySQLPools() map[string]MySQLPoolConfig {
	return r.mySQLServers
}

func (r *validatedRegistry) GetLocalCachePools() map[string]LocalCachePoolConfig {
	return r.localCacheServers
}

func (r *validatedRegistry) GetRedisPools() map[string]RedisPoolConfig {
	return r.redisServers
}

func (r *validatedRegistry) CreateEngine() Engine {
	return &engineImplementation{registry: r}
}

func (r *validatedRegistry) GetEntitySchema(entityName string) EntitySchema {
	t, has := r.entities[entityName]
	if !has {
		return nil
	}
	return getEntitySchema(r, t)
}

func (r *validatedRegistry) GetEntitySchemaForEntity(entity Entity) EntitySchema {
	t := reflect.TypeOf(entity)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	entitySchema := getEntitySchema(r, t)
	if entitySchema == nil {
		panic(fmt.Errorf("entity '%s' is not registered", t.String()))
	}
	return entitySchema
}

func (r *validatedRegistry) GetEntitySchemaForCachePrefix(cachePrefix string) EntitySchema {
	for _, schema := range r.entitySchemas {
		if schema.cachePrefix == cachePrefix {
			return schema
		}
	}
	return nil
}

func (r *validatedRegistry) GetEnum(code string) Enum {
	return r.enums[code]
}
