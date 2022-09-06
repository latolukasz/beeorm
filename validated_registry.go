package beeorm

import (
	"fmt"
	"reflect"
)

type ValidatedRegistry interface {
	CreateEngine() *Engine
	GetTableSchema(entityName string) TableSchema
	GetTableSchemaForEntity(entity Entity) TableSchema
	GetTableSchemaForCachePrefix(cachePrefix string) TableSchema
	GetSourceRegistry() *Registry
	GetEnum(code string) Enum
	GetRedisStreams() map[string]map[string][]string
	GetMySQLPools() map[string]MySQLPoolConfig
	GetLocalCachePools() map[string]LocalCachePoolConfig
	GetRedisPools() map[string]RedisPoolConfig
	GetEntities() map[string]reflect.Type
}

type validatedRegistry struct {
	registry           *Registry
	tableSchemas       map[reflect.Type]*tableSchema
	entities           map[string]reflect.Type
	localCacheServers  map[string]LocalCachePoolConfig
	mySQLServers       map[string]MySQLPoolConfig
	redisServers       map[string]RedisPoolConfig
	redisStreamGroups  map[string]map[string]map[string]bool
	redisStreamPools   map[string]string
	enums              map[string]Enum
	timeOffset         int64
	defaultQueryLogger *defaultLogLogger
}

func (r *validatedRegistry) GetSourceRegistry() *Registry {
	return r.registry
}

func (r *validatedRegistry) GetEntities() map[string]reflect.Type {
	return r.entities
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

func (r *validatedRegistry) CreateEngine() *Engine {
	return &Engine{registry: r}
}

func (r *validatedRegistry) GetTableSchema(entityName string) TableSchema {
	t, has := r.entities[entityName]
	if !has {
		return nil
	}
	return getTableSchema(r, t)
}

func (r *validatedRegistry) GetTableSchemaForEntity(entity Entity) TableSchema {
	t := reflect.TypeOf(entity)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	tableSchema := getTableSchema(r, t)
	if tableSchema == nil {
		panic(fmt.Errorf("entity '%s' is not registered", t.String()))
	}
	return tableSchema
}

func (r *validatedRegistry) GetTableSchemaForCachePrefix(cachePrefix string) TableSchema {
	for _, schema := range r.tableSchemas {
		if schema.cachePrefix == cachePrefix {
			return schema
		}
	}
	return nil
}

func (r *validatedRegistry) GetEnum(code string) Enum {
	return r.enums[code]
}
