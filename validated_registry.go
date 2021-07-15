package beeorm

import (
	"context"
	"fmt"
	"reflect"
)

type ValidatedRegistry interface {
	CreateEngine(ctx context.Context) *Engine
	GetTableSchema(entityName string) TableSchema
	GetTableSchemaForEntity(entity Entity) TableSchema
	GetSourceRegistry() *Registry
	GetEnum(code string) Enum
	GetRedisStreams() map[string]map[string][]string
	GetMySQLPools() map[string]MySQLPoolConfig
	GetLocalCachePools() map[string]LocalCachePoolConfig
	GetRedisPools() map[string]RedisPoolConfig
	GetRedisSearchIndices() map[string][]*RedisSearchIndex
	GetEntities() map[string]reflect.Type
}

type validatedRegistry struct {
	registry           *Registry
	tableSchemas       map[reflect.Type]*tableSchema
	entities           map[string]reflect.Type
	redisSearchIndexes map[string]map[string]*RedisSearchIndex
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

func (r *validatedRegistry) GetRedisSearchIndices() map[string][]*RedisSearchIndex {
	indices := make(map[string][]*RedisSearchIndex)
	for pool, list := range r.redisSearchIndexes {
		indices[pool] = make([]*RedisSearchIndex, 0)
		for _, index := range list {
			indices[pool] = append(indices[pool], index)
		}
	}
	return indices
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

func (r *validatedRegistry) GetMySQLPools() map[string]MySQLPoolConfig {
	return r.mySQLServers
}

func (r *validatedRegistry) GetLocalCachePools() map[string]LocalCachePoolConfig {
	return r.localCacheServers
}

func (r *validatedRegistry) GetRedisPools() map[string]RedisPoolConfig {
	return r.redisServers
}

func (r *validatedRegistry) CreateEngine(ctx context.Context) *Engine {
	return &Engine{registry: r, context: ctx}
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

func (r *validatedRegistry) GetEnum(code string) Enum {
	return r.enums[code]
}
