package beeorm

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/pkg/errors"
)

func (e *Engine) RedisSearchAggregate(entity Entity, query *RedisSearchAggregate, pager *Pager) (result []map[string]string, totalRows uint64) {
	schema := e.GetRegistry().GetTableSchemaForEntity(entity).(*tableSchema)
	return e.GetRedisSearch(schema.searchCacheName).Aggregate(schema.redisSearchIndex.Name, query, pager)
}

func (e *Engine) RedisSearchIds(entity Entity, query *RedisSearchQuery, pager *Pager) (ids []uint64, totalRows uint64) {
	schema := e.GetRegistry().GetTableSchemaForEntity(entity).(*tableSchema)
	return redisSearch(e, schema, query, pager)
}

func (e *Engine) RedisSearch(entities interface{}, query *RedisSearchQuery, pager *Pager, references ...string) (totalRows uint64) {
	return e.redisSearchBase(newSerializer(nil), entities, query, pager, references...)
}

func (e *Engine) RedisSearchCount(entity Entity, query *RedisSearchQuery) (totalRows uint64) {
	schema := e.GetRegistry().GetTableSchemaForEntity(entity).(*tableSchema)
	_, totalRows = redisSearch(e, schema, query, NewPager(0, 0))
	return totalRows
}

func (e *Engine) redisSearchBase(serializer *serializer, entities interface{}, query *RedisSearchQuery, pager *Pager, references ...string) (totalRows uint64) {
	elem := reflect.ValueOf(entities).Elem()
	_, has, name := getEntityTypeForSlice(e.registry, elem.Type(), true)
	if !has {
		panic(fmt.Errorf("entity '%s' is not registered", name))
	}
	schema := e.GetRegistry().GetTableSchema(name).(*tableSchema)
	ids, total := redisSearch(e, schema, query, pager)
	if total > 0 {
		tryByIDs(serializer, e, ids, reflect.ValueOf(entities).Elem(), references)
	}
	return total
}

func (e *Engine) RedisSearchOne(entity Entity, query *RedisSearchQuery, references ...string) (found bool) {
	return e.redisSearchOne(entity, query, references...)
}

func (e *Engine) redisSearchOne(entity Entity, query *RedisSearchQuery, references ...string) (found bool) {
	schema := e.GetRegistry().GetTableSchemaForEntity(entity).(*tableSchema)
	ids, total := redisSearch(e, schema, query, NewPager(1, 1))
	if total == 0 {
		return false
	}
	found, _ = loadByID(newSerializer(nil), e, ids[0], entity, true, references...)
	return found
}

func redisSearch(e *Engine, schema *tableSchema, query *RedisSearchQuery, pager *Pager) ([]uint64, uint64) {
	if schema.redisSearchIndex == nil {
		panic(errors.Errorf("entity %s is not searchable", schema.t.String()))
	}
	for k := range query.filtersString {
		_, has := schema.columnMapping[k]
		if !has {
			panic(fmt.Errorf("unknown field %s", k))
		}
		valid := false
	MAIN:
		for _, field := range schema.redisSearchIndex.Fields {
			if field.Name == k {
				if field.Type == "TEXT" {
					valid = true
					break MAIN
				}
				panic(fmt.Errorf("string filter on fields %s with type %s not allowed", k, field.Type))
			}
		}
		if !valid {
			panic(fmt.Errorf("missing `searchable` tag for field %s", k))
		}
	}
	for k := range query.filtersNumeric {
		_, has := schema.columnMapping[k]
		if !has {
			panic(fmt.Errorf("unknown field %s", k))
		}
		valid := false
	MAIN2:
		for _, field := range schema.redisSearchIndex.Fields {
			if field.Name == k {
				if field.Type == "NUMERIC" {
					valid = true
					break MAIN2
				}
				panic(fmt.Errorf("numeric filter on fields %s with type %s not allowed", k, field.Type))
			}
		}
		if !valid {
			panic(fmt.Errorf("missing `searchable` tag for field %s", k))
		}
	}
	for k := range query.filtersTags {
		_, has := schema.columnMapping[k]
		if !has {
			panic(fmt.Errorf("unknown field %s", k))
		}
		valid := false
	MAIN3:
		for _, field := range schema.redisSearchIndex.Fields {
			if field.Name == k {
				if field.Type == "TAG" {
					valid = true
					break MAIN3
				}
				panic(fmt.Errorf("tag filter on fields %s with type %s not allowed", k, field.Type))
			}
		}
		if !valid {
			panic(fmt.Errorf("missing `searchable` tag for field %s", k))
		}
	}
	search := e.GetRedisSearch(schema.searchCacheName)
	totalRows, res := search.search(schema.redisSearchIndex.Name, query, pager, true)
	ids := make([]uint64, len(res))
	for i, v := range res {
		ids[i], _ = strconv.ParseUint(v.(string)[6:], 10, 64)
	}
	return ids, totalRows
}
