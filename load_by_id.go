package beeorm

import (
	"fmt"
	"reflect"
)

const cacheNilValue = ""

func loadByID(serializer *serializer, engine *Engine, id uint64, entity Entity, useCache bool, lazy bool, references ...string) (found bool, schema *tableSchema) {
	orm := initIfNeeded(engine.registry, entity)
	schema = orm.tableSchema
	localCache, hasLocalCache := schema.GetLocalCache(engine)
	redisCache, hasRedis := schema.GetRedisCache(engine)
	var cacheKey string
	if useCache {
		if !hasLocalCache && engine.hasRequestCache {
			hasLocalCache = true
			localCache = engine.GetLocalCache(requestCacheKey)
		}

		if hasLocalCache {
			cacheKey = schema.getCacheKey(id)
			e, has := localCache.Get(cacheKey)
			if has {
				if e == cacheNilValue {
					return false, schema
				}
				data := e.([]byte)
				fillFromBinary(serializer, id, engine.registry, data, entity, lazy)
				if len(references) > 0 {
					warmUpReferences(serializer, engine, schema, orm.value, references, false, lazy)
				}
				return true, schema
			}
		}
		if hasRedis {
			cacheKey = schema.getCacheKey(id)
			row, has := redisCache.Get(cacheKey)
			if has {
				if row == cacheNilValue {
					return false, schema
				}
				fillFromBinary(serializer, id, engine.registry, []byte(row), entity, lazy)
				if len(references) > 0 {
					warmUpReferences(serializer, engine, schema, orm.value, references, false, lazy)
				}
				return true, schema
			}
		}
	}

	found, _, data := searchRow(serializer, false, engine, NewWhere("`ID` = ?", id), entity, lazy, nil)
	if !found {
		if localCache != nil {
			localCache.Set(cacheKey, cacheNilValue)
		}
		if redisCache != nil {
			redisCache.Set(cacheKey, cacheNilValue, 60)
		}
		return false, schema
	}
	if useCache {
		if localCache != nil {
			localCache.Set(cacheKey, orm.copyBinary())
		}
		if redisCache != nil {
			redisCache.Set(cacheKey, orm.binary, 0)
		}
	}

	if len(references) > 0 {
		warmUpReferences(serializer, engine, schema, orm.elem, references, false, lazy)
	} else {
		data[0] = id
	}
	return true, schema
}

func initIfNeeded(registry *validatedRegistry, entity Entity) *ORM {
	orm := entity.getORM()
	if !orm.initialised {
		orm.initialised = true
		value := reflect.ValueOf(entity)
		elem := value.Elem()
		t := elem.Type()
		tableSchema := getTableSchema(registry, t)
		if tableSchema == nil {
			panic(fmt.Errorf("entity '%s' is not registered", t.String()))
		}
		orm.tableSchema = tableSchema
		orm.value = value
		orm.elem = elem
		orm.idElem = elem.Field(1)
	}
	return orm
}
