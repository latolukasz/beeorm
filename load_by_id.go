package beeorm

import (
	"fmt"
	"reflect"
	"time"
)

const cacheNilValue = ""

func loadByID(serializer *serializer, engine *engineImplementation, id uint64, entity Entity, useCache bool, references ...string) (found bool, schema *entitySchema) {
	orm := initIfNeeded(engine.registry, entity)
	schema = orm.entitySchema
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
				fillFromBinary(serializer, id, engine.registry, data, entity)
				if len(references) > 0 {
					warmUpReferences(serializer, engine, schema, orm.value, references, false)
				}
				return true, schema
			}
		}
		if hasRedis {
			cacheKey = schema.getCacheKey(id)
			row, has := redisCache.Get(cacheKey)
			if has {
				if row == cacheNilValue {
					if localCache != nil {
						localCache.Set(cacheKey, cacheNilValue)
					}
					return false, schema
				}
				fillFromBinary(serializer, id, engine.registry, []byte(row), entity)
				if len(references) > 0 {
					warmUpReferences(serializer, engine, schema, orm.value, references, false)
				}
				if localCache != nil {
					localCache.Set(cacheKey, orm.copyBinary())
				}
				return true, schema
			}
		}
	}
	where := NewWhere("`ID` = ?", id)
	where.ShowFakeDeleted()
	found, _, data := searchRow(serializer, engine, where, entity, nil)
	if !found {
		if localCache != nil {
			localCache.Set(cacheKey, cacheNilValue)
		}
		if redisCache != nil {
			redisCache.Set(cacheKey, cacheNilValue, 30*time.Second)
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
		warmUpReferences(serializer, engine, schema, orm.elem, references, false)
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
		entitySchema := getEntitySchema(registry, t)
		if entitySchema == nil {
			panic(fmt.Errorf("entity '%s' is not registered", t.String()))
		}
		orm.entitySchema = entitySchema
		orm.value = value
		orm.elem = elem
	}
	return orm
}
