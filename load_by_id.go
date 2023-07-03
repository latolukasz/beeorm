package beeorm

import (
	"fmt"
	"reflect"
)

const cacheNilValue = ""

func loadByID(serializer *serializer, engine *engineImplementation, id uint64, entity Entity, entitySchema *tableSchema, useCache bool, references ...string) (found bool, foundEntity Entity, schema *tableSchema) {
	if entity != nil {
		orm := initIfNeeded(engine.registry, entity)
		schema = orm.tableSchema
	} else {
		schema = entitySchema
	}
	localCache, hasLocalCache := schema.GetLocalCache(engine)
	redisCache, hasRedis := schema.GetRedisCache(engine)
	var cacheKey string
	if useCache {
		if !hasLocalCache && engine.hasRequestCache {
			hasLocalCache = true
			localCache = engine.GetLocalCache(requestCacheKey)
		}

		if hasLocalCache {
			cacheKey = engine.getCacheKey(schema, id)
			e, has := localCache.Get(cacheKey)
			if has {
				if e == cacheNilValue {
					return false, nil, schema
				}
				if entity != nil {
					fillFromBinary(serializer, engine.registry, e.(reflect.Value).Interface().(Entity).getORM().binary, entity)
					if len(references) > 0 {
						warmUpReferences(serializer, engine, schema, entity.getORM().value, references, false)
					}
					return true, entity, schema
				}
				entity = e.(reflect.Value).Interface().(Entity)
				return true, entity, schema
			}
		}
		if hasRedis {
			cacheKey = engine.getCacheKey(schema, id)
			row, has := redisCache.Get(cacheKey)
			if has {
				if row == cacheNilValue {
					if localCache != nil {
						localCache.Set(cacheKey, cacheNilValue)
					}
					return false, nil, schema
				}
				fillFromBinary(serializer, engine.registry, []byte(row), entity)
				if len(references) > 0 {
					warmUpReferences(serializer, engine, schema, entity.getORM().value, references, false)
				}
				if localCache != nil {
					localCache.Set(cacheKey, entity.getORM().value)
				}
				return true, entity, schema
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
			redisCache.Set(cacheKey, cacheNilValue, 60)
		}
		return false, nil, schema
	}
	if useCache {
		if localCache != nil {
			localCache.Set(cacheKey, entity.getORM().value)
		}
		if redisCache != nil {
			redisCache.Set(cacheKey, entity.getORM().binary, 0)
		}
	}

	if len(references) > 0 {
		warmUpReferences(serializer, engine, schema, entity.getORM().elem, references, false)
	} else {
		data[0] = id
	}
	return true, entity, schema
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
