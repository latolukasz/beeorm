package beeorm

import (
	"reflect"
	"strconv"
)

func GetByID[E Entity, I ID](c Context, id I) (entity E) {
	entity = getByID[E, I](c.(*contextImplementation), id, nil)
	return
}

func getByID[E Entity, I ID](c *contextImplementation, id I, entityToFill Entity) (entity E) {
	schema := c.engine.registry.entitySchemas[reflect.TypeOf(entity)]
	idUint64 := uint64(id)
	if schema.hasLocalCache {
		e, has := schema.localCache.getEntity(c, idUint64)
		if has {
			if e == emptyReflect {
				return
			}
			entity = e.Interface().(E)
			return
		}
	}
	cacheRedis, hasRedis := schema.GetRedisCache()
	var cacheKey string
	if hasRedis {
		cacheKey = strconv.FormatUint(idUint64, 10)
		row, has := cacheRedis.HGet(c, schema.GetCacheKey(), cacheKey)
		if has {
			if row == cacheNilValue {
				if schema.hasLocalCache {
					schema.localCache.setEntity(c, idUint64, emptyReflect)
				}
				return
			}
			if entityToFill == nil {
				entity = schema.NewEntity().(E)
			} else {
				entity = entityToFill.(E)
			}
			fillFromBinary(c, schema, []byte(row), entity)
			//if len(references) > 0 {
			//	warmUpReferences(c, schema, orm.value, references, false)
			//}
			if schema.hasLocalCache {
				schema.localCache.setEntity(c, idUint64, entity.getORM().value)
			}
			return
		}
	}
	entity, found := searchRow[E](c, NewWhere("`ID` = ?", idUint64), nil, false)
	if !found {
		if schema.hasLocalCache {
			schema.localCache.setEntity(c, idUint64, emptyReflect)
		}
		if hasRedis {
			cacheRedis.HSet(c, schema.GetCacheKey(), cacheKey, cacheNilValue)
		}
		return
	}
	if schema.hasLocalCache {
		schema.localCache.setEntity(c, idUint64, entity.getORM().value)
	}
	if hasRedis {
		cacheRedis.HSet(c, schema.GetCacheKey(), cacheKey, string(entity.getORM().binary))
	}
	return
}
