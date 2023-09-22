package beeorm

import (
	"reflect"
	"strconv"
)

func GetByID[E Entity](c Context, id uint64) (entity E) {
	entity = getByID[E](c.(*contextImplementation), id, nil)
	return
}

func getByID[E Entity](c *contextImplementation, id uint64, entityToFill Entity) (entity E) {
	schema := c.engine.registry.entitySchemas[reflect.TypeOf(entity)]
	if schema.hasLocalCache {
		e, has := schema.localCache.getEntity(c, id)
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
		cacheKey = strconv.FormatUint(id, 10)
		row, has := cacheRedis.HGet(c, schema.GetCacheKey(), cacheKey)
		if has {
			if row == cacheNilValue {
				if schema.hasLocalCache {
					schema.localCache.setEntity(c, id, emptyReflect)
				}
				return
			}
			var value reflect.Value
			if entityToFill == nil {
				value = reflect.New(schema.GetType().Elem())
				entity = value.Interface().(E)
			} else {
				entity = entityToFill.(E)
				value = reflect.ValueOf(entity)
			}
			s := c.getSerializer()
			s.buffer.WriteString(row)
			deserializeFromBinary(s, schema, value.Elem())
			if schema.hasLocalCache {
				schema.localCache.setEntity(c, id, value)
			}
			return
		}
	}
	entity, found := searchRow[E](c, NewWhere("`ID` = ?", id), nil, false)
	if !found {
		if schema.hasLocalCache {
			schema.localCache.setEntity(c, id, emptyReflect)
		}
		if hasRedis {
			cacheRedis.HSet(c, schema.GetCacheKey(), cacheKey, cacheNilValue)
		}
		return
	}
	if schema.hasLocalCache {
		schema.localCache.setEntity(c, id, reflect.ValueOf(entity))
	}
	if hasRedis {
		s := c.getSerializer()
		serializeEntity(schema, reflect.ValueOf(entity).Elem(), s)
		cacheRedis.HSet(c, schema.GetCacheKey(), cacheKey, string(s.Read()))
	}
	return
}
