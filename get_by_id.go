package beeorm

import (
	"reflect"
	"strconv"
)

func GetByID[E any](c Context, id uint64) (entity *E) {
	entity = getByID[E](c.(*contextImplementation), id, nil)
	return
}

func getByID[E any](c *contextImplementation, id uint64, entityToFill *E) (entity *E) {
	schema := c.engine.registry.entitySchemas[reflect.TypeOf(entity)]
	if schema.hasLocalCache {
		e, has := schema.localCache.getEntity(c, id)
		if has {
			if e == nil {
				return
			}
			entity = e.(*E)
			return
		}
	}
	cacheRedis, hasRedis := schema.GetRedisCache()
	var cacheKey string
	if hasRedis {
		cacheKey = schema.GetCacheKey() + ":" + strconv.FormatUint(id, 10)
		row := cacheRedis.LRange(c, cacheKey, 0, int64(len(schema.columnNames)+1))
		l := len(row)
		if len(row) > 0 {
			if l == 1 {
				if schema.hasLocalCache {
					schema.localCache.setEntity(c, id, nil)
				}
				return
			}
			var value reflect.Value
			if entityToFill == nil {
				value = reflect.New(schema.t)
				entity = value.Interface().(*E)
			} else {
				entity = entityToFill
				value = reflect.ValueOf(entity)
			}
			if deserializeFromRedis(row, schema, value.Elem()) {
				if schema.hasLocalCache {
					schema.localCache.setEntity(c, id, entity)
				}
				return
			}
		}
	}
	entity, found := searchRow[E](c, NewWhere("`ID` = ?", id), nil, false)
	if !found {
		if schema.hasLocalCache {
			schema.localCache.setEntity(c, id, nil)
		}
		if hasRedis {
			p := c.RedisPipeLine(cacheRedis.GetCode())
			p.Del(cacheKey)
			p.RPush(cacheKey, cacheNilValue)
			p.Exec(c)
		}
		return
	}
	if schema.hasLocalCache {
		schema.localCache.setEntity(c, id, entity)
	}
	if hasRedis {
		bind := make(Bind)
		err := fillBindFromOneSource(c, bind, reflect.ValueOf(entity).Elem(), schema.fields, "")
		checkError(err)
		values := convertBindToRedisValue(bind, schema)
		cacheRedis.RPush(c, cacheKey, values...)
	}
	return
}
