package beeorm

import (
	"reflect"
	"strconv"
)

func GetByID[E Entity, I ID](c Context, id I, references ...string) (entity E) {
	entity = getByID[E, I](c, id, nil, references...)
	return
}

func getByID[E Entity, I ID](c Context, id I, entityToFill Entity, references ...string) (entity E) {
	schema := GetEntitySchema[E](c)
	idUint64 := uint64(id)
	cacheLocal, hasLocalCache := schema.GetLocalCache()
	cacheRedis, hasRedis := schema.GetRedisCache()
	var cacheKey string
	if hasLocalCache {
		e, has := cacheLocal.Get(c, idUint64)
		if has {
			if e == cacheNilValue {
				return
			}
			entity = e.(reflect.Value).Interface().(E)
			//if len(references) > 0 {
			//	warmUpReferences(c, schema, orm.value, references, false)
			//}
			return
		}
	}
	if hasRedis {
		cacheKey = strconv.FormatUint(idUint64, 10)
		row, has := cacheRedis.HGet(c, schema.GetCacheKey(), cacheKey)
		if has {
			if row == cacheNilValue {
				if hasLocalCache {
					cacheLocal.Set(c, idUint64, cacheNilValue)
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
			if hasLocalCache {
				cacheLocal.Set(c, idUint64, entity.getORM().value)
			}
			return
		}
	}
	entity, found := searchRow[E](c, NewWhere("`ID` = ?", idUint64), nil, false, nil)
	if found {
		if hasLocalCache {
			cacheLocal.Set(c, idUint64, cacheNilValue)
		}
		if hasRedis {
			cacheRedis.HSet(c, schema.GetCacheKey(), cacheKey, cacheNilValue)
		}
		return
	}
	if hasLocalCache {
		cacheLocal.Set(c, cacheKey, entity.getORM().value)
	}
	if hasRedis {
		cacheRedis.HSet(c, schema.GetCacheKey(), cacheKey, string(entity.getORM().binary))
	}
	return
}
