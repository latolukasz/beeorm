package beeorm

import (
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
	if hasLocalCache {
		e, has := cacheLocal.get(c, idUint64)
		if has {
			if e.IsZero() {
				return
			}
			entity = e.Interface().(E)
			//if len(references) > 0 {
			//	warmUpReferences(c, schema, orm.value, references, false)
			//}
			return
		}
	}
	var cacheKey string
	if hasRedis {
		cacheKey = strconv.FormatUint(idUint64, 10)
		row, has := cacheRedis.HGet(c, schema.GetCacheKey(), cacheKey)
		if has {
			if row == cacheNilValue {
				if hasLocalCache {
					cacheLocal.addNil(c, idUint64)
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
				cacheLocal.add(c, idUint64, entity.getORM().value)
			}
			return
		}
	}
	entity, found := searchRow[E](c, NewWhere("`ID` = ?", idUint64), nil, false, nil)
	if !found {
		if hasLocalCache {
			cacheLocal.addNil(c, idUint64)
		}
		if hasRedis {
			cacheRedis.HSet(c, schema.GetCacheKey(), cacheKey, cacheNilValue)
		}
		return
	}
	if hasLocalCache {
		cacheLocal.add(c, idUint64, entity.getORM().value)
	}
	if hasRedis {
		cacheRedis.HSet(c, schema.GetCacheKey(), cacheKey, string(entity.getORM().binary))
	}
	return
}
