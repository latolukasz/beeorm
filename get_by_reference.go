package beeorm

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
)

const redisValidSetValue = "Y"

func GetByReference[E any](c Context, referenceName string, id uint64) EntityIterator[E] {
	if id == 0 {
		return nil
	}
	var e E
	schema := c.(*contextImplementation).engine.registry.entitySchemas[reflect.TypeOf(e)]
	if schema == nil {
		panic(fmt.Errorf("entity '%T' is not registered", e))
	}
	def, has := schema.references[referenceName]
	if !has {
		panic(fmt.Errorf("unknow reference name `%s`", referenceName))
	}
	lc, hasLocalCache := schema.GetLocalCache()
	if !def.Cached {
		return Search[E](c, NewWhere("`"+referenceName+"` = ?", id), nil)
	}
	if hasLocalCache {
		fromCache, hasInCache := lc.getReference(c, referenceName, id)
		if hasInCache {
			if fromCache == cacheNilValue {
				return &emptyResultsIterator[E]{}
			}
			defSchema := c.Engine().Registry().EntitySchema(def.Type).(*entitySchema)
			if defSchema.hasLocalCache {
				results := &entityIterator[E]{index: -1}
				results.rows = fromCache.([]*E)
				return results
			}
			return GetByIDs[E](c, fromCache.([]uint64)...)
		}
	}
	rc, hasRedisCache := schema.GetRedisCache()
	if !hasRedisCache {
		rc = c.Engine().Redis(DefaultPoolCode)
	}
	idAsString := strconv.FormatUint(id, 10)
	redisSetKey := schema.cacheKey + ":" + referenceName + ":" + idAsString
	fromRedis := rc.SMembers(c, redisSetKey)
	if len(fromRedis) > 0 {
		ids := make([]uint64, len(fromRedis))
		k := 0
		hasValidValue := false
		for _, value := range fromRedis {
			if value == redisValidSetValue {
				hasValidValue = true
				continue
			} else if value == cacheNilValue {
				continue
			}
			ids[k], _ = strconv.ParseUint(value, 10, 64)
			k++
		}
		if hasValidValue {
			if k == 0 {
				if hasLocalCache {
					lc.setReference(c, referenceName, id, cacheNilValue)
				}
				return &emptyResultsIterator[E]{}
			}
			ids = ids[0:k]
			slices.Sort(ids)
			values := GetByIDs[E](c, ids...)
			if hasLocalCache {
				if values.Len() == 0 {
					lc.setReference(c, referenceName, id, cacheNilValue)
				} else {
					defSchema := c.Engine().Registry().EntitySchema(def.Type).(*entitySchema)
					if defSchema.hasLocalCache {
						lc.setReference(c, referenceName, id, values.all())
					} else {
						lc.setReference(c, referenceName, id, ids)
					}
				}
			}
			return values
		}
	}
	if hasLocalCache {
		ids := SearchIDs[E](c, NewWhere("`"+referenceName+"` = ?", id), nil)
		if len(ids) == 0 {
			lc.setReference(c, referenceName, id, cacheNilValue)
			rc.SAdd(c, redisSetKey, cacheNilValue)
			return &emptyResultsIterator[E]{}
		}
		idsForRedis := make([]interface{}, len(ids))
		for i, value := range ids {
			idsForRedis[i] = strconv.FormatUint(value, 10)
		}
		p := c.RedisPipeLine(rc.GetCode())
		p.Del(redisSetKey)
		p.SAdd(redisSetKey, redisValidSetValue)
		p.SAdd(redisSetKey, idsForRedis...)
		p.Exec(c)
		values := GetByIDs[E](c, ids...)
		defSchema := c.Engine().Registry().EntitySchema(def.Type).(*entitySchema)
		if defSchema.hasLocalCache {
			lc.setReference(c, referenceName, id, values.all())
		} else {
			lc.setReference(c, referenceName, id, ids)
		}
		return values
	}
	values := Search[E](c, NewWhere("`"+referenceName+"` = ?", id), nil)
	if values.Len() == 0 {
		rc.SAdd(c, redisSetKey, redisValidSetValue, cacheNilValue)
	} else {
		idsForRedis := make([]interface{}, values.Len()+1)
		idsForRedis[0] = redisValidSetValue
		i := 0
		for values.Next() {
			idsForRedis[i+1] = strconv.FormatUint(reflect.ValueOf(values.Entity()).Elem().Field(0).Uint(), 10)
			i++
		}
		values.reset()
		rc.SAdd(c, redisSetKey, idsForRedis...)
	}
	return values
}
