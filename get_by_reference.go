package beeorm

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
)

const redisValidSetValue = "Y"

func GetByReference[E any](c Context, referenceName string, id uint64) []*E {
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
		panic(fmt.Errorf("unknows reference name `%s`", referenceName))
	}
	lc, hasLocalCache := schema.GetLocalCache()
	if !def.Cached {
		if hasLocalCache {
			ids := SearchIDs[E](c, NewWhere("`"+referenceName+"` = ?", id), nil)
			return GetByIDs[E](c, ids...)
		}
		return Search[E](c, NewWhere("`"+referenceName+"` = ?", id), nil)
	}
	if hasLocalCache {
		fromCache, hasInCache := lc.getReference(c, referenceName, id)
		if hasInCache {
			if fromCache == cacheNilValue {
				return make([]*E, 0)
			}
			defSchema := c.Engine().Registry().EntitySchema(def.Type).(*entitySchema)
			if defSchema.hasLocalCache {
				return fromCache.([]*E)
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
				return make([]*E, 0)
			}
			ids = ids[0:k]
			slices.Sort(ids)
			values := GetByIDs[E](c, ids...)
			if hasLocalCache {
				if len(values) == 0 {
					lc.setReference(c, referenceName, id, cacheNilValue)
				} else {
					defSchema := c.Engine().Registry().EntitySchema(def.Type).(*entitySchema)
					if defSchema.hasLocalCache {
						lc.setReference(c, referenceName, id, values)
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
			return make([]*E, 0)
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
			lc.setReference(c, referenceName, id, values)
		} else {
			lc.setReference(c, referenceName, id, ids)
		}
		return values
	}
	values := Search[E](c, NewWhere("`"+referenceName+"` = ?", id), nil)
	if len(values) == 0 {
		rc.SAdd(c, redisSetKey, redisValidSetValue, cacheNilValue)
	} else {
		idsForRedis := make([]interface{}, len(values)+1)
		idsForRedis[0] = redisValidSetValue
		for i, value := range values {
			idsForRedis[i+1] = strconv.FormatUint(reflect.ValueOf(value).Elem().Field(0).Uint(), 10)
		}
		rc.SAdd(c, redisSetKey, idsForRedis...)
	}
	return values
}
