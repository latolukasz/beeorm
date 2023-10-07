package beeorm

import (
	"fmt"
	"reflect"
	"strconv"
)

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
		if fromRedis[0] == cacheNilValue {
			fromRedis = fromRedis[1:]
		}
		if len(fromRedis) == 0 {
			if hasLocalCache {
				lc.setReference(c, referenceName, id, cacheNilValue)
			}
			return make([]*E, 0)
		}
		ids := make([]uint64, len(fromRedis)-1)
		for i, value := range fromRedis {
			ids[i], _ = strconv.ParseUint(value, 10, 64)
		}
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
		rc.SAdd(c, redisSetKey, idsForRedis...)
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
		rc.SAdd(c, redisSetKey, cacheNilValue)
	} else {
		idsForRedis := make([]interface{}, len(values))
		for i, value := range values {
			idsForRedis[i] = strconv.FormatUint(reflect.ValueOf(value).Elem().Field(0).Uint(), 10)
		}
		rc.SAdd(c, redisSetKey, idsForRedis...)
	}
	return values
}

func fillReferenceInRedis(c Context, schema *entitySchema, referenceName, id, redisSetKey string, p *RedisPipeLine) {
	ids, _ := searchIDs(c, schema, NewWhere("`"+referenceName+"` = ?", id), nil, false)
	if len(ids) == 0 {
		p.SAdd(redisSetKey, cacheNilValue)
		return
	}
	members := make([]interface{}, len(ids))
	for i, value := range ids {
		members[i] = value
	}
	p.SAdd(redisSetKey, members...)
}
