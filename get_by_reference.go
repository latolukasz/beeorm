package beeorm

import (
	"fmt"
	"reflect"
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
	// TODO add redis
	if hasLocalCache {
		fromCache, hasInCache := lc.getReference(c, referenceName, id)
		if !hasInCache {
			ids := SearchIDs[E](c, NewWhere("`"+referenceName+"` = ?", id), nil)
			rows := GetByIDs[E](c, ids...)
			lc.setReference(c, referenceName, id, rows)
			return rows
		}
		return fromCache.([]*E)
	}
	return make([]*E, 0)
}
