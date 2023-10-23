package beeorm

import (
	"fmt"
	"reflect"
)

const cacheAllFakeReferenceKey = "all"

var allEntitiesWhere = NewWhere("1")

func GetAll[E any](c Context) EntityIterator[E] {
	var e E
	schema := c.(*contextImplementation).engine.registry.entitySchemas[reflect.TypeOf(e)]
	if schema == nil {
		panic(fmt.Errorf("entity '%T' is not registered", e))
	}
	if !schema.cacheAll {
		return Search[E](c, allEntitiesWhere, nil)
	}
	lc, hasLocalCache := schema.GetLocalCache()
	return getCachedList[E](c, cacheAllFakeReferenceKey, 0, hasLocalCache, lc, schema, schema)
}
