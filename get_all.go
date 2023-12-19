package beeorm

import (
	"fmt"
	"reflect"
)

const cacheAllFakeReferenceKey = "all"

var allEntitiesWhere = NewWhere("1")

func GetAll[E any](orm ORM) EntityIterator[E] {
	var e E
	schema := orm.(*ormImplementation).engine.registry.entitySchemas[reflect.TypeOf(e)]
	if schema == nil {
		panic(fmt.Errorf("entity '%T' is not registered", e))
	}
	if !schema.cacheAll {
		return Search[E](orm, allEntitiesWhere, nil)
	}
	lc, hasLocalCache := schema.GetLocalCache()
	return getCachedList[E](orm, cacheAllFakeReferenceKey, 0, hasLocalCache, lc, schema, schema)
}
