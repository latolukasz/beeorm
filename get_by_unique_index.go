package beeorm

import (
	"fmt"
	"reflect"
	"strconv"
)

func GetByUniqueIndex[E any](orm ORM, indexName string, attributes ...any) *E {
	var e E
	schema := orm.(*ormImplementation).engine.registry.entitySchemas[reflect.TypeOf(e)]
	if schema == nil {
		panic(fmt.Errorf("entity '%T' is not registered", e))
	}
	columns, has := schema.uniqueIndices[indexName]
	if !has {
		panic(fmt.Errorf("unknown index name `%s`", indexName))
	}
	if len(columns) != len(attributes) {
		panic(fmt.Errorf("invalid number of index `%s` attributes, got %d, %d expected",
			indexName, len(attributes), len(columns)))
	}
	hSetKey := schema.getCacheKey() + ":" + indexName
	s := ""
	for i, attr := range attributes {
		if attr == nil {
			panic(fmt.Errorf("nil attribute for index name `%s` is not allowed", indexName))
		}
		val, err := schema.columnAttrToStringSetters[columns[i]](attr, false)
		checkError(err)
		s += val
	}
	hField := hashString(s)
	cache, hasRedis := schema.GetRedisCache()
	if !hasRedis {
		cache = orm.Engine().Redis(DefaultPoolCode)
	}
	previousID, inUse := cache.HGet(orm, hSetKey, hField)
	if inUse {
		id, _ := strconv.ParseUint(previousID, 10, 64)
		return GetByID[E](orm, id)
	}
	return nil
}
