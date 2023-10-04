package beeorm

import (
	"fmt"
	"reflect"
	"strconv"
)

func GetByUniqueKey[E any](c Context, indexName string, attributes ...any) *E {
	var e E
	schema := c.(*contextImplementation).engine.registry.entitySchemas[reflect.TypeOf(e)]
	if schema == nil {
		panic(fmt.Errorf("entity '%T' is not registered", e))
	}
	columns, has := schema.uniqueIndices[indexName]
	if !has {
		panic(fmt.Errorf("unknows index name `%s`", indexName))
	}
	if len(columns) != len(attributes) {
		panic(fmt.Errorf("invalid number of index `%s` attributes, got %d, %d expected",
			indexName, len(attributes), len(columns)))
	}
	hSetKey := schema.GetCacheKey() + ":" + indexName
	sBuilder := c.getStringBuilder2()
	for i, attr := range attributes {
		if attr == nil {
			panic(fmt.Errorf("nil attribute for index name `%s` is not allowed", indexName))
		}
		val, err := schema.columnAttrToStringSetters[columns[i]](attr)
		checkError(err)
		sBuilder.WriteString(val)
	}
	hField := hashString(sBuilder.String())
	cache, hasRedis := schema.GetRedisCache()
	if !hasRedis {
		cache = c.Engine().Redis(DefaultPoolCode)
	}
	previousID, inUse := cache.HGet(c, hSetKey, hField)
	if inUse {
		id, _ := strconv.ParseUint(previousID, 10, 64)
		return GetByID[E](c, id)
	}
	return nil
}
