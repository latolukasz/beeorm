package beeorm

import (
	"fmt"
	"reflect"
	"strconv"
)

func GetByUniqueIndex[E any](orm ORM, indexName string, attributes ...any) (entity *E, found bool) {
	var e E
	schema := orm.(*ormImplementation).engine.registry.entitySchemas[reflect.TypeOf(e)]
	if schema == nil {
		panic(fmt.Errorf("entity '%T' is not registered", e))
	}
	definition, has := schema.uniqueIndexes[indexName]
	if !has {
		panic(fmt.Errorf("unknown index name `%s`", indexName))
	}
	if len(definition.Columns) != len(attributes) {
		panic(fmt.Errorf("invalid number of index `%s` attributes, got %d, %d expected",
			indexName, len(attributes), len(definition.Columns)))
	}
	var redisForCache RedisCache
	var hSetKey, hField string
	if definition.Cached {
		hSetKey = schema.getCacheKey() + ":" + indexName
		s := ""
		for i, attr := range attributes {
			if attr == nil {
				panic(fmt.Errorf("nil attribute for index name `%s` is not allowed", indexName))
			}
			val, err := schema.columnAttrToStringSetters[definition.Columns[i]](attr, false)
			checkError(err)
			s += val
		}
		hField = hashString(s)
		cache, hasRedis := schema.GetRedisCache()
		if !hasRedis {
			cache = orm.Engine().Redis(DefaultPoolCode)
		}
		redisForCache = cache
		previousID, inUse := cache.HGet(orm, hSetKey, hField)
		if inUse {
			id, _ := strconv.ParseUint(previousID, 10, 64)
			entity, found = GetByID[E](orm, id)
			if !found {
				cache.HDel(orm, hSetKey, hField)
			}
			return entity, found
		}
	}

	for i, attribute := range attributes {
		setter := schema.fieldBindSetters[definition.Columns[i]]
		bind, err := setter(attribute)
		if err != nil {
			panic(err)
		}
		attributes[i] = bind
	}
	entity, found = SearchOne[E](orm, definition.CreteWhere(false, attributes))
	if !found {
		return nil, false
	}
	if definition.Cached {
		redisForCache.HSet(orm, hSetKey, hField, strconv.FormatUint(reflect.ValueOf(entity).Elem().FieldByName("ID").Uint(), 10))
	}
	return entity, true
}
