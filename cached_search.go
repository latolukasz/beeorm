package beeorm

//
//import (
//	"fmt"
//	jsoniter "github.com/json-iterator/go"
//	"reflect"
//	"strconv"
//	"strings"
//)
//
//func CachedSearchOne[E Entity](c Context, indexName string, arguments ...interface{}) (entity E, found bool) {
//	return cachedSearchOne[E](c, indexName, arguments)
//}
//
//func CachedSearch[E Entity](c Context, indexName string, arguments ...interface{}) []E {
//	return cachedSearch[E](c.(*contextImplementation), indexName, arguments)
//}
//
//func cachedSearch[E Entity](c *contextImplementation, indexName string, arguments []interface{}) []E {
//	schema := GetEntitySchema[E](c).(*entitySchema)
//	definition, has := schema.cachedIndexes[indexName]
//	if !has {
//		panic(fmt.Errorf("index %s not found", indexName))
//	}
//	if !schema.hasLocalCache && !schema.hasRedisCache {
//		panic(fmt.Errorf("cache search not allowed for entity without cache: '%s'", schema.GetType().String()))
//	}
//	cacheKey := getCacheKeySearch(indexName, arguments)
//	if schema.hasLocalCache {
//		fromCacheLocal, hasInLocalCache := schema.localCache.Get(c, cacheKey)
//		if hasInLocalCache {
//			return fromCacheLocal.([]E)
//		}
//	}
//	if schema.hasRedisCache {
//		// TODO
//	}
//	ids, _ := searchIDs(c, schema.GetType(), NewWhere(definition.Query, arguments...), nil, false)
//	entities := GetByIDs[E](c, ids...)
//	if schema.hasLocalCache {
//		schema.localCache.Set(c, cacheKey, entities)
//	}
//	if schema.hasRedisCache {
//		// TODO
//	}
//	return entities
//}
//
//func cachedSearchOne[E Entity](c Context, indexName string, arguments []interface{}) (entity E, found bool) {
//	value := reflect.ValueOf(entity)
//	entityType := value.Elem().Type()
//	schema := GetEntitySchema[E](c).(*entitySchema)
//	if schema == nil {
//		panic(fmt.Errorf("entity '%s' is not registered", entityType.String()))
//	}
//	definition, has := schema.getCachedIndexes(true, false)[indexName]
//	if !has {
//		panic(fmt.Errorf("index %s not found", indexName))
//	}
//	where := NewWhere(definition.Query, arguments...)
//	cacheLocal, hasLocalCache := schema.GetLocalCache()
//	cacheRedis, hasRedis := schema.GetRedisCache()
//	if !hasLocalCache && !hasRedis {
//		panic(fmt.Errorf("cache search not allowed for entity without cache: '%s'", entityType.String()))
//	}
//	cacheKey := getCacheKeySearch(indexName, where.GetParameters())
//	var fromCache map[string]interface{}
//	if hasLocalCache {
//		fromLocalCache, hasInLocalCache := cacheLocal.Get(c, cacheKey)
//		if hasInLocalCache {
//			fromCache = map[string]interface{}{"1": fromLocalCache}
//		} else {
//			fromCache = map[string]interface{}{"1": nil}
//		}
//	}
//	if fromCache["1"] == nil && hasRedis {
//		fromCache = cacheRedis.HMGet(c, cacheKey, "1")
//	}
//	id := uint64(0)
//	if fromCache["1"] == nil {
//		results, _ := searchIDs(c, entityType, where, NewPager(1, 1), false)
//		l := len(results)
//		value := strconv.Itoa(l)
//		if l > 0 {
//			id = results[0]
//			value += " " + strconv.FormatUint(results[0], 10)
//		}
//		if hasLocalCache {
//			cacheLocal.Set(c, cacheKey, value)
//		}
//		if hasRedis {
//			cacheRedis.HSet(c, cacheKey, "1", value)
//		}
//	} else {
//		ids := strings.Split(fromCache["1"].(string), " ")
//		if ids[0] != "0" {
//			id, _ = strconv.ParseUint(ids[1], 10, 64)
//		}
//	}
//	if id > 0 {
//		entity = GetByID[E](c, id)
//		return entity, true
//	}
//	return entity, false
//}
//
//func getCacheKeySearch(indexName string, parameters []interface{}) string {
//	asString, err := jsoniter.ConfigFastest.MarshalToString(parameters)
//	checkError(err)
//	return indexName + asString
//}
