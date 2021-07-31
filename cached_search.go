package beeorm

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/segmentio/fasthash/fnv1a"
)

const idsOnCachePage = 100

func cachedSearch(serializer *serializer, engine *Engine, entities interface{}, indexName string, pager *Pager,
	arguments []interface{}, lazy, checkIsSlice bool, references []string) (totalRows int, ids []uint64) {
	value := reflect.ValueOf(entities)
	entityType, has, name := getEntityTypeForSlice(engine.registry, value.Type(), checkIsSlice)
	if !has {
		panic(fmt.Errorf("entity '%s' is not registered", name))
	}
	schema := getTableSchema(engine.registry, entityType)
	definition, has := schema.cachedIndexes[indexName]
	if !has {
		panic(fmt.Errorf("index %s not found", indexName))
	}
	if pager == nil {
		pager = NewPager(1, definition.Max)
	}
	start := (pager.GetCurrentPage() - 1) * pager.GetPageSize()
	if start+pager.GetPageSize() > definition.Max {
		panic(fmt.Errorf("max cache index page size (%d) exceeded %s", definition.Max, indexName))
	}
	localCache, hasLocalCache := schema.GetLocalCache(engine)
	if !hasLocalCache && engine.hasRequestCache {
		hasLocalCache = true
		localCache = engine.GetLocalCache(requestCacheKey)
	}
	redisCache, hasRedis := schema.GetRedisCache(engine)
	if !hasLocalCache && !hasRedis {
		panic(fmt.Errorf("cache search not allowed for entity without cache: '%s'", entityType.String()))
	}
	where := NewWhere(definition.Query, arguments...)
	cacheKey := getCacheKeySearch(schema, indexName, where.GetParameters()...)

	minCachePage := float64((pager.GetCurrentPage() - 1) * pager.GetPageSize() / idsOnCachePage)
	minCachePageCeil := minCachePage
	maxCachePage := float64((pager.GetCurrentPage()-1)*pager.GetPageSize()+pager.GetPageSize()) / float64(idsOnCachePage)
	maxCachePageCeil := math.Ceil(maxCachePage)
	pages := make([]string, int(maxCachePageCeil-minCachePageCeil))
	j := 0
	for i := minCachePageCeil; i < maxCachePageCeil; i++ {
		pages[j] = strconv.Itoa(int(i) + 1)
		j++
	}
	filledPages := make(map[string][]uint64)
	fromRedis := false
	var fromCache map[string]interface{}
	var nilsKeys []string
	if hasLocalCache {
		nilsKeys = make([]string, 0)
		fromCache = localCache.HMGet(cacheKey, pages...)
		for key, val := range fromCache {
			if val == nil {
				nilsKeys = append(nilsKeys, key)
			}
		}
		if hasRedis && len(nilsKeys) > 0 {
			fromRedis := redisCache.HMGet(cacheKey, nilsKeys...)
			for key, idsFromRedis := range fromRedis {
				fromCache[key] = idsFromRedis
			}
		}
	} else if hasRedis {
		fromRedis = true
		fromCache = redisCache.HMGet(cacheKey, pages...)
	}
	hasNil := false
	totalRows = 0
	minPage := 9999
	maxPage := 0
	for key, idsSlice := range fromCache {
		if idsSlice == nil {
			hasNil = true
			p, _ := strconv.Atoi(key)
			if p < minPage {
				minPage = p
			}
			if p > maxPage {
				maxPage = p
			}
		} else {
			if fromRedis {
				ids := strings.Split(idsSlice.(string), " ")
				totalRows, _ = strconv.Atoi(ids[0])
				length := len(ids)
				idsAsUint := make([]uint64, length-1)
				for i := 1; i < length; i++ {
					idsAsUint[i-1], _ = strconv.ParseUint(ids[i], 10, 64)
				}
				filledPages[key] = idsAsUint
			} else {
				ids := idsSlice.([]uint64)
				totalRows = int(ids[0])
				filledPages[key] = ids[1:]
			}
		}
	}

	if hasNil {
		searchPager := NewPager(minPage, maxPage*idsOnCachePage)
		results, total := searchIDsWithCount(false, engine, where, searchPager, entityType)
		totalRows = total
		cacheFields := make([]interface{}, 0)
		for key, ids := range fromCache {
			if ids == nil {
				page := key
				pageInt, _ := strconv.Atoi(page)
				sliceStart := (pageInt - minPage) * idsOnCachePage
				if sliceStart > total {
					cacheFields = append(cacheFields, page, total)
					continue
				}
				sliceEnd := sliceStart + idsOnCachePage
				if sliceEnd > total {
					sliceEnd = total
				}
				l := len(results)
				if l == 0 {
					cacheFields = append(cacheFields, page, total)
					continue
				}
				sliceEnd = int(math.Min(float64(sliceEnd), float64(l)))
				values := []uint64{uint64(total)}
				foundIDs := results[sliceStart:sliceEnd]
				filledPages[key] = foundIDs
				values = append(values, foundIDs...)
				cacheValue := fmt.Sprintf("%v", values)
				cacheValue = strings.Trim(cacheValue, "[]")
				cacheFields = append(cacheFields, page, cacheValue)
			}
		}
		if hasRedis {
			redisCache.HSet(cacheKey, cacheFields...)
		}
	}
	nilKeysLen := len(nilsKeys)
	if hasLocalCache && nilKeysLen > 0 {
		fields := make(map[string]interface{}, nilKeysLen)
		for _, v := range nilsKeys {
			values := []uint64{uint64(totalRows)}
			values = append(values, filledPages[v]...)
			fields[v] = values
		}
		localCache.HMSet(cacheKey, fields)
	}

	resultsIDs := make([]uint64, 0)
	for i := minCachePageCeil; i < maxCachePageCeil; i++ {
		resultsIDs = append(resultsIDs, filledPages[strconv.Itoa(int(i)+1)]...)
	}
	sliceStart := (pager.GetCurrentPage() - 1) * pager.GetPageSize()
	diff := int(minCachePageCeil) * idsOnCachePage
	sliceStart -= diff
	if sliceStart > totalRows {
		return totalRows, []uint64{}
	}
	sliceEnd := sliceStart + pager.GetPageSize()
	length := len(resultsIDs)
	if sliceEnd > length {
		sliceEnd = length
	}
	idsToReturn := resultsIDs[sliceStart:sliceEnd]
	_, is := entities.(Entity)
	if !is {
		tryByIDs(serializer, engine, idsToReturn, value.Elem(), references, lazy)
	}
	return totalRows, idsToReturn
}

func cachedSearchOne(serializer *serializer, engine *Engine, entity Entity, indexName string, fillStruct, lazy bool, arguments []interface{}, references []string) (has bool) {
	value := reflect.ValueOf(entity)
	entityType := value.Elem().Type()
	schema := getTableSchema(engine.registry, entityType)
	if schema == nil {
		panic(fmt.Errorf("entity '%s' is not registered", entityType.String()))
	}
	definition, has := schema.cachedIndexesOne[indexName]
	if !has {
		panic(fmt.Errorf("index %s not found", indexName))
	}
	Where := NewWhere(definition.Query, arguments...)
	localCache, hasLocalCache := schema.GetLocalCache(engine)
	if !hasLocalCache && engine.hasRequestCache {
		hasLocalCache = true
		localCache = engine.GetLocalCache(requestCacheKey)
	}
	redisCache, hasRedis := schema.GetRedisCache(engine)
	if !hasLocalCache && !hasRedis {
		panic(fmt.Errorf("cache search not allowed for entity without cache: '%s'", entityType.String()))
	}
	cacheKey := getCacheKeySearch(schema, indexName, Where.GetParameters()...)
	var fromCache map[string]interface{}
	if hasLocalCache {
		fromCache = localCache.HMGet(cacheKey, "1")
	}
	if fromCache["1"] == nil && hasRedis {
		fromCache = redisCache.HMGet(cacheKey, "1")
	}
	id := uint64(0)
	if fromCache["1"] == nil {
		results, _ := searchIDs(true, engine, Where, NewPager(1, 1), false, entityType)
		l := len(results)
		value := strconv.Itoa(l)
		if l > 0 {
			id = results[0]
			value += " " + strconv.FormatUint(results[0], 10)
		}
		if hasLocalCache {
			localCache.HMSet(cacheKey, map[string]interface{}{"1": value})
		}
		if hasRedis {
			redisCache.HSet(cacheKey, "1", value)
		}
	} else {
		ids := strings.Split(fromCache["1"].(string), " ")
		if ids[0] != "0" {
			id, _ = strconv.ParseUint(ids[1], 10, 64)
		}
	}
	if id > 0 {
		has = true
		if fillStruct {
			has, _ = loadByID(serializer, engine, id, entity, true, lazy, references...)
		}
		return has
	}
	return false
}

func getCacheKeySearch(tableSchema *tableSchema, indexName string, parameters ...interface{}) string {
	return tableSchema.cachePrefix + "_" + indexName + strconv.Itoa(int(fnv1a.HashString32(fmt.Sprintf("%v", parameters))))
}
