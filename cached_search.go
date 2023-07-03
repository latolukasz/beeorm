package beeorm

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/segmentio/fasthash/fnv1a"
)

const idsOnCachePage = 1000

func cachedSearch(serializer *serializer, engine *engineImplementation, entities interface{}, indexName string, pager *Pager,
	arguments []interface{}, checkIsSlice bool) (totalRows int) {
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
	cacheKey := getCacheKeySearch(engine, schema, indexName, arguments...)
	where := NewWhere(definition.Query, arguments...)

	pageSize := idsOnCachePage
	minCachePage := float64((pager.GetCurrentPage() - 1) * pager.GetPageSize() / pageSize)
	minCachePageCeil := minCachePage
	maxCachePage := float64((pager.GetCurrentPage()-1)*pager.GetPageSize()+pager.GetPageSize()) / float64(pageSize)
	maxCachePageCeil := math.Ceil(maxCachePage)
	size := int(maxCachePageCeil - minCachePageCeil)
	pages := engine.getCacheStrings(size, size)
	j := 0
	for i := minCachePageCeil; i < maxCachePageCeil; i++ {
		pages[j] = strconv.Itoa(int(i) + 1)
		j++
	}
	filledPages := make(map[string][]string)
	var fromCache map[string][]string
	var nilsKeys []string
	if hasLocalCache {
		nilsKeys = make([]string, 0)
		fromCacheLocal, hasInLocalCache := localCache.Get(cacheKey)
		if hasInLocalCache {
			fromCache = map[string][]string{"1": fromCacheLocal.([]string)}
		} else {
			fromCache = map[string][]string{"1": nil}
			nilsKeys = append(nilsKeys, "1")
		}
		if hasRedis && len(nilsKeys) > 0 {
			dataFromRedis := redisCache.HMGet(cacheKey, nilsKeys...)
			if fromCache == nil {
				fromCache = map[string][]string{}
			}
			for key, idsFromRedis := range dataFromRedis {
				if idsFromRedis != nil {
					fromCache[key] = strings.Split(idsFromRedis.(string), " ")
				} else {
					fromCache[key] = nil
				}
			}
		}
	} else if hasRedis {
		dataFromRedis := redisCache.HMGet(cacheKey, pages...)
		for key, idsFromRedis := range dataFromRedis {
			if fromCache == nil {
				fromCache = map[string][]string{}
			}
			if idsFromRedis != nil {
				fromCache[key] = strings.Split(idsFromRedis.(string), " ")
			} else {
				fromCache[key] = nil
			}
		}
	}
	hasNil := false
	totalRows = 0
	minPage := 9999
	maxPage := 0
	for key, keys := range fromCache {
		if keys == nil {
			hasNil = true
			p, _ := strconv.Atoi(key)
			if p < minPage {
				minPage = p
			}
			if p > maxPage {
				maxPage = p
			}
		} else {
			totalRows, _ = strconv.Atoi(keys[0])
			filledPages[key] = keys[1:]
		}
	}
	if hasNil {
		searchPager := NewPager(minPage, maxPage*pageSize)
		results, total := searchIDsWithCount(engine, where, searchPager, entityType)
		totalRows = total
		cacheFields := make([]interface{}, 0)
		for key, ids := range fromCache {
			if ids == nil {
				page := key
				pageInt, _ := strconv.Atoi(page)
				sliceStart := (pageInt - minPage) * pageSize
				if sliceStart > total {
					cacheFields = append(cacheFields, page, total)
					continue
				}
				sliceEnd := sliceStart + pageSize
				if sliceEnd > total {
					sliceEnd = total
				}
				l := len(results)
				if l == 0 {
					cacheFields = append(cacheFields, page, total)
					continue
				}
				sliceEnd = int(math.Min(float64(sliceEnd), float64(l)))
				foundIDs := results[sliceStart:sliceEnd]
				filledPages[key] = make([]string, len(foundIDs))
				cacheValues := engine.getStringBuilder()
				if hasRedis {
					cacheValues.WriteString(strconv.Itoa(total))
				}
				for i, id := range foundIDs {
					cacheKeyValue := engine.getCacheKey(schema, id)
					filledPages[key][i] = cacheKeyValue
					if hasRedis {
						cacheValues.WriteString(" ")
						cacheValues.WriteString(cacheKeyValue)
					}
				}
				cacheFields = append(cacheFields, page, cacheValues.String())
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
			values := []string{strconv.Itoa(totalRows)}
			values = append(values, filledPages[v]...)
			fields[v] = values
		}
		localCache.Set(cacheKey, fields["1"])
	}

	capacity := int(maxCachePageCeil-minCachePageCeil) * pageSize
	resultsKeys := engine.getCacheStrings(0, capacity)
	for i := minCachePageCeil; i < maxCachePageCeil; i++ {
		resultsKeys = append(resultsKeys, filledPages[strconv.Itoa(int(i)+1)]...)
	}
	sliceStart := (pager.GetCurrentPage() - 1) * pager.GetPageSize()
	diff := int(minCachePageCeil) * pageSize
	sliceStart -= diff
	if sliceStart > totalRows {
		return totalRows
	}
	sliceEnd := sliceStart + pager.GetPageSize()
	length := len(resultsKeys)
	if sliceEnd > length {
		sliceEnd = length
	}
	keysToReturn := resultsKeys[sliceStart:sliceEnd]
	_, is := entities.(Entity)
	if !is && len(keysToReturn) > 0 {
		elem := value.Elem()
		_, missing := readByCacheKeys(serializer, engine, keysToReturn, value)
		if missing {
			l := elem.Len()
			missingCounter := 0
			for i := 0; i < l; i++ {
				if elem.Index(i).IsNil() {
					missingCounter++
				}
			}
			if missingCounter > 0 {
				newLength := l - missingCounter
				newSlice := reflect.MakeSlice(elem.Type(), newLength, newLength)
				k := 0
				for i := 0; i < l; i++ {
					val := elem.Index(i)
					if !val.IsNil() {
						newSlice.Index(k).Set(val)
						k++
					}
				}
				totalRows -= missingCounter
				elem.Set(newSlice)
			}
		}
	}
	return totalRows
}

func cachedSearchOne(serializer *serializer, engine *engineImplementation, entity Entity, indexName string, fillStruct bool, arguments []interface{}, references []string) (has bool) {
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
	where := NewWhere(definition.Query, arguments...)
	localCache, hasLocalCache := schema.GetLocalCache(engine)
	if !hasLocalCache && engine.hasRequestCache {
		hasLocalCache = true
		localCache = engine.GetLocalCache(requestCacheKey)
	}
	redisCache, hasRedis := schema.GetRedisCache(engine)
	if !hasLocalCache && !hasRedis {
		panic(fmt.Errorf("cache search not allowed for entity without cache: '%s'", entityType.String()))
	}
	cacheKey := getCacheKeySearch(engine, schema, indexName, where.GetParameters()...)
	var fromCache map[string]interface{}
	if hasLocalCache {
		fromLocalCache, hasInLocalCache := localCache.Get(cacheKey)
		if hasInLocalCache {
			fromCache = map[string]interface{}{"1": fromLocalCache}
		} else {
			fromCache = map[string]interface{}{"1": nil}
		}
	}
	if fromCache["1"] == nil && hasRedis {
		fromCache = redisCache.HMGet(cacheKey, "1")
	}
	id := uint64(0)
	if fromCache["1"] == nil {
		results, _ := searchIDs(engine, where, NewPager(1, 1), false, entityType)
		l := len(results)
		value := strconv.Itoa(l)
		if l > 0 {
			id = results[0]
			value += " " + strconv.FormatUint(results[0], 10)
		}
		if hasLocalCache {
			localCache.Set(cacheKey, value)
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
			has, _, _ = loadByID(serializer, engine, id, entity, nil, true, references...)
		}
		return has
	}
	return false
}

func getCacheKeySearch(engine *engineImplementation, tableSchema *tableSchema, indexName string, parameters ...interface{}) string {
	builder := engine.getStringBuilder()
	builder.WriteString(tableSchema.cachePrefix)
	builder.WriteString("_")
	builder.WriteString(indexName)
	values := fmt.Sprintf("%v", parameters)
	builder.WriteString(strconv.Itoa(int(fnv1a.HashString32(values))))
	return builder.String()
}
