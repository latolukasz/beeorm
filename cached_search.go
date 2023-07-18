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

func CachedSearchOne[E Entity](c Context, indexName string, arguments ...interface{}) E {
	return cachedSearchOne[E](c, indexName, arguments, nil)
}

func CachedSearchOneWithReferences[E Entity](c Context, indexName string, arguments []interface{}, references []string) E {
	return cachedSearchOne[E](c, indexName, arguments, references)
}

func CachedSearch(c Context, entities interface{}, indexName string, pager *Pager, arguments ...interface{}) (totalRows int) {
	totalRows, _ = cachedSearch(c, entities, indexName, pager, arguments, nil)
	return
}

func CachedSearchWithReferences(c Context, entities interface{}, indexName string, pager *Pager, arguments []interface{}, references []string) (totalRows int) {
	totalRows, _ = cachedSearch(c, entities, indexName, pager, arguments, references)
	return
}

func cachedSearch(c Context, entities interface{}, indexName string, pager *Pager,
	arguments []interface{}, references []string) (totalRows int, ids []uint64) {
	value := reflect.ValueOf(entities)
	schema, hasSchema, name := getEntitySchemaForSlice(c.Engine(), value.Type(), true)
	if !hasSchema {
		panic(fmt.Errorf("entity '%s' is not registered", name))
	}
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
	cacheLocal, hasLocalCache := schema.GetLocalCache()
	cacheRedis, hasRedis := schema.GetRedisCache()
	if !hasLocalCache && !hasRedis {
		panic(fmt.Errorf("cache search not allowed for entity without cache: '%s'", schema.t.String()))
	}
	where := NewWhere(definition.Query, arguments...)
	cacheKey := getCacheKeySearch(schema, indexName, where.GetParameters()...)

	pageSize := idsOnCachePage
	if hasLocalCache {
		pageSize = definition.Max
	}
	minCachePage := float64((pager.GetCurrentPage() - 1) * pager.GetPageSize() / pageSize)
	minCachePageCeil := minCachePage
	maxCachePage := float64((pager.GetCurrentPage()-1)*pager.GetPageSize()+pager.GetPageSize()) / float64(pageSize)
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
		fromCacheLocal, hasInLocalCache := cacheLocal.Get(c, cacheKey)
		if hasInLocalCache {
			fromCache = map[string]interface{}{"1": fromCacheLocal}
		} else {
			fromCache = map[string]interface{}{"1": nil}
			nilsKeys = append(nilsKeys, "1")
		}
		if hasRedis && len(nilsKeys) > 0 {
			fromRedis := cacheRedis.HMGet(c, cacheKey, nilsKeys...)
			for key, idsFromRedis := range fromRedis {
				if idsFromRedis != nil {
					ids := strings.Split(idsFromRedis.(string), " ")
					length := len(ids)
					idsAsUint := make([]uint64, length)
					for i := 0; i < length; i++ {
						idsAsUint[i], _ = strconv.ParseUint(ids[i], 10, 64)
					}
					fromCache[key] = idsAsUint
				} else {
					fromCache[key] = idsFromRedis
				}
			}
		}
	} else if hasRedis {
		fromRedis = true
		fromCache = cacheRedis.HMGet(c, cacheKey, pages...)
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
		searchPager := NewPager(minPage, maxPage*pageSize)
		results, total := searchIDs(c, where, searchPager, true)
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
			cacheRedis.HSet(c, cacheKey, cacheFields...)
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
		cacheLocal.Set(c, cacheKey, fields["1"])
	}

	resultsIDs := make([]uint64, 0)
	for i := minCachePageCeil; i < maxCachePageCeil; i++ {
		resultsIDs = append(resultsIDs, filledPages[strconv.Itoa(int(i)+1)]...)
	}
	sliceStart := (pager.GetCurrentPage() - 1) * pager.GetPageSize()
	diff := int(minCachePageCeil) * pageSize
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
	if !is && len(idsToReturn) > 0 {
		elem := value.Elem()
		_, missing := getByIDs(c, idsToReturn, elem, references)
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
	return totalRows, idsToReturn
}

func cachedSearchOne[E Entity](c Context, indexName string, arguments []interface{}, references []string) (entity E) {
	value := reflect.ValueOf(entity)
	entityType := value.Elem().Type()
	schema := GetEntitySchema[E](c)
	if schema == nil {
		panic(fmt.Errorf("entity '%s' is not registered", entityType.String()))
	}
	definition, has := schema.cachedIndexesOne[indexName]
	if !has {
		panic(fmt.Errorf("index %s not found", indexName))
	}
	where := NewWhere(definition.Query, arguments...)
	cacheLocal, hasLocalCache := schema.GetLocalCache()
	cacheRedis, hasRedis := schema.GetRedisCache()
	if !hasLocalCache && !hasRedis {
		panic(fmt.Errorf("cache search not allowed for entity without cache: '%s'", entityType.String()))
	}
	cacheKey := getCacheKeySearch(schema, indexName, where.GetParameters()...)
	var fromCache map[string]interface{}
	if hasLocalCache {
		fromLocalCache, hasInLocalCache := cacheLocal.Get(c, cacheKey)
		if hasInLocalCache {
			fromCache = map[string]interface{}{"1": fromLocalCache}
		} else {
			fromCache = map[string]interface{}{"1": nil}
		}
	}
	if fromCache["1"] == nil && hasRedis {
		fromCache = cacheRedis.HMGet(c, cacheKey, "1")
	}
	id := uint64(0)
	if fromCache["1"] == nil {
		results, _ := searchIDs[E](c, where, NewPager(1, 1), false)
		l := len(results)
		value := strconv.Itoa(l)
		if l > 0 {
			id = results[0]
			value += " " + strconv.FormatUint(results[0], 10)
		}
		if hasLocalCache {
			cacheLocal.Set(c, cacheKey, value)
		}
		if hasRedis {
			cacheRedis.HSet(c, cacheKey, "1", value)
		}
	} else {
		ids := strings.Split(fromCache["1"].(string), " ")
		if ids[0] != "0" {
			id, _ = strconv.ParseUint(ids[1], 10, 64)
		}
	}
	if id > 0 {
		entity = GetByID[E](c, id)
	}
	return entity
}

func getCacheKeySearch(entitySchema EntitySchema, indexName string, parameters ...interface{}) string {
	return entitySchema.cachePrefix + "_" + indexName + strconv.Itoa(int(fnv1a.HashString32(fmt.Sprintf("%v", parameters))))
}
