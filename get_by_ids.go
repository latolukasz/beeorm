package beeorm

import (
	"reflect"
	"strconv"
)

func GetByIDs[E any](c Context, ids ...uint64) EntityIterator[E] {
	return getByIDs[E](c.(*contextImplementation), ids)
}

func getByIDs[E any](c *contextImplementation, ids []uint64) EntityIterator[E] {
	schema := getEntitySchema[E](c)
	if len(ids) == 0 {
		return &emptyResultsIterator[E]{}
	}
	if schema.hasLocalCache {
		return &localCacheIDsIterator[E]{c: c, schema: schema, ids: ids, index: -1}
	}
	results := &entityIterator[E]{index: -1}
	results.rows = make([]*E, len(ids))
	var missingKeys []int
	cacheRedis, hasRedisCache := schema.GetRedisCache()
	var redisPipeline *RedisPipeLine
	if hasRedisCache {
		redisPipeline = c.RedisPipeLine(cacheRedis.GetCode())
		l := int64(len(schema.columnNames) + 1)
		var lRanges []*PipeLineSlice
		if schema.hasLocalCache {
			lRanges = make([]*PipeLineSlice, len(missingKeys))
			for i, key := range missingKeys {
				lRanges[i] = redisPipeline.LRange(schema.cacheKey+":"+strconv.FormatUint(ids[key], 10), 0, l)
			}
		} else {
			lRanges = make([]*PipeLineSlice, len(ids))
			for i, id := range ids {
				lRanges[i] = redisPipeline.LRange(schema.cacheKey+":"+strconv.FormatUint(id, 10), 0, l)
			}
		}
		redisPipeline.Exec(c)
		if schema.hasLocalCache {
			hasZero := false
			for i, key := range missingKeys {
				row := lRanges[i].Result()
				if len(row) > 0 {
					if len(row) == 1 {
						schema.localCache.setEntity(c, ids[key], nil)
					} else {
						value := reflect.New(schema.t)
						e := value.Interface().(*E)
						if deserializeFromRedis(row, schema, value.Elem()) && schema.hasLocalCache {
							schema.localCache.setEntity(c, ids[key], e)
						}
						results.rows[key] = e
					}
					missingKeys[i] = 0
					hasZero = true
				} else {
					missingKeys[i] = key
				}
			}
			if hasZero {
				k := 0
				for _, id := range missingKeys {
					if id > 0 {
						missingKeys[k] = id
						k++
					}
				}
				missingKeys = missingKeys[0:k]
			}
		} else {
			for i, id := range ids {
				row := lRanges[i].Result()
				if len(row) > 0 {
					if len(row) == 1 {
						continue
					}
					value := reflect.New(schema.t)
					e := value.Interface().(*E)
					if deserializeFromRedis(row, schema, value.Elem()) && schema.hasLocalCache {
						schema.localCache.setEntity(c, id, e)
					}
					results.rows[i] = e
				} else {
					missingKeys = append(missingKeys, i)
				}
			}
		}
		if len(missingKeys) == 0 {
			return results
		}
	}
	sBuilder := c.getStringBuilder()
	sBuilder.WriteString("SELECT " + schema.getFieldsQuery() + " FROM `" + schema.GetTableName() + "` WHERE `ID` IN (")
	toSearch := 0
	if len(missingKeys) > 0 {
		for i, key := range missingKeys {
			if i > 0 {
				sBuilder.WriteString(",")
			}
			sBuilder.WriteString(strconv.FormatUint(ids[key], 10))
		}
		toSearch = len(missingKeys)
	} else {
		for i, id := range ids {
			if i > 0 {
				sBuilder.WriteString(",")
			}
			sBuilder.WriteString(strconv.FormatUint(id, 10))
		}
		toSearch = len(ids)
	}

	sBuilder.WriteString(")")
	execRedisPipeline := false
	res, def := schema.GetDB().Query(c, sBuilder.String())
	defer def()
	foundInDB := 0
	for res.Next() {
		foundInDB++
		pointers := prepareScan(schema)
		res.Scan(pointers...)
		value := reflect.New(schema.t)
		deserializeFromDB(schema.fields, value.Elem(), pointers)
		id := *pointers[0].(*uint64)
		for i, originalID := range ids { // TODO too slow
			if id == originalID {
				results.rows[i] = value.Interface().(*E)
			}
		}
		if schema.hasLocalCache {
			schema.localCache.setEntity(c, id, value.Interface().(*E))
		}
		if hasRedisCache {
			bind := make(Bind)
			err := fillBindFromOneSource(c, bind, value.Elem(), schema.fields, "")
			checkError(err)
			values := convertBindToRedisValue(bind, schema)
			redisPipeline.RPush(schema.GetCacheKey()+":"+strconv.FormatUint(id, 10), values...)
			execRedisPipeline = true
		}
	}
	def()
	if foundInDB < toSearch {
		for i, id := range ids {
			if results.rows[i] == nil {
				if !schema.hasLocalCache && !hasRedisCache {
					break
				}
				if schema.hasLocalCache {
					schema.localCache.setEntity(c, id, nil)
				}
				if hasRedisCache {
					cacheKey := schema.GetCacheKey() + ":" + strconv.FormatUint(id, 10)
					redisPipeline.Del(cacheKey)
					redisPipeline.RPush(cacheKey, cacheNilValue)
					execRedisPipeline = true
				}
			}
		}
	}
	if execRedisPipeline {
		redisPipeline.Exec(c)
	}
	return results
}
