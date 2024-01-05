package beeorm

import (
	"reflect"
	"strconv"
)

func GetByIDs[E any](orm ORM, ids ...uint64) EntityIterator[E] {
	return getByIDs[E](orm.(*ormImplementation), ids)
}

func getByIDs[E any](orm *ormImplementation, ids []uint64) EntityIterator[E] {
	schema := getEntitySchema[E](orm)
	if len(ids) == 0 {
		return &emptyResultsIterator[E]{}
	}
	if schema.hasLocalCache {
		return &localCacheIDsIterator[E]{orm: orm, schema: schema, ids: ids, index: -1}
	}
	results := &entityIterator[E]{index: -1}
	results.rows = make([]*E, len(ids))
	var missingKeys []int
	cacheRedis, hasRedisCache := schema.GetRedisCache()
	var redisPipeline *RedisPipeLine
	if hasRedisCache {
		redisPipeline = orm.RedisPipeLine(cacheRedis.GetCode())
		l := int64(len(schema.columnNames) + 1)
		lRanges := make([]*PipeLineSlice, len(ids))
		for i, id := range ids {
			lRanges[i] = redisPipeline.LRange(schema.cacheKey+":"+strconv.FormatUint(id, 10), 0, l)
		}
		redisPipeline.Exec(orm)
		for i, id := range ids {
			row := lRanges[i].Result()
			if len(row) > 0 {
				if len(row) == 1 {
					continue
				}
				value := reflect.New(schema.t)
				e := value.Interface().(*E)
				if deserializeFromRedis(row, schema, value.Elem()) && schema.hasLocalCache {
					schema.localCache.setEntity(orm, id, e)
				}
				results.rows[i] = e
			} else {
				missingKeys = append(missingKeys, i)
			}
		}
		if len(missingKeys) == 0 {
			return results
		}
	}
	sql := "SELECT " + schema.fieldsQuery + " FROM `" + schema.GetTableName() + "` WHERE `ID` IN ("
	toSearch := 0
	if len(missingKeys) > 0 {
		for i, key := range missingKeys {
			if i > 0 {
				sql += ","
			}
			sql += strconv.FormatUint(ids[key], 10)
		}
		toSearch = len(missingKeys)
	} else {
		for i, id := range ids {
			if i > 0 {
				sql += ","
			}
			sql += strconv.FormatUint(id, 10)
		}
		toSearch = len(ids)
	}
	sql += ")"
	execRedisPipeline := false
	res, def := schema.GetDB().Query(orm, sql)
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
			schema.localCache.setEntity(orm, id, value.Interface().(*E))
		}
		if hasRedisCache {
			bind := make(Bind)
			err := fillBindFromOneSource(orm, bind, value.Elem(), schema.fields, "")
			checkError(err)
			values := convertBindToRedisValue(bind, schema)
			redisPipeline.RPush(schema.getCacheKey()+":"+strconv.FormatUint(id, 10), values...)
			execRedisPipeline = true
		}
	}
	def()
	if foundInDB < toSearch && (schema.hasLocalCache || hasRedisCache) {
		for i, id := range ids {
			if results.rows[i] == nil {
				if schema.hasLocalCache {
					schema.localCache.setEntity(orm, id, nil)
				}
				if hasRedisCache {
					cacheKey := schema.getCacheKey() + ":" + strconv.FormatUint(id, 10)
					redisPipeline.Del(cacheKey)
					redisPipeline.RPush(cacheKey, cacheNilValue)
					execRedisPipeline = true
				}
			}
		}
	}
	if execRedisPipeline {
		redisPipeline.Exec(orm)
	}
	return results
}

func warmup(orm *ormImplementation, schema *entitySchema, ids []uint64) {
	if len(ids) == 0 {
		return
	}
	var missingKeys []int
	if schema.hasLocalCache {
		for i, id := range ids {
			_, has := schema.localCache.getEntity(orm, id)
			if !has {
				missingKeys = append(missingKeys, i)
			}
		}
		if missingKeys == nil {
			return
		}
	} else {
		missingKeys = make([]int, len(ids))
		for i := range ids {
			missingKeys[i] = i
		}
	}
	cacheRedis, hasRedisCache := schema.GetRedisCache()
	var redisPipeline *RedisPipeLine
	if hasRedisCache {
		redisPipeline = orm.RedisPipeLine(cacheRedis.GetCode())
		l := int64(len(schema.columnNames) + 1)
		lRanges := make([]*PipeLineSlice, len(missingKeys))
		for i, index := range missingKeys {
			lRanges[i] = redisPipeline.LRange(schema.cacheKey+":"+strconv.FormatUint(ids[index], 10), 0, l)
		}
		redisPipeline.Exec(orm)
		hasMissing := false
		for i, index := range missingKeys {
			row := lRanges[i].Result()
			if len(row) > 0 {
				missingKeys[i] = -1
				if len(row) == 1 {
					if schema.hasLocalCache {
						schema.localCache.setEntity(orm, ids[index], nil)
					}
					continue
				}
				value := reflect.New(schema.t)
				e := value.Interface()
				if deserializeFromRedis(row, schema, value.Elem()) && schema.hasLocalCache {
					schema.localCache.setEntity(orm, ids[index], e)
				}
			} else {
				hasMissing = true
			}
		}
		if !hasMissing {
			return
		}
	}
	sql := "SELECT " + schema.fieldsQuery + " FROM `" + schema.GetTableName() + "` WHERE `ID` IN ("
	for i, key := range missingKeys {
		if key < 0 {
			continue
		}
		if i > 0 {
			sql += ","
		}
		sql += strconv.FormatUint(ids[key], 10)
	}
	sql += ")"
	execRedisPipeline := false
	res, def := schema.GetDB().Query(orm, sql)
	defer def()
	foundInDB := 0
	for res.Next() {
		foundInDB++
		pointers := prepareScan(schema)
		res.Scan(pointers...)
		value := reflect.New(schema.t)
		deserializeFromDB(schema.fields, value.Elem(), pointers)
		id := *pointers[0].(*uint64)
		if schema.hasLocalCache || hasRedisCache {
			for i, index := range missingKeys {
				if index >= 0 && ids[index] == id {
					missingKeys[i] = -1
				}
			}
		}
		if schema.hasLocalCache {
			schema.localCache.setEntity(orm, id, value.Interface())
		}
		if hasRedisCache {
			bind := make(Bind)
			err := fillBindFromOneSource(orm, bind, value.Elem(), schema.fields, "")
			checkError(err)
			values := convertBindToRedisValue(bind, schema)
			redisPipeline.RPush(schema.getCacheKey()+":"+strconv.FormatUint(id, 10), values...)
			execRedisPipeline = true
		}
	}
	def()
	if foundInDB < len(missingKeys) && (schema.hasLocalCache || hasRedisCache) {
		for _, index := range missingKeys {
			if index >= 0 {
				if schema.hasLocalCache {
					schema.localCache.setEntity(orm, ids[index], nil)
				}
				if hasRedisCache {
					cacheKey := schema.getCacheKey() + ":" + strconv.FormatUint(ids[index], 10)
					redisPipeline.Del(cacheKey)
					redisPipeline.RPush(cacheKey, cacheNilValue)
					execRedisPipeline = true
				}
			}
		}
	}
	if execRedisPipeline {
		redisPipeline.Exec(orm)
	}
}
