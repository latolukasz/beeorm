package beeorm

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func tryByIDs(serializer *serializer, engine *engineImplementation, ids []uint64, entities reflect.Value, references []string, read bool) (schema *tableSchema, hasMissing bool) {
	lenIDs := len(ids)
	newSlice := entities.Elem()
	newSlice.Grow(lenIDs)
	newSlice.SetLen(lenIDs)
	if lenIDs == 0 {
		return
	}
	t, has, name := getEntityTypeForSlice(engine.registry, entities.Type(), true)
	if !has {
		panic(fmt.Errorf("entity '%s' is not registered", name))
	}
	for i := range ids {
		newSlice.Index(i).SetZero()
	}

	schema = getTableSchema(engine.registry, t)
	hasLocalCache := schema.hasLocalCache
	hasRedis := schema.hasRedisCache

	var localCache *LocalCache
	var redisCache *RedisCache
	hasValid := false

	if !hasLocalCache && engine.hasRequestCache {
		hasLocalCache = true
		localCache = engine.GetLocalCache(requestCacheKey)
	}

	cacheKeysMap := make(map[string]int)
	duplicates := make(map[string][]int)
	for i, id := range ids {
		key := engine.getCacheKey(schema, id)
		oldValue, hasDuplicate := cacheKeysMap[key]
		if hasDuplicate {
			if len(duplicates[key]) == 0 {
				duplicates[key] = append(duplicates[key], oldValue)
			}
			duplicates[key] = append(duplicates[key], i)
		} else {
			cacheKeysMap[key] = i
		}
	}

	var localCacheToSet []interface{}
	var redisCacheToSet []interface{}
	var cacheKeys []string
	if hasRedis {
		cacheKeys = make([]string, len(cacheKeysMap))
		j := 0
		for k := range cacheKeysMap {
			cacheKeys[j] = k
			j++
		}
	}
	if hasLocalCache {
		if localCache == nil {
			localCache, _ = schema.GetLocalCache(engine)
		}
		for key := range cacheKeysMap {
			val, hasInCache := localCache.Get(key)
			if hasInCache {
				if val != cacheNilValue {
					k := cacheKeysMap[key]
					if !read {
						e := schema.NewEntity()
						newSlice.Index(k).Set(e.getORM().value)
						fillFromBinary(serializer, engine.registry, val.(reflect.Value).Interface().(Entity).getORM().binary, e)
					} else {
						newSlice.Index(k).Set(val.(reflect.Value))
					}
					hasValid = true
				} else {
					hasMissing = true
				}
				cacheKeysMap[key] = -1
			}
		}
	}
	j := 0
	if hasRedis {
		for k, v := range cacheKeysMap {
			if v >= 0 {
				cacheKeys[j] = k
				j++
			}
		}
	}
	if j > 0 {
		redisCache, _ = schema.GetRedisCache(engine)
		inCache := redisCache.MGet(cacheKeys[0:j]...)
		for i, val := range inCache {
			if val != nil {
				if val != cacheNilValue {
					e := schema.NewEntity()
					k := cacheKeysMap[cacheKeys[i]]
					newSlice.Index(k).Set(e.getORM().value)
					fillFromBinary(serializer, engine.registry, []byte(val.(string)), e)
					if hasLocalCache {
						localCacheToSet = append(localCacheToSet, cacheKeys[i], e.getORM().value)
					}
					hasValid = true
				} else {
					hasMissing = true
				}
				cacheKeysMap[cacheKeys[i]] = -1
			}
		}
	}
	var idsDB []uint64
	for _, v := range cacheKeysMap {
		if v >= 0 {
			idsDB = append(idsDB, ids[v])
		}
	}
	if len(idsDB) > 0 {
		query := "SELECT " + schema.fieldsQuery + " FROM `" + schema.tableName + "` WHERE `ID` IN (" + strconv.FormatUint(idsDB[0], 10)
		for _, id := range idsDB[1:] {
			query += "," + strconv.FormatUint(id, 10)
		}
		query += ")"
		pool := schema.GetMysql(engine)
		results, def := pool.Query(query)
		defer def()
		found := 0
		for results.Next() {
			pointers := prepareScan(schema)
			results.Scan(pointers...)
			id := *pointers[schema.idIndex].(*uint64)
			cacheKey := engine.getCacheKey(schema, id)
			e := schema.NewEntity()
			k := cacheKeysMap[cacheKey]
			newSlice.Index(k).Set(e.getORM().value)
			fillFromDBRow(serializer, id, engine.registry, pointers, e)
			if hasLocalCache {
				localCacheToSet = append(localCacheToSet, cacheKey, e.getORM().value)
			}
			if hasRedis {
				redisCacheToSet = append(redisCacheToSet, cacheKey, e.getORM().binary)
			}
			hasValid = true
			found++
		}
		def()
		if !hasMissing && found < len(idsDB) {
			hasMissing = true
		}
	}
	if len(localCacheToSet) > 0 && localCache != nil {
		localCache.MSet(localCacheToSet...)
	}
	if len(redisCacheToSet) > 0 && redisCache != nil {
		redisCache.MSet(redisCacheToSet...)
	}
	for _, list := range duplicates {
		for _, k := range list[1:] {
			val := newSlice.Index(list[0])
			if val.IsNil() {
				newVal := newSlice.Index(k)
				newVal.SetZero()
			} else {
				newSlice.Index(k).Set(val.Interface().(Entity).getORM().value)
			}
		}
	}
	if len(references) > 0 && hasValid {
		warmUpReferences(serializer, engine, schema, entities.Elem(), references, true)
	}
	return
}

func readByCacheKeys(serializer *serializer, engine *engineImplementation, keys []string, entities reflect.Value) (schema *tableSchema, hasMissing bool) {
	lenIDs := len(keys)
	newSlice := entities.Elem()
	newSlice.Grow(lenIDs)
	newSlice.SetLen(lenIDs)
	if lenIDs == 0 {
		return
	}
	t, has, name := getEntityTypeForSlice(engine.registry, entities.Type(), true)
	if !has {
		panic(fmt.Errorf("entity '%s' is not registered", name))
	}

	schema = getTableSchema(engine.registry, t)
	hasLocalCache := schema.hasLocalCache
	hasRedis := schema.hasRedisCache

	var localCache *LocalCache
	var redisCache *RedisCache

	if !hasLocalCache && engine.hasRequestCache {
		hasLocalCache = true
		localCache = engine.GetLocalCache(requestCacheKey)
	}

	var localCacheToSet []interface{}
	var redisCacheToSet []interface{}
	missingMap := map[uint64]int{}
	if hasLocalCache {
		if localCache == nil {
			localCache, _ = schema.GetLocalCache(engine)
		}
		for k, key := range keys {
			val, hasInCache := localCache.Get(key)
			if hasInCache {
				if val != cacheNilValue {
					newSlice.Index(k).Set(val.(reflect.Value))
				} else {
					hasMissing = true
					newSlice.Index(k).SetZero()
				}
			} else {
				id, _ := strconv.ParseUint(key[strings.LastIndex(key, ":")+1:], 10, 64)
				missingMap[id] = k
			}
		}
	} else {
		for k, key := range keys {
			id, _ := strconv.ParseUint(key[strings.LastIndex(key, ":")+1:], 10, 64)
			missingMap[id] = k
		}
	}
	if hasRedis && len(missingMap) > 0 {
		redisCache, _ = schema.GetRedisCache(engine)
		cacheKeys := make([]string, len(missingMap))
		j := 0
		for _, i := range missingMap {
			cacheKeys[j] = keys[i]
			j++
		}
		inCache := redisCache.MGet(cacheKeys[0:j]...)
		for i, val := range inCache {
			if val != nil {
				if val != cacheNilValue {
					e := schema.NewEntity()
					fillFromBinary(serializer, engine.registry, []byte(val.(string)), e)
					id := e.GetID()
					k := missingMap[id]
					newSlice.Index(k).Set(e.getORM().value)
					if hasLocalCache {
						localCacheToSet = append(localCacheToSet, cacheKeys[i], e.getORM().value)
					}
					delete(missingMap, id)
				} else {
					hasMissing = true
				}
			}
		}
	}
	if len(missingMap) > 0 {
		idsDB := make([]uint64, len(missingMap))
		j := 0
		for id := range missingMap {
			idsDB[j] = id
			j++
		}
		query := "SELECT " + schema.fieldsQuery + " FROM `" + schema.tableName + "` WHERE `ID` IN (" + strconv.FormatUint(idsDB[0], 10)
		for _, id := range idsDB[1:] {
			query += "," + strconv.FormatUint(id, 10)
		}
		query += ")"
		pool := schema.GetMysql(engine)
		results, def := pool.Query(query)
		defer def()
		found := 0
		for results.Next() {
			pointers := prepareScan(schema)
			results.Scan(pointers...)
			id := *pointers[schema.idIndex].(*uint64)
			e := schema.NewEntity()
			k := missingMap[id]
			newSlice.Index(k).Set(e.getORM().value)
			fillFromDBRow(serializer, id, engine.registry, pointers, e)
			cacheKey := keys[k]
			if hasLocalCache {
				localCacheToSet = append(localCacheToSet, cacheKey, e.getORM().value)
			}
			if hasRedis {
				redisCacheToSet = append(redisCacheToSet, cacheKey, e.getORM().binary)
			}
			found++
		}
		def()
		if !hasMissing && found < len(idsDB) {
			hasMissing = true
		}
	}
	if len(localCacheToSet) > 0 && localCache != nil {
		localCache.MSet(localCacheToSet...)
	}
	if len(redisCacheToSet) > 0 && redisCache != nil {
		redisCache.MSet(redisCacheToSet...)
	}
	return
}

func warmUpReferences(serializer *serializer, engine *engineImplementation, schema *tableSchema, rows reflect.Value, references []string, many bool) {
	dbMap := make(map[string]map[*tableSchema]map[string][]Entity)
	var localMap map[string]map[string][]Entity
	var redisMap map[string]map[string][]Entity
	l := 1
	if many {
		l = rows.Len()
	}
	if references[0] == "*" {
		references = schema.refOne
	}
	var referencesNextNames map[string][]string
	var referencesNextEntities map[string][]Entity
	for _, ref := range references {
		refName := ref
		pos := strings.Index(refName, "/")
		if pos > 0 {
			if referencesNextNames == nil {
				referencesNextNames = make(map[string][]string)
			}
			if referencesNextEntities == nil {
				referencesNextEntities = make(map[string][]Entity)
			}
			nextRef := refName[pos+1:]
			refName = refName[0:pos]
			referencesNextNames[refName] = append(referencesNextNames[refName], nextRef)
			referencesNextEntities[refName] = nil
		}
		_, has := schema.tags[refName]
		if !has {
			panic(fmt.Errorf("reference %s in %s is not valid", ref, schema.tableName))
		}
		parentRef, has := schema.tags[refName]["ref"]
		manyRef := false
		if !has {
			parentRef, has = schema.tags[refName]["refs"]
			manyRef = true
			if !has {
				panic(fmt.Errorf("reference tag %s is not valid", ref))
			}
		}
		parentSchema := engine.registry.tableSchemas[engine.registry.entities[parentRef]]
		hasLocalCache := parentSchema.hasLocalCache
		if !hasLocalCache && engine.hasRequestCache {
			hasLocalCache = true
		}
		if hasLocalCache && localMap == nil {
			localMap = make(map[string]map[string][]Entity)
		}
		if parentSchema.hasRedisCache && redisMap == nil {
			redisMap = make(map[string]map[string][]Entity)
		}
		for i := 0; i < l; i++ {
			var ref reflect.Value
			var refEntity reflect.Value
			if many {
				refEntity = rows.Index(i)
				if refEntity.IsZero() {
					continue
				}
				ref = reflect.Indirect(refEntity.Elem()).FieldByName(refName)
			} else {
				refEntity = rows
				ref = reflect.Indirect(refEntity).FieldByName(refName)
			}
			if !ref.IsValid() || ref.IsZero() {
				continue
			}
			if manyRef {
				length := ref.Len()
				for i := 0; i < length; i++ {
					e := ref.Index(i).Interface().(Entity)
					if !e.IsLoaded() {
						id := e.GetID()
						if id > 0 {
							fillRefMap(engine, id, referencesNextEntities, refName, e, parentSchema, dbMap, localMap, redisMap)
						}
					}
				}
			} else {
				e := ref.Interface().(Entity)
				if !e.IsLoaded() {
					id := e.GetID()
					if id > 0 {
						fillRefMap(engine, id, referencesNextEntities, refName, e, parentSchema, dbMap, localMap, redisMap)
					}
				}
			}
		}
	}
	for k, v := range localMap {
		l := len(v)
		if l == 1 {
			var key string
			for k := range v {
				key = k
				break
			}
			fromCache, has := engine.GetLocalCache(k).Get(key)
			if has && fromCache != cacheNilValue {
				data := fromCache.(reflect.Value).Interface().(Entity).getORM().binary
				for _, r := range v[key] {
					fillFromBinary(serializer, engine.registry, data, r)
				}
				fillRef(key, localMap, redisMap, dbMap)
			}
		} else if l > 1 {
			keys := make([]string, len(v))
			i := 0
			for k := range v {
				keys[i] = k
				i++
			}
			for key, fromCache := range engine.GetLocalCache(k).MGet(keys...) {
				if fromCache != nil && fromCache != cacheNilValue {
					data := fromCache.(reflect.Value).Interface().(Entity).getORM().binary
					for _, r := range v[keys[key]] {
						fillFromBinary(serializer, engine.registry, data, r)
					}
					fillRef(keys[key], localMap, redisMap, dbMap)
				}
			}
		}
	}
	for k, v := range redisMap {
		l := len(v)
		if l == 0 {
			continue
		}
		keys := make([]string, l)
		i := 0
		for k := range v {
			keys[i] = k
			i++
		}
		for key, fromCache := range engine.GetRedis(k).MGet(keys...) {
			if fromCache != nil && fromCache != cacheNilValue {
				for _, r := range v[keys[key]] {
					fillFromBinary(serializer, engine.registry, []byte(fromCache.(string)), r)
				}
				fillRef(keys[key], nil, redisMap, dbMap)
			}
		}
	}
	for k, v := range dbMap {
		db := engine.GetMysql(k)
		for schema, v2 := range v {
			if len(v2) == 0 {
				continue
			}
			keys := make([]string, len(v2))
			q := make([]string, len(v2))
			i := 0
			for k2 := range v2 {
				keys[i] = k2[strings.Index(k2, ":")+1:]
				q[i] = keys[i]
				i++
			}
			query := "SELECT " + schema.fieldsQuery + " FROM `" + schema.tableName + "` WHERE `ID` IN (" + strings.Join(q, ",") + ")"
			results, def := db.Query(query)
			for results.Next() {
				pointers := prepareScan(schema)
				results.Scan(pointers...)
				id := *pointers[schema.idIndex].(*uint64)
				for _, r := range v2[engine.getCacheKey(schema, id)] {
					fillFromDBRow(serializer, id, engine.registry, pointers, r)
				}
			}
			def()
		}
	}
	for pool, v := range redisMap {
		if len(v) == 0 {
			continue
		}
		values := make([]interface{}, 0)
		for cacheKey, refs := range v {
			values = append(values, cacheKey, refs[0].getORM().binary)
		}
		engine.GetRedis(pool).MSet(values...)
	}
	for pool, v := range localMap {
		if len(v) == 0 {
			continue
		}
		values := make([]interface{}, 0)
		for cacheKey, refs := range v {
			if len(refs[0].getORM().binary) == 0 {
				values = append(values, cacheKey, cacheNilValue)
			} else {
				values = append(values, cacheKey, refs[0].getORM().value)
			}
		}
		engine.GetLocalCache(pool).MSet(values...)
	}

	for refName, entities := range referencesNextEntities {
		l := len(entities)
		if l == 1 {
			warmUpReferences(serializer, engine, entities[0].getORM().tableSchema, reflect.ValueOf(entities[0]),
				referencesNextNames[refName], false)
		} else if l > 1 {
			warmUpReferences(serializer, engine, entities[0].getORM().tableSchema, reflect.ValueOf(entities),
				referencesNextNames[refName], true)
		}
	}
}

func fillRef(key string, localMap map[string]map[string][]Entity,
	redisMap map[string]map[string][]Entity, dbMap map[string]map[*tableSchema]map[string][]Entity) {
	for _, p := range localMap {
		delete(p, key)
	}
	for _, p := range redisMap {
		delete(p, key)
	}
	for _, p := range dbMap {
		for _, p2 := range p {
			delete(p2, key)
		}
	}
}

func fillRefMap(engine *engineImplementation, id uint64, referencesNextEntities map[string][]Entity, refName string, v Entity, parentSchema *tableSchema,
	dbMap map[string]map[*tableSchema]map[string][]Entity,
	localMap map[string]map[string][]Entity, redisMap map[string]map[string][]Entity) {
	_, has := referencesNextEntities[refName]
	if has {
		referencesNextEntities[refName] = append(referencesNextEntities[refName], v)
	}
	cacheKey := engine.getCacheKey(parentSchema, id)
	if dbMap[parentSchema.mysqlPoolName] == nil {
		dbMap[parentSchema.mysqlPoolName] = make(map[*tableSchema]map[string][]Entity)
	}
	if dbMap[parentSchema.mysqlPoolName][parentSchema] == nil {
		dbMap[parentSchema.mysqlPoolName][parentSchema] = make(map[string][]Entity)
	}
	dbMap[parentSchema.mysqlPoolName][parentSchema][cacheKey] = append(dbMap[parentSchema.mysqlPoolName][parentSchema][cacheKey], v)
	hasLocalCache := parentSchema.hasLocalCache
	localCacheName := parentSchema.localCacheName
	if !hasLocalCache && engine.hasRequestCache {
		hasLocalCache = true
		localCacheName = requestCacheKey
	}
	if hasLocalCache {
		if localMap[localCacheName] == nil {
			localMap[localCacheName] = make(map[string][]Entity)
		}
		localMap[localCacheName][cacheKey] = append(localMap[localCacheName][cacheKey], v)
	}
	if parentSchema.hasRedisCache {
		if redisMap[parentSchema.redisCacheName] == nil {
			redisMap[parentSchema.redisCacheName] = make(map[string][]Entity)
		}
		redisMap[parentSchema.redisCacheName][cacheKey] = append(redisMap[parentSchema.redisCacheName][cacheKey], v)
	}
}
