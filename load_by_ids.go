package beeorm

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func tryByIDs(engine *engineImplementation, ids []uint64, entities reflect.Value, references []string) (schema *entitySchema, hasMissing bool) {
	t, has, name := getEntityTypeForSlice(engine.registry, entities.Type(), true)
	if !has {
		panic(fmt.Errorf("entity '%s' is not registered", name))
	}
	resultsSlice := entities.Elem()
	diffCap := len(ids) - resultsSlice.Cap()
	if diffCap > 0 {
		resultsSlice.Grow(diffCap)
	}
	resultsSlice.SetLen(len(ids))
	if len(ids) == 0 {
		return
	}
	for i := range ids {
		resultsSlice.Index(i).SetZero()
	}

	schema = getEntitySchema(engine.registry, t)
	cacheLocal, hasLocalCache := schema.GetLocalCache(engine)
	cacheRedis, hasRedisCache := schema.GetRedisCache(engine)

	foundInCache := 0
	hasCacheNils := false
	if hasLocalCache {
		for i, id := range ids {
			fromLocalCache, hasInLocalCache := cacheLocal.Get(id)
			if hasInLocalCache {
				entity := schema.NewEntity()
				resultsSlice.Index(i).Set(entity.getORM().value)
				if fromLocalCache != cacheNilValue {
					fillFromBinary(engine.getSerializer(nil), engine.registry, fromLocalCache.([]byte), entity)
				} else {
					hasMissing = true
					hasCacheNils = true
				}
				foundInCache++
			}
		}
	}
	if foundInCache < len(ids) && hasRedisCache {
		redisHSetKeys := getMissingIdsFromResults(ids, foundInCache, resultsSlice)
		fromRedisAll := cacheRedis.hMGetUints(schema.cachePrefix, redisHSetKeys...)
		if foundInCache == 0 {
			for i := range redisHSetKeys {
				fromRedisCache := fromRedisAll[i]
				if fromRedisCache != nil {
					entity := schema.NewEntity()
					resultsSlice.Index(i).Set(entity.getORM().value)
					if fromRedisCache != cacheNilValue {
						fillFromBinary(engine.getSerializer(nil), engine.registry, []byte(fromRedisCache.(string)), entity)
					} else {
						hasMissing = true
						hasCacheNils = true
					}
					foundInCache++
				}
			}
		} else {
			for k, id := range redisHSetKeys {
				fromRedisCache := fromRedisAll[k]
				if fromRedisCache != nil {
					for i, idOriginal := range ids {
						if id == idOriginal {
							entity := schema.NewEntity()
							resultsSlice.Index(i).Set(entity.getORM().value)
							if fromRedisCache != cacheNilValue {
								fillFromBinary(engine.getSerializer(nil), engine.registry, []byte(fromRedisCache.(string)), entity)
								if hasLocalCache {
									cacheLocal.Set(id, entity.getORM().copyBinary())
								}
							} else {
								hasMissing = true
								hasCacheNils = true
								if hasLocalCache {
									cacheLocal.Set(id, cacheNilValue)
								}
							}
							foundInCache++
						}
					}
				}
			}
		}
	}
	var redisHSetValues []interface{}
	if foundInCache < len(ids) {
		dbIDs := getMissingIdsFromResults(ids, foundInCache, resultsSlice)
		idsQuery := strings.ReplaceAll(fmt.Sprintf("%v", dbIDs), " ", ",")[1:]
		query := "SELECT " + schema.fieldsQuery + " FROM `" + schema.tableName + "` WHERE `ID` IN (" + idsQuery[:len(idsQuery)-1] + ")"
		results, def := schema.GetMysql(engine).Query(query)
		defer def()
		foundInDB := 0
		for results.Next() {
			foundInDB++
			pointers := prepareScan(schema)
			results.Scan(pointers...)
			entity := schema.NewEntity()
			fillFromDBRow(engine.getSerializer(nil), engine.registry, pointers, entity)
			id := *pointers[0].(*uint64)
			for i, originalID := range ids {
				if id == originalID {
					resultsSlice.Index(i).Set(entity.getORM().value)
				}
			}
			if hasLocalCache {
				cacheLocal.Set(id, entity.getORM().copyBinary())
			}
			if hasRedisCache {
				if len(ids) == 1 {
					cacheRedis.HSet(schema.cachePrefix, id, string(entity.getORM().binary))
				} else {
					redisHSetValues = append(redisHSetValues, id, string(entity.getORM().binary))
				}
			}
		}
		def()
		if redisHSetValues != nil {
			cacheRedis.HSet(schema.cachePrefix, redisHSetValues...)
		}
		if foundInDB < len(dbIDs) {
			for i, id := range ids {
				if resultsSlice.Index(i).IsZero() {
					hasMissing = true
					if !hasLocalCache && !hasRedisCache {
						break
					}
					if hasLocalCache {
						cacheLocal.Set(id, cacheNilValue)
					}
					if hasRedisCache {
						cacheRedis.HSet(schema.cachePrefix, id, cacheNilValue)
					}
				}
			}
		}
	}
	if hasCacheNils {
		for i := range ids {
			inSlice := resultsSlice.Index(i)
			if inSlice.Interface().(Entity).GetID() == 0 {
				inSlice.SetZero()
			}
		}
	}
	return
}

func getMissingIdsFromResults(ids []uint64, foundInCache int, slice reflect.Value) []uint64 {
	if foundInCache == 0 {
		return ids
	}
	result := make([]uint64, len(ids)-foundInCache)
	j := 0
	for i, id := range ids {
		if slice.Index(i).IsZero() {
			result[j] = id
			j++
		}
	}
	return result
}

func tryByIDsOld(serializer *serializer, engine *engineImplementation, ids []uint64, entities reflect.Value, references []string) (schema *entitySchema, hasMissing bool) {
	lenIDs := len(ids)
	newSlice := reflect.MakeSlice(entities.Type(), lenIDs, lenIDs)
	if lenIDs == 0 {
		entities.Set(newSlice)
		return
	}
	t, has, name := getEntityTypeForSlice(engine.registry, entities.Type(), true)
	if !has {
		panic(fmt.Errorf("entity '%s' is not registered", name))
	}

	schema = getEntitySchema(engine.registry, t)
	hasLocalCache := schema.hasLocalCache
	hasRedis := schema.hasRedisCache

	var localCache LocalCache
	var redisCache RedisCache
	hasValid := false

	cacheKeysMap := make(map[uint64]int)
	duplicates := make(map[uint64][]int)
	for i, id := range ids {
		oldValue, hasDuplicate := cacheKeysMap[id]
		if hasDuplicate {
			if len(duplicates[id]) == 0 {
				duplicates[id] = append(duplicates[id], oldValue)
			}
			duplicates[id] = append(duplicates[id], i)
		} else {
			cacheKeysMap[id] = i
		}
	}
	cacheKeys := make([]uint64, len(cacheKeysMap))
	j := 0
	for key := range cacheKeysMap {
		cacheKeys[j] = key
		j++
	}

	var localCacheToSet []interface{}
	var redisCacheToSet []interface{}
	if hasLocalCache {
		localCache, _ = schema.GetLocalCache(engine)
		for i, key := range cacheKeys {
			val, hasInCache := localCache.Get(key)
			if hasInCache {
				if val != cacheNilValue {
					e := schema.NewEntity()
					k := cacheKeysMap[cacheKeys[i]]
					newSlice.Index(k).Set(e.getORM().value)
					fillFromBinary(serializer, engine.registry, val.([]byte), e)
					hasValid = true
				} else {
					hasMissing = true
				}
				cacheKeysMap[cacheKeys[i]] = -1
			}
		}
	}
	j = 0
	for k, v := range cacheKeysMap {
		if v >= 0 {
			cacheKeys[j] = k
			j++
		}
	}
	if hasRedis && j > 0 {
		redisCache, _ = schema.GetRedisCache(engine)
		for i, key := range cacheKeys[0:j] {
			val, hasInCache := redisCache.HGet(schema.cachePrefix, strconv.FormatUint(key, 10))
			if hasInCache {
				if val != cacheNilValue {
					e := schema.NewEntity()
					k := cacheKeysMap[cacheKeys[i]]
					newSlice.Index(k).Set(e.getORM().value)
					fillFromBinary(serializer, engine.registry, []byte(val), e)
					if hasLocalCache {
						localCacheToSet = append(localCacheToSet, cacheKeys[i], e.getORM().copyBinary())
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
			id := *pointers[0].(*uint64)
			e := schema.NewEntity()
			k := cacheKeysMap[id]
			newSlice.Index(k).Set(e.getORM().value)
			fillFromDBRow(serializer, engine.registry, pointers, e)
			if hasLocalCache {
				localCacheToSet = append(localCacheToSet, id, e.getORM().copyBinary())
			}
			if hasRedis {
				redisCacheToSet = append(redisCacheToSet, strconv.FormatUint(id, 10), e.getORM().binary)
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
		redisCache.HSet(schema.cachePrefix, redisCacheToSet...)
	}
	for _, list := range duplicates {
		for _, k := range list[1:] {
			val := newSlice.Index(list[0])
			if val.IsNil() {
				newVal := newSlice.Index(k)
				newVal.Set(reflect.Zero(reflect.PtrTo(schema.t)))
			} else {
				newSlice.Index(k).Set(val.Interface().(Entity).getORM().value)
			}
		}
	}
	entities.Set(newSlice)
	if len(references) > 0 && hasValid {
		warmUpReferences(serializer, engine, schema, entities, references, true)
	}
	return
}

type redisMapType map[string]map[string]map[string][]Entity

func warmUpReferences(serializer *serializer, engine *engineImplementation, schema *entitySchema, rows reflect.Value, references []string, many bool) {
	dbMap := make(map[string]map[*entitySchema]map[string][]Entity)
	var localMap map[string]map[string][]Entity
	var redisMap redisMapType
	l := 1
	if many {
		l = rows.Len()
	}
	if references[0] == "*" {
		references = make([]string, len(schema.references))
		for i, reference := range schema.references {
			references[i] = reference.ColumnName
		}
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
		if !has {
			panic(fmt.Errorf("reference tag %s is not valid", ref))
		}
		parentSchema := engine.registry.entitySchemas[engine.registry.entities[parentRef]]
		hasLocalCache := parentSchema.hasLocalCache
		if hasLocalCache && localMap == nil {
			localMap = make(map[string]map[string][]Entity)
		}
		if parentSchema.hasRedisCache && redisMap == nil {
			redisMap = make(redisMapType)
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
			e := ref.Interface().(Entity)
			if !e.IsLoaded() {
				id := e.GetID()
				if id > 0 {
					fillRefMap(id, referencesNextEntities, refName, e, parentSchema, dbMap, localMap, redisMap)
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
				data := fromCache.([]byte)
				for _, r := range v[key] {
					fillFromBinary(serializer, engine.registry, data, r)
				}
				fillRef(k, key, localMap, redisMap, dbMap)
			}
		} else if l > 1 {
			keys := make([]string, len(v))
			i := 0
			for k := range v {
				keys[i] = k
				i++
			}
			for key, cacheKey := range keys {
				fromCache, hasInCache := engine.GetLocalCache(k).Get(cacheKey)
				if hasInCache && fromCache != cacheNilValue {
					data := fromCache.([]byte)
					for _, r := range v[keys[key]] {
						fillFromBinary(serializer, engine.registry, data, r)
					}
					fillRef(k, keys[key], localMap, redisMap, dbMap)
				}
			}
		}
	}
	for redisCacheName, level1 := range redisMap {
		for hSetKey, level2 := range level1 {
			keys := make([]string, len(level2))
			i := 0
			for k := range level2 {
				keys[i] = k
				i++
			}
			for key, fromCache := range engine.GetRedis(redisCacheName).HMGet(hSetKey, keys...) {
				if fromCache != nil && fromCache != cacheNilValue {
					for _, r := range level2[key] {
						fillFromBinary(serializer, engine.registry, []byte(fromCache.(string)), r)
					}
					fillRef(hSetKey, key, nil, redisMap, dbMap)
				}
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
			query := "SELECT ID" + schema.fieldsQuery + " FROM `" + schema.tableName + "` WHERE `ID` IN (" + strings.Join(q, ",") + ")"
			func() {
				results, def := db.Query(query)
				defer def()
				for results.Next() {
					pointers := prepareScan(schema)
					results.Scan(pointers...)
					id := *pointers[0].(*uint64)
					for _, r := range v2[strconv.FormatUint(id, 10)] {
						fillFromDBRow(serializer, engine.registry, pointers, r)
					}
				}
			}()

		}
	}
	for pool, level1 := range redisMap {
		for cachePrefix, level2 := range level1 {
			values := make([]interface{}, 0)
			for cacheKey, refs := range level2 {
				values = append(values, cacheKey, refs[0].getORM().binary)
			}
			engine.GetRedis(pool).HSet(cachePrefix, values...)
		}
	}
	for pool, v := range localMap {
		if len(v) == 0 {
			continue
		}
		values := make([]interface{}, 0)
		for cacheKey, refs := range v {
			cacheValue := refs[0].getORM().binary
			if len(cacheValue) == 0 {
				values = append(values, cacheKey, cacheNilValue)
			} else {
				values = append(values, cacheKey, cacheValue)
			}
		}
		engine.GetLocalCache(pool).MSet(values...)
	}

	for refName, entities := range referencesNextEntities {
		l := len(entities)
		if l == 1 {
			warmUpReferences(serializer, engine, entities[0].getORM().entitySchema, reflect.ValueOf(entities[0]),
				referencesNextNames[refName], false)
		} else if l > 1 {
			warmUpReferences(serializer, engine, entities[0].getORM().entitySchema, reflect.ValueOf(entities),
				referencesNextNames[refName], true)
		}
	}
}

func fillRef(cachePrefix, key string, localMap map[string]map[string][]Entity,
	redisMap redisMapType, dbMap map[string]map[*entitySchema]map[string][]Entity) {
	for _, p := range localMap {
		delete(p, key)
	}
	for _, p := range redisMap[cachePrefix] {
		delete(p, key)
	}
	for _, p := range dbMap {
		for _, p2 := range p {
			delete(p2, key)
		}
	}
}

func fillRefMap(id uint64, referencesNextEntities map[string][]Entity, refName string, v Entity, parentSchema *entitySchema,
	dbMap map[string]map[*entitySchema]map[string][]Entity,
	localMap map[string]map[string][]Entity, redisMap redisMapType) {
	_, has := referencesNextEntities[refName]
	if has {
		referencesNextEntities[refName] = append(referencesNextEntities[refName], v)
	}
	cacheKey := strconv.FormatUint(id, 10)
	if dbMap[parentSchema.mysqlPoolName] == nil {
		dbMap[parentSchema.mysqlPoolName] = make(map[*entitySchema]map[string][]Entity)
	}
	if dbMap[parentSchema.mysqlPoolName][parentSchema] == nil {
		dbMap[parentSchema.mysqlPoolName][parentSchema] = make(map[string][]Entity)
	}
	dbMap[parentSchema.mysqlPoolName][parentSchema][cacheKey] = append(dbMap[parentSchema.mysqlPoolName][parentSchema][cacheKey], v)
	hasLocalCache := parentSchema.hasLocalCache
	localCacheName := parentSchema.cachePrefix
	if hasLocalCache {
		if localMap[localCacheName] == nil {
			localMap[localCacheName] = make(map[string][]Entity)
		}
		localMap[localCacheName][cacheKey] = append(localMap[localCacheName][cacheKey], v)
	}
	if parentSchema.hasRedisCache {
		if redisMap[parentSchema.redisCacheName] == nil {
			redisMap[parentSchema.redisCacheName] = make(map[string]map[string][]Entity)
		}
		if redisMap[parentSchema.redisCacheName][parentSchema.cachePrefix] == nil {
			redisMap[parentSchema.redisCacheName][parentSchema.cachePrefix] = make(map[string][]Entity)
		}
		redisMap[parentSchema.redisCacheName][parentSchema.cachePrefix][cacheKey] = append(redisMap[parentSchema.redisCacheName][parentSchema.cachePrefix][cacheKey], v)
	}
}
