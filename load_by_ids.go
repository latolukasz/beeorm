package beeorm

//func warmUpReferences(c Context, schema *entitySchema, rows reflect.Value, references []string, many bool) {
//	dbMap := make(map[string]map[*entitySchema]map[string][]Entity)
//	var localMap map[string]map[string][]Entity
//	var redisMap redisMapType
//	l := 1
//	if many {
//		l = rows.Len()
//	}
//	if references[0] == "*" {
//		references = make([]string, len(schema.references))
//		for i, reference := range schema.references {
//			references[i] = reference.ColumnName
//		}
//	}
//	var referencesNextNames map[string][]string
//	var referencesNextEntities map[string][]Entity
//	for _, ref := range references {
//		refName := ref
//		pos := strings.Index(refName, "/")
//		if pos > 0 {
//			if referencesNextNames == nil {
//				referencesNextNames = make(map[string][]string)
//			}
//			if referencesNextEntities == nil {
//				referencesNextEntities = make(map[string][]Entity)
//			}
//			nextRef := refName[pos+1:]
//			refName = refName[0:pos]
//			referencesNextNames[refName] = append(referencesNextNames[refName], nextRef)
//			referencesNextEntities[refName] = nil
//		}
//		_, has := schema.tags[refName]
//		if !has {
//			panic(fmt.Errorf("reference %s in %s is not valid", ref, schema.tableName))
//		}
//		parentRef, has := schema.tags[refName]["ref"]
//		if !has {
//			panic(fmt.Errorf("reference tag %s is not valid", ref))
//		}
//		parentSchema := engine.registry.entitySchemas[engine.registry.entities[parentRef]]
//		hasLocalCache := parentSchema.hasLocalCache
//		if hasLocalCache && localMap == nil {
//			localMap = make(map[string]map[string][]Entity)
//		}
//		if parentSchema.hasRedisCache && redisMap == nil {
//			redisMap = make(redisMapType)
//		}
//		for i := 0; i < l; i++ {
//			var ref reflect.Value
//			var refEntity reflect.Value
//			if many {
//				refEntity = rows.Index(i)
//				if refEntity.IsZero() {
//					continue
//				}
//				ref = reflect.Indirect(refEntity.Elem()).FieldByName(refName)
//			} else {
//				refEntity = rows
//				ref = reflect.Indirect(refEntity).FieldByName(refName)
//			}
//			if !ref.IsValid() || ref.IsZero() {
//				continue
//			}
//			e := ref.Interface().(Entity)
//			if !e.IsLoaded() {
//				id := e.GetID()
//				if id > 0 {
//					fillRefMap(id, referencesNextEntities, refName, e, parentSchema, dbMap, localMap, redisMap)
//				}
//			}
//		}
//	}
//	for k, v := range localMap {
//		l := len(v)
//		if l == 1 {
//			var key string
//			for k := range v {
//				key = k
//				break
//			}
//			fromCache, has := engine.GetLocalCache(k).Get(key)
//			if has && fromCache != cacheNilValue {
//				data := fromCache.([]byte)
//				for _, r := range v[key] {
//					fillFromBinary(serializer, engine.registry, data, r)
//				}
//				fillRef(k, key, localMap, redisMap, dbMap)
//			}
//		} else if l > 1 {
//			keys := make([]string, len(v))
//			i := 0
//			for k := range v {
//				keys[i] = k
//				i++
//			}
//			for key, cacheKey := range keys {
//				fromCache, hasInCache := engine.GetLocalCache(k).Get(cacheKey)
//				if hasInCache && fromCache != cacheNilValue {
//					data := fromCache.([]byte)
//					for _, r := range v[keys[key]] {
//						fillFromBinary(serializer, engine.registry, data, r)
//					}
//					fillRef(k, keys[key], localMap, redisMap, dbMap)
//				}
//			}
//		}
//	}
//	for redisCacheName, level1 := range redisMap {
//		for hSetKey, level2 := range level1 {
//			keys := make([]string, len(level2))
//			i := 0
//			for k := range level2 {
//				keys[i] = k
//				i++
//			}
//			for key, fromCache := range engine.GetRedis(redisCacheName).HMGet(hSetKey, keys...) {
//				if fromCache != nil && fromCache != cacheNilValue {
//					for _, r := range level2[key] {
//						fillFromBinary(serializer, engine.registry, []byte(fromCache.(string)), r)
//					}
//					fillRef(hSetKey, key, nil, redisMap, dbMap)
//				}
//			}
//		}
//	}
//	for k, v := range dbMap {
//		db := engine.GetMysql(k)
//		for schema, v2 := range v {
//			if len(v2) == 0 {
//				continue
//			}
//			keys := make([]string, len(v2))
//			q := make([]string, len(v2))
//			i := 0
//			for k2 := range v2 {
//				keys[i] = k2[strings.Index(k2, ":")+1:]
//				q[i] = keys[i]
//				i++
//			}
//			query := "SELECT ID" + schema.fieldsQuery + " FROM `" + schema.tableName + "` WHERE `ID` IN (" + strings.Join(q, ",") + ")"
//			func() {
//				results, def := db.Query(query)
//				defer def()
//				for results.Next() {
//					pointers := prepareScan(schema)
//					results.Scan(pointers...)
//					id := *pointers[0].(*uint64)
//					for _, r := range v2[strconv.FormatUint(id, 10)] {
//						fillFromDBRow(serializer, engine.registry, pointers, r)
//					}
//				}
//			}()
//
//		}
//	}
//	for pool, level1 := range redisMap {
//		for cachePrefix, level2 := range level1 {
//			values := make([]interface{}, 0)
//			for cacheKey, refs := range level2 {
//				values = append(values, cacheKey, refs[0].getORM().binary)
//			}
//			engine.GetRedis(pool).HSet(cachePrefix, values...)
//		}
//	}
//	for pool, v := range localMap {
//		if len(v) == 0 {
//			continue
//		}
//		values := make([]interface{}, 0)
//		for cacheKey, refs := range v {
//			cacheValue := refs[0].getORM().binary
//			if len(cacheValue) == 0 {
//				values = append(values, cacheKey, cacheNilValue)
//			} else {
//				values = append(values, cacheKey, cacheValue)
//			}
//		}
//		engine.GetLocalCache(pool).MSet(values...)
//	}
//
//	for refName, entities := range referencesNextEntities {
//		l := len(entities)
//		if l == 1 {
//			warmUpReferences(serializer, engine, entities[0].getORM().entitySchema, reflect.ValueOf(entities[0]),
//				referencesNextNames[refName], false)
//		} else if l > 1 {
//			warmUpReferences(serializer, engine, entities[0].getORM().entitySchema, reflect.ValueOf(entities),
//				referencesNextNames[refName], true)
//		}
//	}
//}
//
//func fillRef(cachePrefix, key string, localMap map[string]map[string][]Entity,
//	redisMap redisMapType, dbMap map[string]map[*entitySchema]map[string][]Entity) {
//	for _, p := range localMap {
//		delete(p, key)
//	}
//	for _, p := range redisMap[cachePrefix] {
//		delete(p, key)
//	}
//	for _, p := range dbMap {
//		for _, p2 := range p {
//			delete(p2, key)
//		}
//	}
//}

//func fillRefMap(id uint64, referencesNextEntities map[string][]Entity, refName string, v Entity, parentSchema *entitySchema,
//	dbMap map[string]map[*entitySchema]map[string][]Entity,
//	localMap map[string]map[string][]Entity, redisMap redisMapType) {
//	_, has := referencesNextEntities[refName]
//	if has {
//		referencesNextEntities[refName] = append(referencesNextEntities[refName], v)
//	}
//	cacheKey := strconv.FormatUint(id, 10)
//	if dbMap[parentSchema.mysqlPoolName] == nil {
//		dbMap[parentSchema.mysqlPoolName] = make(map[*entitySchema]map[string][]Entity)
//	}
//	if dbMap[parentSchema.mysqlPoolName][parentSchema] == nil {
//		dbMap[parentSchema.mysqlPoolName][parentSchema] = make(map[string][]Entity)
//	}
//	dbMap[parentSchema.mysqlPoolName][parentSchema][cacheKey] = append(dbMap[parentSchema.mysqlPoolName][parentSchema][cacheKey], v)
//	hasLocalCache := parentSchema.hasLocalCache
//	localCacheName := parentSchema.cachePrefix
//	if hasLocalCache {
//		if localMap[localCacheName] == nil {
//			localMap[localCacheName] = make(map[string][]Entity)
//		}
//		localMap[localCacheName][cacheKey] = append(localMap[localCacheName][cacheKey], v)
//	}
//	if parentSchema.hasRedisCache {
//		if redisMap[parentSchema.redisCacheName] == nil {
//			redisMap[parentSchema.redisCacheName] = make(map[string]map[string][]Entity)
//		}
//		if redisMap[parentSchema.redisCacheName][parentSchema.cachePrefix] == nil {
//			redisMap[parentSchema.redisCacheName][parentSchema.cachePrefix] = make(map[string][]Entity)
//		}
//		redisMap[parentSchema.redisCacheName][parentSchema.cachePrefix][cacheKey] = append(redisMap[parentSchema.redisCacheName][parentSchema.cachePrefix][cacheKey], v)
//	}
//}
