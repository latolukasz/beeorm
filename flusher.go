package beeorm

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type Bind map[string]interface{}

type DuplicatedKeyError struct {
	Message string
	Index   string
}

func (err *DuplicatedKeyError) Error() string {
	return err.Message
}

type ForeignKeyError struct {
	Message    string
	Constraint string
}

func (err *ForeignKeyError) Error() string {
	return err.Message
}

type Flusher interface {
	Track(entity ...Entity) Flusher
	Flush()
	FlushWithCheck() error
	FlushInTransactionWithCheck() error
	FlushWithFullCheck() error
	FlushLazy()
	FlushInTransaction()
	Clear()
	Delete(entity ...Entity) Flusher
	ForceDelete(entity ...Entity) Flusher
}

type flusher struct {
	engine                 *Engine
	trackedEntities        []Entity
	trackedEntitiesCounter int
	redisFlusher           *redisFlusher
	updateSQLs             map[string][]string
	deleteBinds            map[reflect.Type]map[uint64]Entity
	lazyMap                map[string]interface{}
	localCacheDeletes      map[string][]string
	localCacheSets         map[string][]interface{}
}

func (f *flusher) Track(entity ...Entity) Flusher {
	for _, entity := range entity {
		initIfNeeded(f.engine.registry, entity)
		if f.trackedEntities == nil {
			f.trackedEntities = []Entity{entity}
		} else {
			f.trackedEntities = append(f.trackedEntities, entity)
		}
		f.trackedEntitiesCounter++
		if f.trackedEntitiesCounter == 10001 {
			panic(fmt.Errorf("track limit 10000 exceeded"))
		}
	}
	return f
}

func (f *flusher) Delete(entity ...Entity) Flusher {
	for _, e := range entity {
		e.markToDelete()
	}
	f.Track(entity...)
	return f
}

func (f *flusher) ForceDelete(entity ...Entity) Flusher {
	for _, e := range entity {
		e.forceMarkToDelete()
	}
	f.Track(entity...)
	return f
}

func (f *flusher) Flush() {
	f.flushTrackedEntities(false, false)
}

func (f *flusher) FlushWithCheck() error {
	return f.flushWithCheck(false)
}

func (f *flusher) FlushInTransactionWithCheck() error {
	return f.flushWithCheck(true)
}

func (f *flusher) FlushWithFullCheck() error {
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				f.Clear()
				asErr := r.(error)
				err = asErr
			}
		}()
		f.flushTrackedEntities(false, false)
	}()
	return err
}

func (f *flusher) FlushLazy() {
	f.flushTrackedEntities(true, false)
}

func (f *flusher) FlushInTransaction() {
	f.flushTrackedEntities(false, true)
}

func (f *flusher) Clear() {
	f.trackedEntities = nil
	f.trackedEntitiesCounter = 0
	f.clear()
}

func (f *flusher) flushTrackedEntities(lazy bool, transaction bool) {
	if f.trackedEntitiesCounter == 0 {
		return
	}
	var dbPools map[string]*DB
	if transaction {
		dbPools = make(map[string]*DB)
		for _, entity := range f.trackedEntities {
			db := entity.getORM().tableSchema.GetMysql(f.engine)
			dbPools[db.GetPoolConfig().GetCode()] = db
		}
		for _, db := range dbPools {
			db.Begin()
		}
	}
	defer func() {
		for _, db := range dbPools {
			db.Rollback()
		}
	}()
	f.flush(true, lazy, transaction, f.trackedEntities...)
	if transaction {
		for _, db := range dbPools {
			db.Commit()
		}
	}
	f.clear()
}

func (f *flusher) flushWithCheck(transaction bool) error {
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				f.Clear()
				asErr := r.(error)
				assErr1, is := asErr.(*ForeignKeyError)
				if is {
					err = assErr1
					return
				}
				assErr2, is := asErr.(*DuplicatedKeyError)
				if is {
					err = assErr2
					return
				}
				panic(asErr)
			}
		}()
		f.flushTrackedEntities(false, transaction)
	}()
	return err
}

func (f *flusher) flush(root bool, lazy bool, transaction bool, entities ...Entity) {
	var insertKeys map[reflect.Type][]string
	insertArguments := make(map[reflect.Type][]interface{})
	insertBinds := make(map[reflect.Type][]Bind)
	insertReflectValues := make(map[reflect.Type][]Entity)

	var referencesToFlash map[Entity]Entity

	for _, entity := range entities {
		initIfNeeded(f.engine.registry, entity)
		if entity.IsLazy() {
			panic(fmt.Errorf("lazy entity can't be flushed: %v [%d]", entity.getORM().elem.Type().String(), entity.GetID()))
		}
		schema := entity.getORM().tableSchema
		if !transaction && schema.GetMysql(f.engine).inTransaction {
			transaction = true
		}
		for _, refName := range schema.refOne {
			refValue := entity.getORM().elem.FieldByName(refName)
			if refValue.IsValid() && !refValue.IsNil() {
				refEntity := refValue.Interface().(Entity)
				initIfNeeded(f.engine.registry, refEntity)
				if refEntity.GetID() == 0 {
					if referencesToFlash == nil {
						referencesToFlash = make(map[Entity]Entity)
					}
					referencesToFlash[refEntity] = refEntity
				}
			}
		}
		for _, refName := range schema.refMany {
			refValue := entity.getORM().elem.FieldByName(refName)
			if refValue.IsValid() && !refValue.IsNil() {
				length := refValue.Len()
				for i := 0; i < length; i++ {
					refEntity := refValue.Index(i).Interface().(Entity)
					initIfNeeded(f.engine.registry, refEntity)
					if refEntity.GetID() == 0 {
						if referencesToFlash == nil {
							referencesToFlash = make(map[Entity]Entity)
						}
						referencesToFlash[refEntity] = refEntity
					}
				}
			}
		}
		if referencesToFlash != nil {
			continue
		}

		orm := entity.getORM()
		builder, isDirty := orm.getDirtyBind(f.engine)
		if !isDirty {
			continue
		}
		bindLength := len(builder.bind)

		t := orm.tableSchema.t
		currentID := entity.GetID()
		if orm.fakeDelete && !orm.tableSchema.hasFakeDelete {
			orm.delete = true
		}
		if orm.delete {
			if f.deleteBinds == nil {
				f.deleteBinds = make(map[reflect.Type]map[uint64]Entity)
			}
			if f.deleteBinds[t] == nil {
				f.deleteBinds[t] = make(map[uint64]Entity)
			}
			f.deleteBinds[t][currentID] = entity
		} else if !orm.inDB {
			onUpdate := entity.getORM().onDuplicateKeyUpdate
			if onUpdate != nil {
				if lazy {
					panic(fmt.Errorf("lazy flush on duplicate key is not supported"))
				}
				if currentID > 0 {
					builder.bind["ID"] = currentID
					bindLength++
				}
				values := make([]string, bindLength)
				columns := make([]string, bindLength)
				bindRow := make([]interface{}, bindLength)
				i := 0
				for key, val := range builder.bind {
					columns[i] = "`" + key + "`"
					values[i] = "?"
					bindRow[i] = val
					i++
				}
				/* #nosec */
				sql := "INSERT INTO " + schema.tableName + "(" + strings.Join(columns, ",") + ") VALUES (" + strings.Join(values, ",") + ")"
				sql += " ON DUPLICATE KEY UPDATE "
				first := true
				for k, v := range onUpdate {
					if !first {
						sql += ", "
					}
					sql += "`" + k + "` = ?"
					bindRow = append(bindRow, v)
					first = false
				}
				if len(onUpdate) == 0 {
					sql += "`Id` = `Id`"
				}
				db := schema.GetMysql(f.engine)
				result := db.Exec(sql, bindRow...)
				affected := result.RowsAffected()
				if affected > 0 {
					lastID := result.LastInsertId()
					orm.inDB = true
					orm.loaded = true
					orm := entity.getORM()
					orm.idElem.SetUint(lastID)
					orm.serialize(f.engine.getSerializer())
					if affected == 1 {
						f.updateCacheForInserted(entity, lazy, lastID, builder.bind)
					} else {
						for k, v := range onUpdate {
							err := entity.SetField(k, v)
							checkError(err)
						}
						bind, _ := orm.GetDirtyBind(f.engine)
						_, _ = loadByID(f.engine, lastID, entity, false, lazy)
						f.updateCacheAfterUpdate(entity, bind, builder.current, schema, lastID, false)
					}
				} else {
				OUTER:
					for _, index := range schema.uniqueIndices {
						fields := make([]string, 0)
						binds := make([]interface{}, 0)
						for _, column := range index {
							if builder.bind[column] == nil {
								continue OUTER
							}
							fields = append(fields, "`"+column+"` = ?")
							binds = append(binds, builder.bind[column])
						}
						findWhere := NewWhere(strings.Join(fields, " AND "), binds)
						f.engine.SearchOne(findWhere, entity)
						break
					}
				}
				continue
			}
			if currentID > 0 {
				builder.bind["ID"] = currentID
				bindLength++
			}
			if insertKeys == nil {
				insertKeys = make(map[reflect.Type][]string)
			}
			if insertKeys[t] == nil {
				fields := make([]string, bindLength)
				i := 0
				for key := range builder.bind {
					fields[i] = key
					i++
				}
				insertKeys[t] = fields
			}
			_, has := insertBinds[t]
			if !has {
				insertBinds[t] = make([]Bind, 0)
			}
			for _, key := range insertKeys[t] {
				insertArguments[t] = append(insertArguments[t], builder.bind[key])
			}
			insertReflectValues[t] = append(insertReflectValues[t], entity)
			insertBinds[t] = append(insertBinds[t], builder.bind)
		} else {
			if !entity.IsLoaded() {
				panic(fmt.Errorf("entity is not loaded and can't be updated: %v [%d]", entity.getORM().elem.Type().String(), currentID))
			}
			/* #nosec */
			sql := "UPDATE " + schema.GetTableName() + " SET "
			first := true
			for key, value := range builder.updateBind {
				if !first {
					sql += ","
				}
				first = false
				sql += "`" + key + "`=" + value
			}
			sql += " WHERE `ID` = " + strconv.FormatUint(currentID, 10)
			db := schema.GetMysql(f.engine)
			if lazy {
				var logEvents []*LogQueueValue
				var dirtyEvents []*dirtyQueueValue
				serializer := f.engine.getSerializer()
				serializer.buffer.Reset()
				entity.getORM().serialize(serializer)
				logEvent, dirtyEvent := f.updateCacheAfterUpdate(entity, builder.bind, builder.current, schema, currentID, true)
				if logEvent != nil {
					logEvents = append(logEvents, logEvent)
				}
				if dirtyEvent != nil {
					dirtyEvents = append(dirtyEvents, dirtyEvent)
				}
				f.fillLazyQuery(db.GetPoolConfig().GetCode(), sql, nil, logEvents, dirtyEvents)
			} else {
				if f.updateSQLs == nil {
					f.updateSQLs = make(map[string][]string)
				}
				f.updateSQLs[schema.mysqlPoolName] = append(f.updateSQLs[schema.mysqlPoolName], sql)
				serializer := f.engine.getSerializer()
				serializer.buffer.Reset()
				entity.getORM().serialize(serializer)
				f.updateCacheAfterUpdate(entity, builder.bind, builder.current, schema, currentID, false)
			}
		}
	}

	if referencesToFlash != nil {
		if lazy {
			panic(fmt.Errorf("lazy flush for unsaved references is not supported"))
		}
		toFlush := make([]Entity, len(referencesToFlash))
		i := 0
		for _, v := range referencesToFlash {
			toFlush[i] = v
			i++
		}
		f.flush(false, false, transaction, toFlush...)
		rest := make([]Entity, 0)
		for _, v := range entities {
			_, has := referencesToFlash[v]
			if !has {
				rest = append(rest, v)
			}
		}
		if len(rest) > 0 {
			f.flush(true, false, transaction, rest...)
		}
		return
	}
	for typeOf, values := range insertKeys {
		schema := getTableSchema(f.engine.registry, typeOf)
		/* #nosec */
		sql := "INSERT INTO " + schema.tableName
		l := len(values)
		if l > 0 {
			sql += "("
		}
		first := true
		for _, val := range values {
			if !first {
				sql += ","
			}
			first = false
			sql += "`" + val + "`"
		}
		if l > 0 {
			sql += ")"
		}
		sql += " VALUES "
		bindPart := "("
		if l > 0 {
			bindPart += "?"
		}
		for i := 1; i < l; i++ {
			bindPart += ",?"
		}
		bindPart += ")"
		l = len(insertBinds[typeOf])
		for i := 0; i < l; i++ {
			if i > 0 {
				sql += ","
			}
			sql += bindPart
		}
		db := schema.GetMysql(f.engine)
		if lazy {
			var logEvents []*LogQueueValue
			var dirtyEvents []*dirtyQueueValue
			for key, entity := range insertReflectValues[typeOf] {
				logEvent, dirtyEvent := f.updateCacheForInserted(entity, lazy, 0, insertBinds[typeOf][key])
				if logEvent != nil {
					logEvents = append(logEvents, logEvent)
				}
				if dirtyEvent != nil {
					dirtyEvents = append(dirtyEvents, dirtyEvent)
				}
			}
			f.fillLazyQuery(db.GetPoolConfig().GetCode(), sql, insertArguments[typeOf], logEvents, dirtyEvents)
		} else {
			res := db.Exec(sql, insertArguments[typeOf]...)
			id := res.LastInsertId()
			for key, entity := range insertReflectValues[typeOf] {
				bind := insertBinds[typeOf][key]
				insertedID := entity.GetID()
				orm := entity.getORM()
				orm.inDB = true
				orm.loaded = true
				if insertedID == 0 {
					orm.idElem.SetUint(id)
					insertedID = id
					id = id + db.GetPoolConfig().getAutoincrement()
				}
				orm.serialize(f.engine.getSerializer())
				f.updateCacheForInserted(entity, lazy, insertedID, bind)
			}
		}
	}
	if root {
		for pool, queries := range f.updateSQLs {
			db := f.engine.GetMysql(pool)
			l := len(queries)
			if l == 1 {
				db.Exec(queries[0])
				continue
			}
			forcedTransaction := l >= 3 && !db.inTransaction
			func() {
				if forcedTransaction {
					db.Begin()
					defer db.Rollback()
				}
				_, def := db.Query(strings.Join(queries, ";") + ";")
				defer def()
				if forcedTransaction {
					db.Commit()
				}
			}()
		}
		for typeOf, deleteBinds := range f.deleteBinds {
			schema := getTableSchema(f.engine.registry, typeOf)
			ids := make([]interface{}, len(deleteBinds))
			var logEvents []*LogQueueValue
			var dirtyEvents []*dirtyQueueValue
			i := 0
			for id := range deleteBinds {
				ids[i] = id
				i++
			}
			/* #nosec */
			sql := "DELETE FROM `" + schema.tableName + "` WHERE " + NewWhere("`ID` IN ?", ids).String()
			db := schema.GetMysql(f.engine)
			if !lazy {
				usage := schema.GetUsage(f.engine.registry)
				if len(usage) > 0 {
					for refT, refColumns := range usage {
						for _, refColumn := range refColumns {
							refSchema := getTableSchema(f.engine.registry, refT)
							_, isCascade := refSchema.tags[refColumn]["cascade"]
							if isCascade {
								subValue := reflect.New(reflect.SliceOf(reflect.PtrTo(refT)))
								subElem := subValue.Elem()
								sub := subValue.Interface()
								pager := NewPager(1, 1000)
								where := NewWhere("`"+refColumn+"` IN ?", ids)
								for {
									f.engine.Search(where, pager, sub)
									total := subElem.Len()
									if total == 0 {
										break
									}
									toDeleteAll := make([]Entity, total)
									for i := 0; i < total; i++ {
										toDeleteValue := subElem.Index(i).Interface().(Entity)
										toDeleteValue.markToDelete()
										toDeleteAll[i] = toDeleteValue
									}
									f.flush(true, transaction, lazy, toDeleteAll...)
								}
							}
						}
					}
				}
				_ = db.Exec(sql, ids...)
			}

			localCache, hasLocalCache := schema.GetLocalCache(f.engine)
			redisCache, hasRedis := schema.GetRedisCache(f.engine)
			if !hasLocalCache && f.engine.hasRequestCache {
				hasLocalCache = true
				localCache = f.engine.GetLocalCache(requestCacheKey)
			}
			for id, entity := range deleteBinds {
				orm := entity.getORM()
				builder, _ := orm.getDirtyBind(f.engine)
				if !lazy {
					f.addDirtyQueues(builder.current, schema, id, "d", lazy)
					f.addToLogQueue(schema, id, builder.current, nil, entity.getORM().logMeta, lazy)
				} else {
					logEvent := f.addToLogQueue(schema, id, builder.current, nil, orm.logMeta, lazy)
					if logEvent != nil {
						logEvents = append(logEvents, logEvent)
					}
					dirtyEvent := f.addDirtyQueues(builder.current, schema, id, "d", lazy)
					if dirtyEvent != nil {
						dirtyEvents = append(dirtyEvents, dirtyEvent)
					}
				}
				if hasLocalCache || hasRedis {
					cacheKey := schema.getCacheKey(id)
					keys := f.getCacheQueriesKeys(schema, builder.bind, builder.current, true, true)
					if hasLocalCache {
						f.addLocalCacheSet(localCache.config.GetCode(), cacheKey, cacheNilValue)
						f.addLocalCacheDeletes(localCache.config.GetCode(), keys...)
					}
					if hasRedis {
						f.getRedisFlusher().Del(redisCache.config.GetCode(), cacheKey)
						f.getRedisFlusher().Del(redisCache.config.GetCode(), keys...)
					}
				}
				if schema.hasSearchCache {
					key := schema.redisSearchPrefix + strconv.FormatUint(id, 10)
					f.getRedisFlusher().Del(schema.searchCacheName, key)
				}
			}
			if lazy {
				f.fillLazyQuery(db.GetPoolConfig().GetCode(), sql, ids, logEvents, dirtyEvents)
			}
		}
		if f.localCacheDeletes != nil {
			if lazy {
				lazyMap := f.getLazyMap()
				lazyMap["cl"] = f.localCacheDeletes
			} else {
				for cacheCode, allKeys := range f.localCacheDeletes {
					f.engine.GetLocalCache(cacheCode).Remove(allKeys...)
				}
			}
		}
		for cacheCode, keys := range f.localCacheSets {
			cache := f.engine.GetLocalCache(cacheCode)
			if !transaction {
				cache.MSet(keys...)
			} else {
				if f.engine.afterCommitLocalCacheSets == nil {
					f.engine.afterCommitLocalCacheSets = make(map[string][]interface{})
				}
				f.engine.afterCommitLocalCacheSets[cacheCode] = append(f.engine.afterCommitLocalCacheSets[cacheCode], keys...)
			}
		}
	}
	if lazy {
		lazyMap := f.getLazyMap()
		deletesRedisCache, has := lazyMap["cr"].(map[string][]string)
		if !has {
			deletesRedisCache = make(map[string][]string)
			lazyMap["cr"] = deletesRedisCache
		}
		for cacheCode, commands := range f.getRedisFlusher().pipelines {
			if commands.deletes != nil {
				deletesRedisCache[cacheCode] = commands.deletes
			}
		}
	} else if transaction {
		f.engine.afterCommitRedisFlusher = f.getRedisFlusher()
	}
	if len(f.lazyMap) > 0 {
		f.getRedisFlusher().Publish(lazyChannelName, f.lazyMap)
		f.lazyMap = nil
	}
	if f.redisFlusher != nil && !transaction && root {
		f.redisFlusher.Flush()
	}
}

func (f *flusher) updateCacheForInserted(entity Entity, lazy bool, id uint64, bind Bind) (*LogQueueValue, *dirtyQueueValue) {
	schema := entity.getORM().tableSchema
	localCache, hasLocalCache := schema.GetLocalCache(f.engine)
	if !hasLocalCache && f.engine.hasRequestCache {
		hasLocalCache = true
		localCache = f.engine.GetLocalCache(requestCacheKey)
	}
	redisCache, hasRedis := schema.GetRedisCache(f.engine)
	if hasLocalCache || hasRedis {
		cacheKey := schema.getCacheKey(id)
		keys := f.getCacheQueriesKeys(schema, bind, nil, false, true)
		if hasLocalCache {
			if !lazy {
				f.addLocalCacheSet(localCache.config.GetCode(), cacheKey, entity.getORM().copyBinary())
			} else {
				f.addLocalCacheDeletes(localCache.config.GetCode(), schema.getCacheKey(id))
			}
			f.addLocalCacheDeletes(localCache.config.GetCode(), keys...)
		}
		if hasRedis {
			f.getRedisFlusher().Del(redisCache.config.GetCode(), cacheKey)
			f.getRedisFlusher().Del(redisCache.config.GetCode(), keys...)
		}
	}
	f.fillRedisSearchFromBind(schema, bind, id)
	return f.addToLogQueue(schema, id, nil, bind, entity.getORM().logMeta, lazy), f.addDirtyQueues(bind, schema, id, "i", lazy)
}

func (f *flusher) getRedisFlusher() *redisFlusher {
	if f.redisFlusher == nil {
		f.redisFlusher = f.engine.afterCommitRedisFlusher
		if f.redisFlusher == nil {
			f.redisFlusher = &redisFlusher{engine: f.engine}
		}
	}
	return f.redisFlusher
}

func (f *flusher) getLazyMap() map[string]interface{} {
	if f.lazyMap == nil {
		f.lazyMap = make(map[string]interface{})
	}
	return f.lazyMap
}

func (f *flusher) updateCacheAfterUpdate(entity Entity, bind, current Bind, schema *tableSchema, currentID uint64, lazy bool) (*LogQueueValue, *dirtyQueueValue) {
	localCache, hasLocalCache := schema.GetLocalCache(f.engine)
	redisCache, hasRedis := schema.GetRedisCache(f.engine)
	if !hasLocalCache && f.engine.hasRequestCache {
		hasLocalCache = true
		localCache = f.engine.GetLocalCache(requestCacheKey)
	}
	if hasLocalCache || hasRedis {
		cacheKey := schema.getCacheKey(currentID)
		keysOld := f.getCacheQueriesKeys(schema, bind, current, true, false)
		keysNew := f.getCacheQueriesKeys(schema, bind, current, false, false)
		if hasLocalCache {
			f.addLocalCacheSet(localCache.config.GetCode(), cacheKey, entity.getORM().copyBinary())
			f.addLocalCacheDeletes(localCache.config.GetCode(), keysOld...)
			f.addLocalCacheDeletes(localCache.config.GetCode(), keysNew...)
		}
		if hasRedis {
			redisFlusher := f.getRedisFlusher()
			redisFlusher.Del(redisCache.config.GetCode(), cacheKey)
			redisFlusher.Del(redisCache.config.GetCode(), keysOld...)
			redisFlusher.Del(redisCache.config.GetCode(), keysNew...)
		}
	}
	f.fillRedisSearchFromBind(schema, bind, entity.GetID())
	dirtyValue := f.addDirtyQueues(bind, schema, currentID, "u", lazy)
	if schema.hasLog {
		return f.addToLogQueue(schema, currentID, current, bind, entity.getORM().logMeta, lazy), dirtyValue
	}
	return nil, dirtyValue
}

func (f *flusher) addDirtyQueues(bind Bind, schema *tableSchema, id uint64, action string, lazy bool) *dirtyQueueValue {
	var key *dirtyEvent
	var allStreams []string
	for stream, columns := range schema.dirtyFields {
		for _, column := range columns {
			isDirty := column == "ORM"
			if !isDirty {
				_, isDirty = bind[column]
			}
			if !isDirty {
				continue
			}
			if key == nil {
				key = &dirtyEvent{A: action, E: schema.t.String(), I: id}
			}
			if !lazy {
				f.getRedisFlusher().Publish(stream, key)
			} else {
				allStreams = append(allStreams, stream)
			}
			break
		}
	}
	if !lazy || key == nil {
		return nil
	}
	return &dirtyQueueValue{Event: key, Streams: allStreams}
}

func (f *flusher) addToLogQueue(tableSchema *tableSchema, id uint64, before, changes, entityMeta Bind, lazy bool) *LogQueueValue {
	if !tableSchema.hasLog {
		return nil
	}
	if changes != nil && len(tableSchema.skipLogs) > 0 {
		skipped := 0
		for _, skip := range tableSchema.skipLogs {
			_, has := changes[skip]
			if has {
				skipped++
			}
		}
		if skipped == len(changes) {
			return nil
		}
	}
	val := &LogQueueValue{TableName: tableSchema.logTableName, ID: id,
		PoolName: tableSchema.logPoolName, Before: before,
		Changes: changes, Updated: time.Now(), Meta: entityMeta}
	if val.Meta == nil {
		val.Meta = f.engine.logMetaData
	} else {
		for k, v := range f.engine.logMetaData {
			val.Meta[k] = v
		}
	}
	if !lazy {
		f.getRedisFlusher().Publish(logChannelName, val)
	}
	return val
}

func (f *flusher) fillRedisSearchFromBind(schema *tableSchema, bind Bind, id uint64) {
	if schema.hasSearchCache {
		if schema.hasFakeDelete {
			val, has := bind["FakeDelete"]
			if has && val.(uint64) > 0 {
				f.getRedisFlusher().Del(schema.searchCacheName, schema.redisSearchPrefix+strconv.FormatUint(id, 10))
			}
		}
		values := make([]interface{}, 0)
		idMap, has := schema.mapBindToRedisSearch["ID"]
		if has {
			values = append(values, "ID", idMap(id))
		}
		hasChangedField := false
		for k, f := range schema.mapBindToRedisSearch {
			v, has := bind[k]
			if has {
				values = append(values, k, f(v))
				hasChangedField = true
			}
		}
		if hasChangedField {
			f.getRedisFlusher().HSet(schema.searchCacheName, schema.redisSearchPrefix+strconv.FormatUint(id, 10), values...)
		}
	}
}

func (f *flusher) getCacheQueriesKeys(schema *tableSchema, bind, current Bind, old, addedDeleted bool) (keys []string) {
	keys = make([]string, 0)
	for indexName, definition := range schema.cachedIndexesAll {
		if !addedDeleted && schema.hasFakeDelete {
			_, addedDeleted = bind["FakeDelete"]
		}
		if addedDeleted && len(definition.TrackedFields) == 0 {
			keys = append(keys, getCacheKeySearch(schema, indexName))
		}
		for _, trackedField := range definition.TrackedFields {
			_, has := bind[trackedField]
			if has || addedDeleted {
				attributes := make([]interface{}, 0)
				for _, trackedFieldSub := range definition.QueryFields {
					val, has := bind[trackedFieldSub]
					if !has || old {
						val = current[trackedFieldSub]
					}
					if !schema.hasFakeDelete || trackedFieldSub != "FakeDelete" {
						attributes = append(attributes, val)
					}
				}
				keys = append(keys, getCacheKeySearch(schema, indexName, attributes...))
				break
			}
		}
	}
	return
}

func (f *flusher) addLocalCacheSet(cacheCode string, keys ...interface{}) {
	if f.localCacheSets == nil {
		f.localCacheSets = make(map[string][]interface{})
	}
	f.localCacheSets[cacheCode] = append(f.localCacheSets[cacheCode], keys...)
}

func (f *flusher) addLocalCacheDeletes(cacheCode string, keys ...string) {
	if len(keys) == 0 {
		return
	}
	if f.localCacheDeletes == nil {
		f.localCacheDeletes = make(map[string][]string)
	}
	f.localCacheDeletes[cacheCode] = append(f.localCacheDeletes[cacheCode], keys...)
}

func (f *flusher) fillLazyQuery(dbCode string, sql string, values []interface{}, logEvent []*LogQueueValue, dirtyData []*dirtyQueueValue) {
	lazyMap := f.getLazyMap()
	updatesMap := lazyMap["q"]
	if updatesMap == nil {
		updatesMap = make([]interface{}, 0)
		lazyMap["q"] = updatesMap
	}
	lazyValue := make([]interface{}, 3)
	lazyValue[0] = dbCode
	lazyValue[1] = sql
	lazyValue[2] = values
	lazyMap["q"] = append(updatesMap.([]interface{}), lazyValue)
	if len(logEvent) > 0 {
		lazyMap["l"] = logEvent
	}
	if len(dirtyData) > 0 {
		lazyMap["d"] = dirtyData
	}
}

func (f *flusher) clear() {
	f.updateSQLs = nil
	f.deleteBinds = nil
	f.localCacheDeletes = nil
	f.localCacheSets = nil
}
