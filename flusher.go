package beeorm

import (
	"fmt"
	"strconv"
	"strings"
)

type Bind map[string]string

type DuplicatedKeyError struct {
	Message string
	Index   string
}

func (err *DuplicatedKeyError) Error() string {
	return err.Message
}

type FlushType int

const (
	Insert FlushType = iota
	InsertUpdate
	Update
	Delete
)

type ForeignKeyError struct {
	Message    string
	Constraint string
}

func (err *ForeignKeyError) Error() string {
	return err.Message
}

type DataFlusher struct {
	Events map[string][]*EntityCacheFlushData
	Parent *flusher
}

func (df *DataFlusher) AddEvent(flushData *EntityCacheFlushData) {
	if df.Events == nil {
		df.Events = map[string][]*EntityCacheFlushData{flushData.EntityName: {flushData}}
	} else {
		df.Events[flushData.EntityName] = append(df.Events[flushData.EntityName], flushData)
	}
}

func (df *DataFlusher) Execute(engine Engine, lazy bool) {
	//TODO
}

type EntityCacheFlushData struct {
	*EntitySQLFlushData
	RedisDeletes      map[string][]string
	LocalCacheDeletes map[string][]string
}

func (el *EntityCacheFlushData) PublishToStream(stream string, event interface{}) {
	//TODO
}

func (el *EntityCacheFlushData) DeleteInRedis(pool string, key ...string) {
	if len(key) > 0 {
		deletes, has := el.RedisDeletes[pool]
		if !has {
			el.RedisDeletes[pool] = key
		} else {
			el.RedisDeletes[pool] = append(deletes, key...)
		}
	}

}

func (el *EntityCacheFlushData) DeleteInLocalCache(pool string, key ...string) {
	if len(key) > 0 {
		deletes, has := el.LocalCacheDeletes[pool]
		if !has {
			el.LocalCacheDeletes[pool] = key
		} else {
			el.LocalCacheDeletes[pool] = append(deletes, key...)
		}
	}
}

func (el *EntityCacheFlushData) AddInLocalCache(pool, key string, value interface{}) {
	// TODO
}

type Flusher interface {
	Track(entity ...Entity) Flusher
	Flush()
	FlushWithCheck() error
	FlushWithFullCheck() error
	FlushLazy()
	Clear()
	Delete(entity ...Entity) Flusher
	ForceDelete(entity ...Entity) Flusher
}

type flusher struct {
	engine                 *engineImplementation
	trackedEntities        []Entity
	trackedEntitiesCounter int
	serializer             *serializer
}

func (f *flusher) Track(entity ...Entity) Flusher {
main:
	for _, e := range entity {
		initIfNeeded(f.engine.registry, e)
		if f.trackedEntities == nil {
			f.trackedEntities = []Entity{e}
		} else {
			for _, old := range f.trackedEntities {
				if old == e {
					continue main
				}
			}
			f.trackedEntities = append(f.trackedEntities, e)
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
	f.flushTrackedEntities(false)
}

func (f *flusher) FlushWithCheck() error {
	return f.flushWithCheck()
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
		f.flushTrackedEntities(false)
	}()
	return err
}

func (f *flusher) FlushLazy() {
	f.flushTrackedEntities(true)
}

func (f *flusher) Clear() {
	f.trackedEntities = nil
	f.trackedEntitiesCounter = 0
}

func (f *flusher) flushTrackedEntities(lazy bool) {
	if f.trackedEntitiesCounter == 0 {
		return
	}
	dataFlusher := f.buildDataFlasher()
	dataFlusher.Execute(f.engine, lazy)
}

func (f *flusher) flushWithCheck() error {
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
		f.flushTrackedEntities(false)
	}()
	return err
}

func (f *flusher) getSerializer() *serializer {
	if f.serializer == nil {
		f.serializer = newSerializer(nil)
	}
	return f.serializer
}

func (f *flusher) buildDataFlasher() *DataFlusher {
	dataFlusher := &DataFlusher{}
	for _, entity := range f.trackedEntities {
		initIfNeeded(f.engine.registry, entity)
		f.buildReferences(entity, dataFlusher)

		orm := entity.getORM()
		entitySQLFlushData, isDirty := orm.buildDirtyBind(f.getSerializer())
		if !isDirty {
			continue
		}
		entityCacheFlushData := &EntityCacheFlushData{EntitySQLFlushData: entitySQLFlushData}

		currentID := entity.GetID()
		if orm.fakeDelete && !orm.tableSchema.hasFakeDelete {
			orm.delete = true
		}
		if orm.delete {
			f.fillCacheFlushDataForDelete(entity, entityCacheFlushData)
		} else if !orm.inDB {
			if currentID == 0 && orm.tableSchema.hasUUID {
				currentID = uuid()
				orm.idElem.SetUint(currentID)
			}
			if currentID > 0 {
				entityCacheFlushData.EntitySQLFlushData.Update["ID"] = strconv.FormatUint(currentID, 10)
			}
		} else {
			f.fillCacheFlushDataForUpdate(entity, entityCacheFlushData)
		}
		dataFlusher.AddEvent(entityCacheFlushData)
	}
}

func (f *flusher) updateRedisCache(root bool, lazy bool, transaction bool) {
	if lazy {
		lazyMap := f.getLazyMap()
		deletesRedisCache, has := lazyMap["cr"].(map[string][]string)
		for cacheCode, commands := range f.getRedisFlusher().pipelines {
			if commands.deletes != nil {
				if !has {
					deletesRedisCache = make(map[string][]string)
					lazyMap["cr"] = deletesRedisCache
					has = true
				}
				deletesRedisCache[cacheCode] = commands.deletes
			}
		}
		if transaction {
			f.engine.afterCommitRedisFlusher = f.getRedisFlusher()
		}
	} else if transaction {
		f.engine.afterCommitRedisFlusher = f.getRedisFlusher()
	}
	for _, lazyEvent := range f.lazyEvents {
		f.getRedisFlusher().Publish(LazyChannelName, lazyEvent)
	}
	if f.redisFlusher != nil && !transaction && root {
		f.redisFlusher.Flush()
	}
}

func (f *flusher) updateLocalCache(lazy bool, transaction bool) {
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

func (f *flusher) executeUpdates() {
	for pool, queries := range f.updateSQLs {
		db := f.engine.GetMysql(pool)
		l := len(queries)
		if l == 1 {
			db.Exec(queries[0])
			continue
		}
		_, def := db.Query(strings.Join(queries, ";") + ";")
		def()
	}
}

func (f *flusher) executeInserts(flushPackage *flushPackage, lazy bool) {
	for typeOf, values := range flushPackage.insertKeys {
		schema := getTableSchema(f.engine.registry, typeOf)
		f.stringBuilder.WriteString("INSERT INTO `")
		f.stringBuilder.WriteString(schema.tableName)
		f.stringBuilder.WriteString("`")
		l := len(values)
		if l > 0 {
			f.stringBuilder.WriteString("(")
		}
		first := true
		for _, val := range values {
			if !first {
				f.stringBuilder.WriteString(",")
			}
			first = false
			f.stringBuilder.WriteString("`" + val + "`")
		}
		if l > 0 {
			f.stringBuilder.WriteString(")")
		}
		f.stringBuilder.WriteString(" VALUES ")
		for i, row := range flushPackage.insertSQLBinds[typeOf] {
			if i > 0 {
				f.stringBuilder.WriteString(",")
			}
			f.stringBuilder.WriteString("(")
			for j, val := range values {
				if j > 0 {
					f.stringBuilder.WriteString(",")
				}
				f.stringBuilder.WriteString(row[val])
			}
			f.stringBuilder.WriteString(")")
		}
		sql := f.stringBuilder.String()
		f.stringBuilder.Reset()
		db := schema.GetMysql(f.engine)
		if lazy {
			for key, entity := range flushPackage.insertReflectValues[typeOf] {
				if schema.hasUUID {
					entity.getORM().serialize(f.getSerializer())
				}
				f.updateCacheForInserted(entity, lazy, entity.GetID(), flushPackage.insertBinds[typeOf][key])
			}
			f.fillLazyQuery(db.GetPoolConfig().GetCode(), sql, true, 0)
		} else {
			res := db.Exec(sql)
			id := res.LastInsertId()
			for key, entity := range flushPackage.insertReflectValues[typeOf] {
				bind := flushPackage.insertBinds[typeOf][key]
				insertedID := entity.GetID()
				orm := entity.getORM()
				orm.inDB = true
				orm.loaded = true
				if insertedID == 0 {
					orm.idElem.SetUint(id)
					insertedID = id
					id = id + db.GetPoolConfig().getAutoincrement()
				}
				orm.serialize(f.getSerializer())
				f.updateCacheForInserted(entity, lazy, insertedID, bind)
			}
		}
	}
}

func (f *flusher) fillCacheFlushDataForUpdate(entity Entity, entityFlushData *EntityCacheFlushData) {
	if !entity.IsLoaded() {
		panic(fmt.Errorf("entity is not loaded and can't be updated: %v [%d]", entity.getORM().elem.Type().String(), entity.GetID()))
	}
	schema := entity.getORM().tableSchema
	localCache, hasLocalCache := schema.GetLocalCache(f.engine)
	redisCache, hasRedis := schema.GetRedisCache(f.engine)
	if !hasLocalCache && f.engine.hasRequestCache {
		hasLocalCache = true
		localCache = f.engine.GetLocalCache(requestCacheKey)
	}
	if hasLocalCache || hasRedis {
		cacheKey := schema.getCacheKey(entity.GetID())
		keysOld := f.getCacheQueriesKeys(schema, entityFlushData.Update, entityFlushData.Old, true, false)
		keysNew := f.getCacheQueriesKeys(schema, entityFlushData.Update, entityFlushData.Old, false, false)
		if hasLocalCache {
			entityFlushData.AddInLocalCache(localCache.config.GetCode(), cacheKey, entity.getORM().copyBinary())
			entityFlushData.DeleteInLocalCache(localCache.config.GetCode(), keysOld...)
			entityFlushData.DeleteInLocalCache(localCache.config.GetCode(), keysNew...)
		}
		if hasRedis {

			entityFlushData.DeleteInRedis(redisCache.config.GetCode(), cacheKey)
			entityFlushData.DeleteInRedis(redisCache.config.GetCode(), keysOld...)
			entityFlushData.DeleteInRedis(redisCache.config.GetCode(), keysNew...)
		}
	}
}

func (f *flusher) fillCacheFlushDataForDelete(entity Entity, entityFlushData *EntityCacheFlushData) {
	schema := entity.getORM().tableSchema
	localCache, hasLocalCache := schema.GetLocalCache(f.engine)
	redisCache, hasRedis := schema.GetRedisCache(f.engine)
	if !hasLocalCache && f.engine.hasRequestCache {
		hasLocalCache = true
		localCache = f.engine.GetLocalCache(requestCacheKey)
	}

	if hasLocalCache || hasRedis {
		cacheKey := schema.getCacheKey(entity.GetID())
		keys := f.getCacheQueriesKeys(schema, entityFlushData.Update, entityFlushData.Old, true, true)
		if hasLocalCache {
			entityFlushData.AddInLocalCache(localCache.config.GetCode(), cacheKey, cacheNilValue)
			entityFlushData.DeleteInLocalCache(localCache.config.GetCode(), keys...)
		}
		if hasRedis {
			entityFlushData.DeleteInRedis(redisCache.config.GetCode(), cacheKey)
			entityFlushData.DeleteInRedis(redisCache.config.GetCode(), keys...)
		}
	}
}

func (f *flusher) buildReferences(entity Entity, dataFlusher *DataFlusher) {
	for _, refName := range entity.getORM().tableSchema.refOne {
		refValue := entity.getORM().elem.FieldByName(refName)
		if refValue.IsValid() && !refValue.IsNil() {
			refEntity := refValue.Interface().(Entity)
			initIfNeeded(f.engine.registry, refEntity)
			if refEntity.GetID() == 0 {
				if dataFlusher.Parent == nil {
					dataFlusher.Parent = &flusher{engine: f.engine}
				}
				dataFlusher.Parent.Track(refEntity)
			}
		}
	}
}

func (f *flusher) updateCacheForInserted(entity Entity, lazy bool, id uint64, bind Bind) {
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
			if !lazy || schema.hasUUID {
				f.addLocalCacheSet(localCache.config.GetCode(), cacheKey, entity.getORM().copyBinary())
			} else {
				f.addLocalCacheDeletes(localCache.config.GetCode(), schema.getCacheKey(id))
			}
			f.addLocalCacheDeletes(localCache.config.GetCode(), keys...)
		}
		if hasRedis {
			if schema.hasUUID {
				f.getRedisFlusher().Set(redisCache.config.GetCode(), cacheKey, entity.getORM().binary)
			} else {
				f.getRedisFlusher().Del(redisCache.config.GetCode(), cacheKey)
			}
			f.getRedisFlusher().Del(redisCache.config.GetCode(), keys...)
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

func (f *flusher) fillLazyQuery(dbCode string, sql string, insert bool, id uint64) {
	lazyMap := f.getLazyMap()
	updatesMap := lazyMap["q"]
	idsMap := lazyMap["i"]
	if updatesMap == nil {
		updatesMap = make([]interface{}, 0)
		lazyMap["q"] = updatesMap
		idsMap = make([]interface{}, 0)
		lazyMap["i"] = updatesMap
	}
	lazyValue := make([]interface{}, 3)
	lazyValue[0] = dbCode
	lazyValue[1] = sql
	lazyMap["q"] = append(updatesMap.([]interface{}), lazyValue)
	lazyMap["i"] = append(idsMap.([]interface{}), id)
	lazyMap["o"] = "i"
	if !insert {
		lazyMap["o"] = "u"
	}
}
