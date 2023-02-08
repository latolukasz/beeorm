package beeorm

import (
	"fmt"
	"strconv"
	"strings"
)

type Bind map[string]string

func (m Bind) Get(key string) string {
	return m[key]
}

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
	Update
	Delete
	insertUpdate
)

func (ft FlushType) String() string {
	switch ft {
	case Insert:
		return "INSERT"
	case insertUpdate:
		return "INSERT ON DUPLICATE KEY UPDATE"
	case Update:
		return "UPDATE"
	case Delete:
		return "DELETE"
	}
	return ""
}

type ForeignKeyError struct {
	Message    string
	Constraint string
}

func (err *ForeignKeyError) Error() string {
	return err.Message
}

type FlusherCacheSetter interface {
	GetLocalCacheSetter(code ...string) LocalCacheSetter
	GetRedisCacheSetter(code ...string) RedisCacheSetter
	PublishToStream(stream string, body interface{}, meta Bind)
}

type Flusher interface {
	FlusherCacheSetter
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
	trackedEntities        map[uintptr]Entity
	trackedEntitiesCounter int
	serializer             *serializer
	events                 []*entitySQLFlush
	stringBuilder          strings.Builder
	localCacheSetters      map[string]*localCacheSetter
	redisCacheSetters      map[string]*redisCacheSetter
}

func (f *flusher) addFlushEvent(sqlFlush *entitySQLFlush) {
	f.events = append(f.events, sqlFlush)
}

func (f *flusher) execute(lazy, fromLazyConsumer bool) {
	if len(f.events) == 0 {
		return
	}

	for _, plugin := range f.engine.registry.plugins {
		interfaceEntityFlushing, isInterfaceEntityFlushing := plugin.(PluginInterfaceEntityFlushing)
		if isInterfaceEntityFlushing {
			for _, e := range f.events {
				interfaceEntityFlushing.PluginInterfaceEntityFlushing(f.engine, e)
			}
		}
	}

	if lazy {
		f.buildCache(true, false)
		for _, cache := range f.localCacheSetters {
			cache.flush()
		}
		f.localCacheSetters = nil
		f.engine.GetEventBroker().Publish(LazyFlushChannelName, f.events, nil)
		for _, e := range f.events {
			if e.ID == 0 && e.entity != nil {
				e.entity.getORM().lazy = true
			}
		}
		f.events = nil
		return
	}
	checkReferences := true
	startTransaction := make(map[*DB]bool)
	func() {
		defer func() {
			for db := range startTransaction {
				if db.inTransaction {
					db.Rollback()
				}
			}
		}()
		for checkReferences {
			checkReferences = false
			group := make(map[*DB]map[string]map[FlushType][]*entitySQLFlush)
			for _, e := range f.events {
				if e.flushed {
					continue
				}
				if len(e.References) > 0 {
					checkReferences = true
				} else {
					schema := f.engine.registry.GetEntitySchema(e.Entity)
					db := schema.GetMysql(f.engine)
					byDB, hasDB := group[db]
					if !hasDB {
						byDB = make(map[string]map[FlushType][]*entitySQLFlush)
						group[db] = byDB
					}
					byTable, hasTable := byDB[schema.GetTableName()]
					if !hasTable {
						byTable = make(map[FlushType][]*entitySQLFlush)
						byDB[schema.GetTableName()] = byTable
					}
					byTable[e.Action] = append(byTable[e.Action], e)
				}
			}
		MAIN:
			for db, byDB := range group {
				if !db.IsInTransaction() {
					if len(byDB) > 1 || checkReferences {
						startTransaction[db] = true
						continue
					}
					for _, byAction := range byDB {
						if len(byAction) > 1 || len(byAction[Update]) > 1 {
							startTransaction[db] = true
							continue MAIN
						}
					}
				}
			}
			for db := range startTransaction {
				if !db.inTransaction {
					db.Begin()
				}
			}
			for db, byDB := range group {
				for tableName, byAction := range byDB {
					deleteEvents, hasDeletes := byAction[Delete]
					if hasDeletes {
						f.executeDeletes(db, tableName, deleteEvents)
					}
					insertEvents, hasInserts := byAction[Insert]
					if hasInserts {
						f.executeInserts(db, tableName, insertEvents)
						if checkReferences {
							for _, e := range f.events {
								for column, address := range e.References {
									for _, inserted := range insertEvents {
										if inserted.TempID == address {
											e.Update[column] = strconv.FormatUint(inserted.ID, 10)
											delete(e.References, column)
										}
									}
								}
							}
						}
					}
					insertUpdateEvents, hasInsertUpdates := byAction[insertUpdate]
					if hasInsertUpdates {
						f.executeInsertOnDuplicateKeyUpdates(db, tableName, insertUpdateEvents)
					}
					updateEvents, hasUpdates := byAction[Update]
					if hasUpdates {
						f.executeUpdates(db, tableName, updateEvents)
					}
				}
			}
			if !checkReferences {
				for db := range startTransaction {
					db.Commit()
				}
			}
		}
	}()
	f.buildCache(false, fromLazyConsumer)
	f.events = nil
}

func (f *flusher) flushCacheSetters() {
	for _, cache := range f.localCacheSetters {
		cache.flush()
	}
	f.localCacheSetters = nil
	for _, cache := range f.redisCacheSetters {
		cache.flush()
	}
	f.redisCacheSetters = nil
}

func (f *flusher) executeInserts(db *DB, table string, events []*entitySQLFlush) {
	f.stringBuilder.Reset()
	f.stringBuilder.WriteString("INSERT INTO `" + table + "`")
	f.stringBuilder.WriteString("(ID")
	k := 0
	columns := make([]string, len(events[0].Update))
	for column := range events[0].Update {
		f.stringBuilder.WriteString(",`" + column + "`")
		columns[k] = column
		k++
	}
	f.stringBuilder.WriteString(") VALUES")
	valuesPart := "(?" + strings.Repeat(",?", len(events[0].Update)) + ")"
	f.stringBuilder.WriteString(valuesPart)
	f.stringBuilder.WriteString(strings.Repeat(","+valuesPart, len(events)-1))

	args := make([]interface{}, 0)
	for _, e := range events {
		id := e.ID
		if id > 0 {
			args = append(args, id)
		} else {
			args = append(args, nil)
		}
		for _, column := range columns {
			val := e.Update[column]
			if val == NullBindValue {
				args = append(args, nil)
			} else {
				args = append(args, val)
			}
		}
	}
	newID := db.Exec(f.stringBuilder.String(), args...).LastInsertId()
	for _, e := range events {
		e.flushed = true
		if e.entity != nil {
			orm := e.entity.getORM()
			orm.inDB = true
			orm.loaded = true
			if e.ID == 0 {
				orm.id = newID
			}
			orm.serialize(f.getSerializer())
		}
		if e.ID == 0 {
			e.ID = newID
			newID += db.GetPoolConfig().getAutoincrement()
		}
		f.executePluginInterfaceEntityFlushed(e)
	}
}

func (f *flusher) executeUpdates(db *DB, table string, events []*entitySQLFlush) {
	l := len(events)
	for i, e := range events {
		if e.flushed {
			continue
		}
		f.stringBuilder.Reset()
		f.stringBuilder.WriteString("UPDATE `" + table + "` SET ")
		bind := e.Update
		for k := i + 1; k < l; k++ {
			nextEvent := events[k]
			if nextEvent.ID == e.ID {
				for key, bindValue := range nextEvent.Update {
					bind[key] = bindValue
				}
				nextEvent.flushed = true
				nextEvent.skip = true
			}
		}
		args := make([]interface{}, len(bind))
		k := 0
		for key, value := range bind {
			if k > 0 {
				f.stringBuilder.WriteString(",")
			}
			f.stringBuilder.WriteString("`" + key + "`=?")
			if value == NullBindValue {
				args[k] = nil
			} else {
				args[k] = value
			}
			k++
		}
		f.stringBuilder.WriteString(" WHERE ID=" + strconv.FormatUint(e.ID, 10))
		results := db.Exec(f.stringBuilder.String(), args...)
		if results.RowsAffected() == 0 {
			e.skip = true
		}
		e.flushed = true
		if e.entity != nil {
			orm := e.entity.getORM()
			orm.inDB = true
			orm.loaded = true
			orm.serialize(f.getSerializer())
		}
		f.executePluginInterfaceEntityFlushed(e)
	}
}

func (f *flusher) executeInsertOnDuplicateKeyUpdates(db *DB, table string, events []*entitySQLFlush) {
	for _, e := range events {
		args := make([]interface{}, len(e.Update)+len(e.UpdateOnDuplicate)+1)
		f.stringBuilder.Reset()
		f.stringBuilder.WriteString("INSERT INTO `" + table + "`")
		f.stringBuilder.WriteString("(ID")
		if events[0].ID > 0 {
			args[0] = events[0].ID
		}
		k := 1
		for column, value := range events[0].Update {
			f.stringBuilder.WriteString(",`" + column + "`")
			if value == NullBindValue {
				args[k] = nil
			} else {
				args[k] = value
			}
			k++
		}
		f.stringBuilder.WriteString(") VALUES(?")
		f.stringBuilder.WriteString(strings.Repeat(",?", len(events[0].Update)))
		f.stringBuilder.WriteString(") ON DUPLICATE KEY UPDATE ")
		if len(events[0].UpdateOnDuplicate) == 0 {
			f.stringBuilder.WriteString("ID=ID")
		} else {
			j := 0
			for column, value := range events[0].UpdateOnDuplicate {
				if j > 0 {
					f.stringBuilder.WriteString(",")
				}
				f.stringBuilder.WriteString("`" + column + "`=?")
				if value == NullBindValue {
					args[k] = nil
				} else {
					args[k] = value
				}
				j++
				k++
			}
		}
		result := db.Exec(f.stringBuilder.String(), args...)
		rowsAffected := result.RowsAffected()
		if rowsAffected == 2 {
			e.Action = Update
			e.clearLocalCache = true
			e.Update = e.UpdateOnDuplicate
			for column, value := range e.UpdateOnDuplicate {
				if e.entity != nil {
					err := e.entity.SetField(column, value)
					checkError(err)
				}
			}
			e.UpdateOnDuplicate = nil
		} else if rowsAffected == 0 {
			if e.entity != nil && e.ID == 0 {
				schema := f.engine.GetRegistry().GetEntitySchemaForEntity(e.entity).(*entitySchema)
			OUTER:
				for _, uniqueIndex := range schema.uniqueIndices {
					fields := make([]string, 0)
					binds := make([]interface{}, 0)
					for _, column := range uniqueIndex {
						currentValue, hasCurrent := e.Update[column]
						if !hasCurrent || currentValue == NullBindValue {
							continue OUTER
						}
						fields = append(fields, "`"+column+"` = ?")
						binds = append(binds, e.Update[column])
					}
					where := NewWhere("SELECT ID FROM `"+table+"` WHERE "+strings.Join(fields, " AND "), binds...)
					id := uint64(0)
					if db.QueryRow(where, &id) {
						e.ID = id
						e.entity.getORM().id = id
					}
					break
				}
			}
			e.skip = true
		} else {
			e.Action = Insert
		}
		e.flushed = true
		if e.entity != nil {
			orm := e.entity.getORM()
			orm.inDB = true
			orm.loaded = true
			orm.serialize(f.getSerializer())
			if rowsAffected > 0 {
				orm.id = result.LastInsertId()
			}
		}
		if rowsAffected > 0 {
			e.ID = result.LastInsertId()
			f.executePluginInterfaceEntityFlushed(e)
		}
	}
}

func (f *flusher) executeDeletes(db *DB, table string, events []*entitySQLFlush) {
	f.stringBuilder.Reset()
	f.stringBuilder.WriteString("DELETE FROM `" + table + "` WHERE ID IN(?")
	f.stringBuilder.WriteString(strings.Repeat(",?", len(events)-1) + ")")
	args := make([]interface{}, len(events))
	for i, e := range events {
		args[i] = e.ID
	}
	db.Exec(f.stringBuilder.String(), args...)
	for _, e := range events {
		e.flushed = true
		f.executePluginInterfaceEntityFlushed(e)
	}
}

func (f *flusher) executePluginInterfaceEntityFlushed(e *entitySQLFlush) {
	for _, plugin := range f.engine.registry.plugins {
		interfaceEntityFlushed, isInterfaceEntityFlushed := plugin.(PluginInterfaceEntityFlushed)
		if isInterfaceEntityFlushed {
			interfaceEntityFlushed.PluginInterfaceEntityFlushed(f.engine, e, f)
		}
	}
}

func (f *flusher) Track(entity ...Entity) Flusher {
	for _, e := range entity {
		initIfNeeded(f.engine.registry, e)
		address := e.getORM().value.Pointer()
		if f.trackedEntities == nil {
			f.trackedEntities = map[uintptr]Entity{address: e}
			f.trackedEntitiesCounter++
		} else {
			_, has := f.trackedEntities[address]
			if !has {
				f.trackedEntities[address] = e
				f.trackedEntitiesCounter++
			}
		}
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

func (f *flusher) GetLocalCacheSetter(code ...string) LocalCacheSetter {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	cache, has := f.localCacheSetters[dbCode]
	if !has {
		cache = &localCacheSetter{engine: f.engine, code: dbCode}
		if f.localCacheSetters == nil {
			f.localCacheSetters = make(map[string]*localCacheSetter)
		}
		f.localCacheSetters[dbCode] = cache
	}
	return cache
}

func (f *flusher) GetRedisCacheSetter(code ...string) RedisCacheSetter {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	cache, has := f.redisCacheSetters[dbCode]
	if !has {
		cache = &redisCacheSetter{engine: f.engine, code: dbCode}
		if f.redisCacheSetters == nil {
			f.redisCacheSetters = make(map[string]*redisCacheSetter)
		}
		f.redisCacheSetters[dbCode] = cache
	}
	return cache
}

func (f *flusher) PublishToStream(stream string, body interface{}, meta Bind) {
	f.GetRedisCacheSetter(getRedisCodeForStream(f.engine.registry, stream)).xAdd(stream, createEventSlice(body, meta))
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
	f.events = nil
	f.localCacheSetters = nil
	f.redisCacheSetters = nil
}

func (f *flusher) flushTrackedEntities(lazy bool) {
	if f.trackedEntitiesCounter > 0 {
		f.buildFlushEvents(f.trackedEntities, true)
		f.execute(lazy, false)
		f.events = nil
	}
	f.flushCacheSetters()
}

func (f *flusher) flushWithCheck() error {
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				f.Clear()
				asErr := convertSQLError(r.(error))
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

func (f *flusher) buildFlushEvents(source map[uintptr]Entity, root bool) {
	references := make(map[uintptr]Entity)
	for _, entity := range source {
		initIfNeeded(f.engine.registry, entity)
		if !root {
			_, has := f.trackedEntities[entity.getORM().value.Pointer()]
			if has {
				continue
			}
		}
		orm := entity.getORM()
		entitySQLFlushData, isDirty := orm.buildDirtyBind(f.getSerializer(), false)
		entitySQLFlushData.entity = entity
		if !isDirty {
			continue
		}
		f.checkReferencesToInsert(entity, entitySQLFlushData, references)

		currentID := entity.GetID()
		if orm.entitySchema.hasUUID && !orm.inDB && currentID == 0 {
			currentID = uuid()
			orm.id = currentID
			entitySQLFlushData.ID = currentID
		}
		f.addFlushEvent(entitySQLFlushData)
	}
	if len(references) > 0 {
		f.buildFlushEvents(references, false)
	}
}

func (f *flusher) buildCache(lazy, fromLazyConsumer bool) {
	for _, e := range f.events {
		if e.skip || e.ID == 0 {
			continue
		}
		schema := f.engine.registry.GetEntitySchema(e.Entity).(*entitySchema)
		hasLocalCache := schema.hasLocalCache
		localCacheCode := schema.localCacheName
		hasRedis := schema.hasRedisCache
		if !hasLocalCache && f.engine.hasRequestCache {
			hasLocalCache = true
			localCacheCode = requestCacheKey
		}
		if !hasLocalCache && !hasRedis {
			continue
		}
		cacheKey := schema.getCacheKey(e.ID)
		switch e.Action {
		case Insert:
			if lazy {
				e.entity.getORM().serialize(f.getSerializer())
				if hasLocalCache {
					f.GetLocalCacheSetter(localCacheCode).Set(cacheKey, e.entity.getORM().copyBinary())
				}
				if hasRedis {
					f.GetRedisCacheSetter(schema.redisCacheName).Set(cacheKey, e.entity.getORM().copyBinary(), 0)
				}
				return
			}
			keys := f.getCacheQueriesKeys(schema, e.Update, nil, false, true)
			if hasLocalCache {
				setter := f.GetLocalCacheSetter(localCacheCode)
				if e.entity != nil {
					e.entity.getORM().serialize(f.getSerializer())
					setter.Set(cacheKey, e.entity.getORM().copyBinary())
				} else {
					setter.Remove(cacheKey)
				}
				setter.Remove(keys...)
			}
			if hasRedis {
				setter := f.GetRedisCacheSetter(schema.redisCacheName)
				setter.Del(cacheKey)
				setter.Del(keys...)
			}
			break
		case Update:
			if lazy {
				if hasLocalCache {
					setter := f.GetLocalCacheSetter(localCacheCode)
					e.entity.getORM().serialize(f.getSerializer())
					setter.Set(cacheKey, e.entity.getORM().copyBinary())
				}
				break
			}
			keysOld := f.getCacheQueriesKeys(schema, e.Update, e.Old, true, false)
			keysNew := f.getCacheQueriesKeys(schema, e.Update, e.Old, false, false)
			if hasLocalCache {
				setter := f.GetLocalCacheSetter(localCacheCode)
				if !fromLazyConsumer || e.clearLocalCache {
					if e.entity != nil {
						setter.Set(cacheKey, e.entity.getORM().copyBinary())
					} else {
						setter.Remove(cacheKey)
					}
				}
				setter.Remove(keysOld...)
				setter.Remove(keysNew...)
			}
			if hasRedis {
				setter := f.GetRedisCacheSetter(schema.redisCacheName)
				setter.Del(cacheKey)
				setter.Del(keysOld...)
				setter.Del(keysNew...)
			}
			break
		case Delete:
			if lazy && hasLocalCache {
				f.GetLocalCacheSetter(localCacheCode).Set(cacheKey, cacheNilValue)
				break
			}
			keys := f.getCacheQueriesKeys(schema, e.Update, e.Old, true, true)
			if hasLocalCache {
				setter := f.GetLocalCacheSetter(localCacheCode)
				setter.Set(cacheKey, cacheNilValue)
				setter.Remove(keys...)
			}
			if hasRedis {
				setter := f.GetRedisCacheSetter(schema.redisCacheName)
				setter.Del(cacheKey)
				setter.Del(keys...)
			}
			break
		}
	}
}

func (f *flusher) checkReferencesToInsert(entity Entity, entitySQLFlushData *entitySQLFlush, references map[uintptr]Entity) {
	for _, refName := range entity.getORM().entitySchema.refOne {
		refValue := entity.getORM().elem.FieldByName(refName)
		if refValue.IsValid() && !refValue.IsNil() {
			refEntity := refValue.Interface().(Entity)
			initIfNeeded(f.engine.registry, refEntity)
			if refEntity.GetID() == 0 {
				address := refValue.Pointer()
				references[address] = refEntity
				if entitySQLFlushData.References == nil {
					entitySQLFlushData.References = map[string]uint64{refName: uint64(address)}
				} else {
					entitySQLFlushData.References[refName] = uint64(address)
				}
			}
		}
	}
}

func (f *flusher) getCacheQueriesKeys(schema *entitySchema, bind, current Bind, old, addedDeleted bool) (keys []string) {
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
