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

func (ft FlushType) String() string {
	switch ft {
	case Insert:
		return "INSERT"
	case InsertUpdate:
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
	trackedEntities        map[uintptr]Entity
	trackedEntitiesCounter int
	serializer             *serializer
	events                 []*EntitySQLFlush
	stringBuilder          strings.Builder
}

func (f *flusher) addFlushEvent(sqlFlush *EntitySQLFlush) {
	f.events = append(f.events, sqlFlush)
}

func (f *flusher) execute(lazy bool) {
	//TODO
	fmt.Printf("EVENTS %d\n", len(f.events))
	for i, e := range f.events {
		fmt.Printf("---EVENT %d---\n", i)
		fmt.Printf("ID: %v\n", e.ID)
		fmt.Printf("EntityName: %v\n", e.EntityName)
		fmt.Printf("Action: %v\n", e.Action)
		fmt.Printf("Update: %v\n", e.Update)
		fmt.Printf("Old: %v\n", e.Old)
		fmt.Printf("TempID: %v\n", e.TempID)
		fmt.Printf("References: %v\n\n", e.References)
	}
	if len(f.events) == 0 {
		return
	}
	if lazy {
		f.engine.GetEventBroker().Publish(LazyFlushChannelName, f.events)
		return
	}
	checkReferences := true
	startTransaction := make(map[*DB]bool)
	f.engine.EnableQueryDebug()
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
			group := make(map[*DB]map[string]map[FlushType][]*EntitySQLFlush)
			for _, e := range f.events {
				if e.flushed {
					continue
				}
				if len(e.References) > 0 {
					checkReferences = true
				} else {
					schema := f.engine.registry.GetTableSchema(e.EntityName)
					db := schema.GetMysql(f.engine)
					byDB, hasDB := group[db]
					if !hasDB {
						byDB = make(map[string]map[FlushType][]*EntitySQLFlush)
						group[db] = byDB
					}
					byTable, hasTable := byDB[schema.GetTableName()]
					if !hasTable {
						byTable = make(map[FlushType][]*EntitySQLFlush)
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
						if len(byAction) > 1 {
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
					for action, events := range byAction {
						switch action {
						case Insert:
							f.executeInserts(db, tableName, events)
							if checkReferences {
								for _, e := range f.events {
									for column, address := range e.References {
										for _, inserted := range events {
											if inserted.TempID == address {
												e.Update[column] = strconv.FormatUint(inserted.ID, 10)
												delete(e.References, column)
											}
										}
									}
								}
							}
							break
						case Update:
							f.executeUpdates(db, tableName, events)
							break
						case InsertUpdate:
							f.executeInsertOnDuplicateKeyUpdates(db, tableName, events)
							break
						}
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

	f.events = nil
}

func (f *flusher) executeInserts(db *DB, table string, events []*EntitySQLFlush) {
	f.stringBuilder.Reset()
	f.stringBuilder.WriteString("INSERT INTO `" + table + "`")
	f.stringBuilder.WriteString("(")
	k := 0
	columns := make([]string, len(events[0].Update))
	for column := range events[0].Update {
		if k > 0 {
			f.stringBuilder.WriteString(",")
		}
		f.stringBuilder.WriteString("`" + column + "`")
		columns[k] = column
		k++
	}
	f.stringBuilder.WriteString(") VALUES")
	valuesPart := "(?" + strings.Repeat(",?", len(events[0].Update)-1) + ")"
	f.stringBuilder.WriteString(valuesPart)
	f.stringBuilder.WriteString(strings.Repeat(valuesPart, len(events)-1))

	args := make([]interface{}, 0)
	for _, e := range events {
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
				orm.idElem.SetUint(newID)
			}
			orm.serialize(f.getSerializer())
		}
		if e.ID == 0 {
			e.ID = newID
			newID += db.GetPoolConfig().getAutoincrement()
		}
	}
}

func (f *flusher) executeUpdates(db *DB, table string, events []*EntitySQLFlush) {
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
		db.Exec(f.stringBuilder.String(), args...)
		e.flushed = true
		if e.entity != nil {
			orm := e.entity.getORM()
			orm.inDB = true
			orm.loaded = true
			orm.serialize(f.getSerializer())
		}
	}
}

func (f *flusher) executeInsertOnDuplicateKeyUpdates(db *DB, table string, events []*EntitySQLFlush) {
	for _, e := range events {

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
	f.buildFlushEvents(f.trackedEntities, true)
	f.execute(lazy)
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
		entitySQLFlushData, isDirty := orm.buildDirtyBind(f.getSerializer())
		entitySQLFlushData.entity = entity
		if !isDirty {
			continue
		}
		f.checkReferencesToInsert(entity, entitySQLFlushData, references)

		currentID := entity.GetID()
		if orm.tableSchema.hasUUID && !orm.inDB && currentID == 0 {
			currentID = uuid()
			orm.idElem.SetUint(currentID)
			entitySQLFlushData.Update["ID"] = strconv.FormatUint(currentID, 10)
		}

		//entityCacheFlushData := &EntityCacheFlush{EntitySQLFlush: entitySQLFlushData}
		//
		//currentID := entity.GetID()
		//if orm.fakeDelete && !orm.tableSchema.hasFakeDelete {
		//	orm.delete = true
		//}
		//if orm.delete {
		//	f.fillCacheFlushDataForDelete(entity, entityCacheFlushData)
		//} else if !orm.inDB {
		//	if currentID == 0 && orm.tableSchema.hasUUID {
		//		currentID = uuid()
		//		orm.idElem.SetUint(currentID)
		//	}
		//	if currentID > 0 {
		//		entityCacheFlushData.EntitySQLFlush.Update["ID"] = strconv.FormatUint(currentID, 10)
		//	}
		//	f.fillCacheFlushDataForInsert(entity, entityCacheFlushData)
		//} else {
		//	f.fillCacheFlushDataForUpdate(entity, entityCacheFlushData)
		//}
		f.addFlushEvent(entitySQLFlushData)
	}
	if len(references) > 0 {
		f.buildFlushEvents(references, false)
	}
}

func (f *flusher) fillCacheFlushDataForInsert(entity Entity, entityFlushData *EntityCacheFlush) {
	schema := entity.getORM().tableSchema
	localCache, hasLocalCache := schema.GetLocalCache(f.engine)
	redisCache, hasRedis := schema.GetRedisCache(f.engine)
	if !hasLocalCache && f.engine.hasRequestCache {
		hasLocalCache = true
		localCache = f.engine.GetLocalCache(requestCacheKey)
	}
	if !hasLocalCache && !hasRedis {
		return
	}
	cacheKey := schema.getCacheKey(entity.GetID())
	keys := f.getCacheQueriesKeys(schema, entityFlushData.Update, nil, false, true)
	if hasLocalCache {
		entityFlushData.AddInLocalCache(localCache.config.GetCode(), cacheKey, entity.getORM().copyBinary())
		entityFlushData.DeleteInLocalCache(localCache.config.GetCode(), keys...)
	}
	if hasRedis {
		entityFlushData.DeleteInRedis(redisCache.config.GetCode(), cacheKey)
		entityFlushData.DeleteInRedis(redisCache.config.GetCode(), keys...)
	}
}

func (f *flusher) fillCacheFlushDataForUpdate(entity Entity, entityFlushData *EntityCacheFlush) {
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

func (f *flusher) fillCacheFlushDataForDelete(entity Entity, entityFlushData *EntityCacheFlush) {
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

func (f *flusher) checkReferencesToInsert(entity Entity, entitySQLFlushData *EntitySQLFlush, references map[uintptr]Entity) {
	for _, refName := range entity.getORM().tableSchema.refOne {
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
