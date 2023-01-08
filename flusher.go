package beeorm

import (
	"fmt"
	"strconv"
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
	trackedEntities        map[Entity]Entity
	trackedEntitiesCounter int
	serializer             *serializer
	events                 []*EntityCacheFlush
}

func (f *flusher) addFlushEvent(flushData *EntityCacheFlush) {
	f.events = append(f.events, flushData)
}

func (f *flusher) execute(lazy bool) {
	//TODO
}

func (f *flusher) Track(entity ...Entity) Flusher {
	for _, e := range entity {
		initIfNeeded(f.engine.registry, e)
		if f.trackedEntities == nil {
			f.trackedEntities = map[Entity]Entity{e: e}
			f.trackedEntitiesCounter++
		} else {
			_, has := f.trackedEntities[e]
			if !has {
				f.trackedEntities[e] = e
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
	f.buildFlushEvents()
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

func (f *flusher) buildFlushEvents() {
	for _, entity := range f.trackedEntities {
		initIfNeeded(f.engine.registry, entity)
		f.checkReferencesToInsert(entity)

		orm := entity.getORM()
		entitySQLFlushData, isDirty := orm.buildDirtyBind(f.getSerializer())
		if !isDirty {
			continue
		}
		entityCacheFlushData := &EntityCacheFlush{EntitySQLFlush: entitySQLFlushData}

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
				entityCacheFlushData.EntitySQLFlush.Update["ID"] = strconv.FormatUint(currentID, 10)
			}
		} else {
			f.fillCacheFlushDataForUpdate(entity, entityCacheFlushData)
		}
		f.addFlushEvent(entityCacheFlushData)
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

func (f *flusher) checkReferencesToInsert(entity Entity) {
	for _, refName := range entity.getORM().tableSchema.refOne {
		refValue := entity.getORM().elem.FieldByName(refName)
		if refValue.IsValid() && !refValue.IsNil() {
			refEntity := refValue.Interface().(Entity)
			initIfNeeded(f.engine.registry, refEntity)
			if refEntity.GetID() == 0 {
				f.Track(refEntity)
				f.checkReferencesToInsert(refEntity)
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
