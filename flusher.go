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
	FlushWithFullCheck() error
	FlushLazy()
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
	stringBuilder          strings.Builder
	serializer             *serializer
}

func (f *flusher) Track(entity ...Entity) Flusher {
main:
	for _, entity := range entity {
		initIfNeeded(f.engine.registry, entity)
		if f.trackedEntities == nil {
			f.trackedEntities = []Entity{entity}
		} else {
			for _, old := range f.trackedEntities {
				if old == entity {
					continue main
				}
			}
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
	executed := false
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
		if !executed {
			if dbPools == nil {
				dbPools = make(map[string]*DB)
				for _, entity := range f.trackedEntities {
					db := entity.getORM().tableSchema.GetMysql(f.engine)
					dbPools[db.GetPoolConfig().GetCode()] = db
				}
			}
			for _, db := range dbPools {
				db.Rollback()
			}
		}
	}()
	useTransaction := f.flush(true, lazy, transaction, f.trackedEntities...)
	if transaction {
		for _, db := range dbPools {
			db.Commit()
		}
	} else if useTransaction {
		if dbPools == nil {
			dbPools = make(map[string]*DB)
			for _, entity := range f.trackedEntities {
				db := entity.getORM().tableSchema.GetMysql(f.engine)
				dbPools[db.GetPoolConfig().GetCode()] = db
			}
		}
		for _, db := range dbPools {
			db.Commit()
		}
	}
	executed = true
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

func (f *flusher) getSerializer() *serializer {
	if f.serializer == nil {
		f.serializer = newSerializer(nil)
	}
	return f.serializer
}

type flushPackage struct {
	insertKeys          map[reflect.Type][]string
	insertBinds         map[reflect.Type][]Bind
	insertSQLBinds      map[reflect.Type][]map[string]string
	insertReflectValues map[reflect.Type][]Entity
	referencesToFlash   map[Entity]Entity
}

func (f *flusher) flush(root bool, lazy bool, transaction bool, entities ...Entity) (useTransaction bool) {
	flushPackage := &flushPackage{
		insertKeys:          make(map[reflect.Type][]string),
		insertBinds:         make(map[reflect.Type][]Bind),
		insertSQLBinds:      make(map[reflect.Type][]map[string]string),
		insertReflectValues: make(map[reflect.Type][]Entity),
		referencesToFlash:   make(map[Entity]Entity),
	}

	for _, entity := range entities {
		initIfNeeded(f.engine.registry, entity)
		schema := entity.getORM().tableSchema
		if !transaction && schema.GetMysql(f.engine).inTransaction {
			transaction = true
		}
		if f.checkReferences(schema, entity, flushPackage) {
			continue
		}

		orm := entity.getORM()
		bindBuilder, isDirty := orm.buildDirtyBind(f.getSerializer())
		if !isDirty {
			continue
		}

		t := orm.tableSchema.t
		currentID := entity.GetID()
		if orm.fakeDelete && !orm.tableSchema.hasFakeDelete {
			orm.delete = true
		}
		if orm.delete {
			f.flushDelete(t, currentID, entity)
		} else if !orm.inDB {
			if currentID > 0 {
				bindBuilder.bind["ID"] = currentID
				if bindBuilder.buildSQL {
					bindBuilder.sqlBind["ID"] = strconv.FormatUint(currentID, 10)
				}
			}
			if f.flushOnDuplicateKey(lazy, bindBuilder, schema, entity) {
				continue
			}
			f.flushInsert(t, bindBuilder, flushPackage, entity)
		} else {
			f.flushUpdate(entity, bindBuilder, currentID, schema, lazy)
		}
	}

	if f.flushReferences(flushPackage, lazy, transaction, entities) {
		return !transaction
	}
	if !transaction {
		diffs := len(flushPackage.insertKeys)
		if diffs <= 1 {
			diffs += len(f.updateSQLs)
			if diffs <= 1 {
				diffs += len(f.deleteBinds)
				if diffs <= 1 && len(f.updateSQLs) == 1 {
					for _, queries := range f.updateSQLs {
						diffs = len(queries)
					}
				}
			}
		}
		if diffs > 1 {
			f.startTransaction()
			useTransaction = true
		}
	}

	f.executeInserts(flushPackage, lazy)
	if root {
		f.executeUpdates()
		f.executeDeletes(lazy)
		f.updateLocalCache(lazy, useTransaction || transaction)
	}
	f.updateRedisCache(root, lazy, useTransaction || transaction)
	return useTransaction
}

func (f *flusher) updateRedisCache(root bool, lazy bool, transaction bool) {
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
		f.getRedisFlusher().Publish(LazyChannelName, f.lazyMap)
		f.lazyMap = nil
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

func (f *flusher) executeDeletes(lazy bool) {
	for typeOf, deleteBinds := range f.deleteBinds {
		queryExecuted := false
		schema := getTableSchema(f.engine.registry, typeOf)
		var logEvents []*LogQueueValue
		var dirtyEvents []*dirtyQueueValue
		i := 0
		f.stringBuilder.WriteString("DELETE FROM `")
		f.stringBuilder.WriteString(schema.tableName)
		f.stringBuilder.WriteString("` WHERE `ID` IN (")
		for id := range deleteBinds {
			if i > 0 {
				f.stringBuilder.WriteString(",")
			}
			f.stringBuilder.WriteString(strconv.FormatUint(id, 10))
			i++
		}
		f.stringBuilder.WriteString(")")
		deleteSQL := f.stringBuilder.String()
		f.stringBuilder.Reset()
		db := schema.GetMysql(f.engine)
		localCache, hasLocalCache := schema.GetLocalCache(f.engine)
		redisCache, hasRedis := schema.GetRedisCache(f.engine)
		if !hasLocalCache && f.engine.hasRequestCache {
			hasLocalCache = true
			localCache = f.engine.GetLocalCache(requestCacheKey)
		}
		for id, entity := range deleteBinds {
			orm := entity.getORM()
			bindBuilder, _ := orm.buildDirtyBind(f.getSerializer())
			if !lazy {
				if !queryExecuted {
					_ = db.Exec(deleteSQL)
					queryExecuted = true
				}
				f.addDirtyQueues(bindBuilder.current, schema, id, "d", lazy)
				f.addToLogQueue(schema, id, bindBuilder.current, nil, entity.getORM().logMeta, lazy)
			} else {
				logEvent := f.addToLogQueue(schema, id, bindBuilder.current, nil, orm.logMeta, lazy)
				if logEvent != nil {
					logEvents = append(logEvents, logEvent)
				}
				dirtyEvent := f.addDirtyQueues(bindBuilder.current, schema, id, "d", lazy)
				if dirtyEvent != nil {
					dirtyEvents = append(dirtyEvents, dirtyEvent)
				}
			}
			if hasLocalCache || hasRedis {
				cacheKey := schema.getCacheKey(id)
				keys := f.getCacheQueriesKeys(schema, bindBuilder.bind, bindBuilder.current, true, true)
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
			f.fillLazyQuery(db.GetPoolConfig().GetCode(), deleteSQL, logEvents, dirtyEvents)
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
		f.stringBuilder.WriteString("INSERT INTO ")
		f.stringBuilder.WriteString(schema.tableName)
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
		if lazy && !schema.hasSearchCache {
			var logEvents []*LogQueueValue
			var dirtyEvents []*dirtyQueueValue
			for key, entity := range flushPackage.insertReflectValues[typeOf] {
				logEvent, dirtyEvent := f.updateCacheForInserted(entity, lazy, 0, flushPackage.insertBinds[typeOf][key])
				if logEvent != nil {
					logEvents = append(logEvents, logEvent)
				}
				if dirtyEvent != nil {
					dirtyEvents = append(dirtyEvents, dirtyEvent)
				}
			}
			f.fillLazyQuery(db.GetPoolConfig().GetCode(), sql, logEvents, dirtyEvents)
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

func (f *flusher) flushInsert(t reflect.Type, bindBuilder *bindBuilder, flushPackage *flushPackage, entity Entity) {
	if flushPackage.insertKeys[t] == nil {
		fields := make([]string, len(bindBuilder.bind))
		i := 0
		for key := range bindBuilder.bind {
			fields[i] = key
			i++
		}
		flushPackage.insertKeys[t] = fields
	}
	_, has := flushPackage.insertBinds[t]
	if !has {
		flushPackage.insertBinds[t] = make([]Bind, 0)
		flushPackage.insertSQLBinds[t] = make([]map[string]string, 0)
	}
	flushPackage.insertReflectValues[t] = append(flushPackage.insertReflectValues[t], entity)
	flushPackage.insertBinds[t] = append(flushPackage.insertBinds[t], bindBuilder.bind)
	flushPackage.insertSQLBinds[t] = append(flushPackage.insertSQLBinds[t], bindBuilder.sqlBind)
}

func (f *flusher) flushDelete(t reflect.Type, currentID uint64, entity Entity) {
	if f.deleteBinds == nil {
		f.deleteBinds = make(map[reflect.Type]map[uint64]Entity)
	}
	if f.deleteBinds[t] == nil {
		f.deleteBinds[t] = make(map[uint64]Entity)
	}
	f.deleteBinds[t][currentID] = entity
}

func (f *flusher) startTransaction() {
	dbPools := make(map[string]*DB)
	for _, entity := range f.trackedEntities {
		db := entity.getORM().tableSchema.GetMysql(f.engine)
		dbPools[db.GetPoolConfig().GetCode()] = db
	}
	for _, db := range dbPools {
		db.Begin()
	}
}

func (f *flusher) flushReferences(flushPackage *flushPackage, lazy bool, transaction bool, entities []Entity) bool {
	if len(flushPackage.referencesToFlash) > 0 {
		if lazy {
			panic(fmt.Errorf("lazy flush for unsaved references is not supported"))
		}
		if !transaction {
			f.startTransaction()
		}
		toFlush := make([]Entity, len(flushPackage.referencesToFlash))
		i := 0
		for _, v := range flushPackage.referencesToFlash {
			toFlush[i] = v
			i++
		}
		f.flush(false, false, transaction, toFlush...)
		rest := make([]Entity, 0)
		for _, v := range entities {
			_, has := flushPackage.referencesToFlash[v]
			if !has {
				rest = append(rest, v)
			}
		}
		if len(rest) > 0 {
			f.flush(true, false, transaction, rest...)
		}
		return true
	}
	return false
}

func (f *flusher) flushUpdate(entity Entity, bindBuilder *bindBuilder, currentID uint64, schema *tableSchema, lazy bool) {
	if !entity.IsLoaded() {
		panic(fmt.Errorf("entity is not loaded and can't be updated: %v [%d]", entity.getORM().elem.Type().String(), currentID))
	}
	f.stringBuilder.WriteString("UPDATE ")
	f.stringBuilder.WriteString(schema.GetTableName())
	f.stringBuilder.WriteString(" SET ")
	first := true
	for key, value := range bindBuilder.sqlBind {
		if !first {
			f.stringBuilder.WriteString(",")
		}
		first = false
		f.stringBuilder.WriteString("`" + key + "`=" + value)
	}
	f.stringBuilder.WriteString(" WHERE `ID` = ")
	f.stringBuilder.WriteString(strconv.FormatUint(currentID, 10))
	sql := f.stringBuilder.String()
	f.stringBuilder.Reset()
	db := schema.GetMysql(f.engine)
	if lazy {
		var logEvents []*LogQueueValue
		var dirtyEvents []*dirtyQueueValue
		entity.getORM().serialize(f.getSerializer())
		logEvent, dirtyEvent := f.updateCacheAfterUpdate(entity, bindBuilder.bind, bindBuilder.current, schema, currentID, true)
		if logEvent != nil {
			logEvents = append(logEvents, logEvent)
		}
		if dirtyEvent != nil {
			dirtyEvents = append(dirtyEvents, dirtyEvent)
		}
		f.fillLazyQuery(db.GetPoolConfig().GetCode(), sql, logEvents, dirtyEvents)
	} else {
		if f.updateSQLs == nil {
			f.updateSQLs = make(map[string][]string)
		}
		f.updateSQLs[schema.mysqlPoolName] = append(f.updateSQLs[schema.mysqlPoolName], sql)
		entity.getORM().serialize(f.getSerializer())
		f.updateCacheAfterUpdate(entity, bindBuilder.bind, bindBuilder.current, schema, currentID, false)
	}
}

func (f *flusher) flushOnDuplicateKey(lazy bool, bindBuilder *bindBuilder, schema *tableSchema, entity Entity) bool {
	onUpdate := entity.getORM().onDuplicateKeyUpdate
	if onUpdate == nil {
		return false
	}
	if lazy {
		panic(fmt.Errorf("lazy flush on duplicate key is not supported"))
	}
	bindLength := len(bindBuilder.bind)
	values := make([]string, bindLength)
	columns := make([]string, bindLength)
	i := 0

	for key, val := range bindBuilder.sqlBind {
		columns[i] = "`" + key + "`"
		values[i] = val
		i++
	}
	f.stringBuilder.WriteString("INSERT INTO ")
	f.stringBuilder.WriteString(schema.tableName)
	f.stringBuilder.WriteString("(")
	f.stringBuilder.WriteString(strings.Join(columns, ","))
	f.stringBuilder.WriteString(") VALUES (")
	f.stringBuilder.WriteString(strings.Join(values, ","))
	f.stringBuilder.WriteString(")")
	/* #nosec */
	f.stringBuilder.WriteString(" ON DUPLICATE KEY UPDATE ")
	first := true
	for k, v := range onUpdate {
		if !first {
			f.stringBuilder.WriteString(",")
		}
		f.stringBuilder.WriteString("`")
		f.stringBuilder.WriteString(k)
		f.stringBuilder.WriteString("` = ")
		f.stringBuilder.WriteString(escapeSQLValue(v))
		first = false
	}
	if len(onUpdate) == 0 {
		f.stringBuilder.WriteString("ID = ID")
	}
	sql := f.stringBuilder.String()
	f.stringBuilder.Reset()
	db := schema.GetMysql(f.engine)
	result := db.Exec(sql)
	affected := result.RowsAffected()
	if affected > 0 {
		orm := entity.getORM()
		lastID := result.LastInsertId()
		orm.inDB = true
		orm.loaded = true
		orm.idElem.SetUint(lastID)
		orm.serialize(f.getSerializer())
		if affected == 1 {
			f.updateCacheForInserted(entity, lazy, lastID, bindBuilder.bind)
		} else {
			for k, v := range onUpdate {
				err := entity.SetField(k, v)
				checkError(err)
			}
			bindBuilderNew, _ := orm.buildDirtyBind(f.getSerializer())
			_, _ = loadByID(f.getSerializer(), f.engine, lastID, entity, false)
			f.updateCacheAfterUpdate(entity, bindBuilderNew.bind, bindBuilderNew.current, schema, lastID, false)
		}
	} else {
	OUTER:
		for _, index := range schema.uniqueIndices {
			fields := make([]string, 0)
			binds := make([]interface{}, 0)
			for _, column := range index {
				if bindBuilder.bind[column] == nil {
					continue OUTER
				}
				fields = append(fields, "`"+column+"` = ?")
				binds = append(binds, bindBuilder.bind[column])
			}
			findWhere := NewWhere(strings.Join(fields, " AND "), binds)
			f.engine.SearchOne(findWhere, entity)
			break
		}
	}
	return true
}

func (f *flusher) checkReferences(schema *tableSchema, entity Entity, flushPackage *flushPackage) bool {
	has := false
	for _, refName := range schema.refOne {
		refValue := entity.getORM().elem.FieldByName(refName)
		if refValue.IsValid() && !refValue.IsNil() {
			refEntity := refValue.Interface().(Entity)
			initIfNeeded(f.engine.registry, refEntity)
			if refEntity.GetID() == 0 {
				flushPackage.referencesToFlash[refEntity] = refEntity
				has = true
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
					flushPackage.referencesToFlash[refEntity] = refEntity
					has = true
				}
			}
		}
	}
	return has
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
	f.fillRedisSearchFromBind(schema, bind, id, true)
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
	f.fillRedisSearchFromBind(schema, bind, entity.GetID(), false)
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
		f.getRedisFlusher().Publish(LogChannelName, val)
	}
	return val
}

func (f *flusher) fillRedisSearchFromBind(schema *tableSchema, bind Bind, id uint64, insert bool) {
	if schema.hasSearchCache {
		values := make([]interface{}, 0)
		hasChangedField := false
		if schema.hasFakeDelete {
			val, has := bind["FakeDelete"]
			if has && val.(uint64) > 0 {
				if !schema.hasSearchableFakeDelete {
					f.getRedisFlusher().Del(schema.searchCacheName, schema.redisSearchPrefix+strconv.FormatUint(id, 10))
				} else {
					values = append(values, "FakeDelete", "true")
					hasChangedField = true
				}
			}
		}

		idMap, has := schema.mapBindToRedisSearch["ID"]
		if has {
			values = append(values, "ID", idMap(id))
			if !hasChangedField {
				hasChangedField = insert
			}
		}
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

func (f *flusher) fillLazyQuery(dbCode string, sql string, logEvent []*LogQueueValue, dirtyData []*dirtyQueueValue) {
	lazyMap := f.getLazyMap()
	updatesMap := lazyMap["q"]
	if updatesMap == nil {
		updatesMap = make([]interface{}, 0)
		lazyMap["q"] = updatesMap
	}
	lazyValue := make([]interface{}, 3)
	lazyValue[0] = dbCode
	lazyValue[1] = sql
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
