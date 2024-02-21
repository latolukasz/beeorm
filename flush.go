package beeorm

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/puzpuzpuz/xsync/v2"

	jsoniter "github.com/json-iterator/go"
)

type entitySQLOperations map[FlushType][]EntityFlush
type schemaSQLOperations map[*entitySchema]entitySQLOperations
type sqlOperations map[DB]schemaSQLOperations
type dbAction func(db DBBase)
type PostFlushAction func(orm ORM)

func (orm *ormImplementation) Flush() error {
	return orm.flush(false)
}

func (orm *ormImplementation) FlushAsync() error {
	return orm.flush(true)
}

func (orm *ormImplementation) flush(async bool) error {
	orm.mutexFlush.Lock()
	defer orm.mutexFlush.Unlock()
	if orm.trackedEntities == nil || orm.trackedEntities.Size() == 0 {
		return nil
	}
	sqlGroup := orm.groupSQLOperations()
	for _, operations := range sqlGroup {
		for schema, queryOperations := range operations {
			deletes, has := queryOperations[Delete]
			if has {
				err := orm.handleDeletes(async, schema, deletes)
				if err != nil {
					return err
				}
			}
			inserts, has := queryOperations[Insert]
			if has {
				err := orm.handleInserts(async, schema, inserts)
				if err != nil {
					return err
				}
			}
			updates, has := queryOperations[Update]
			if has {
				err := orm.handleUpdates(async, schema, updates)
				if err != nil {
					return err
				}
			}
		}
	}
	var err error
	if !async {
		func() {
			var transactions []DBTransaction
			defer func() {
				for _, tx := range transactions {
					tx.Rollback(orm)
				}
				if rec := recover(); rec != nil {
					asErr, isErr := rec.(error)
					if isErr {
						err = asErr
						return
					}
					err = fmt.Errorf("%v", rec)
				}
			}()
			for code, actions := range orm.flushDBActions {
				var d DBBase
				d = orm.Engine().DB(code)
				if len(actions) > 1 || len(orm.flushDBActions) > 1 {
					tx := d.(DB).Begin(orm)
					transactions = append(transactions, tx)
					d = tx
				}
				for _, action := range actions {
					action(d)
				}
			}
			for _, tx := range transactions {
				tx.Commit(orm)
			}
		}()
	}
	for _, pipeline := range orm.redisPipeLines {
		pipeline.Exec(orm)
	}
	for _, action := range orm.flushPostActions {
		action(orm)
	}
	orm.trackedEntities.Clear()
	orm.flushDBActions = nil
	orm.flushPostActions = orm.flushPostActions[0:0]
	orm.redisPipeLines = nil
	return err
}

func (orm *ormImplementation) ClearFlush() {
	orm.mutexFlush.Lock()
	defer orm.mutexFlush.Unlock()
	orm.trackedEntities.Clear()
	orm.flushDBActions = nil
	orm.flushPostActions = orm.flushPostActions[0:0]
	orm.redisPipeLines = nil
}

func (orm *ormImplementation) handleDeletes(async bool, schema *entitySchema, operations []EntityFlush) error {
	var args []any
	if !async {
		args = make([]any, len(operations))
	}
	sql := "DELETE FROM `" + schema.GetTableName() + "` WHERE ID IN ("
	if async {
		for i, operation := range operations {
			if i > 0 {
				sql += ","
			}
			sql += strconv.FormatUint(operation.ID(), 10)
		}
	} else {
		sql += "?" + strings.Repeat(",?", len(operations)-1)
	}
	sql += ")"
	if !async {
		for i, operation := range operations {
			args[i] = operation.ID()
		}
		orm.appendDBAction(schema, func(db DBBase) {
			db.Exec(orm, sql, args...)
		})
	} else {
		publishAsyncEvent(schema, []any{sql})
	}

	lc, hasLocalCache := schema.GetLocalCache()
	for _, operation := range operations {
		uniqueIndexes := schema.GetUniqueIndexes()
		var bind Bind
		var err error
		deleteFlush := operation.(entityFlushDelete)
		if len(uniqueIndexes) > 0 {
			bind, err = deleteFlush.getOldBind()
			if err != nil {
				return err
			}
			cache := orm.Engine().Redis(schema.getForcedRedisCode())
			for indexName, indexColumns := range uniqueIndexes {
				hSetKey := schema.getCacheKey() + ":" + indexName
				hField, hasKey := buildUniqueKeyHSetField(schema, indexColumns, bind, nil)
				if hasKey {
					orm.RedisPipeLine(cache.GetConfig().GetCode()).HDel(hSetKey, hField)
				}
			}
		}
		if hasLocalCache {
			orm.flushPostActions = append(orm.flushPostActions, func(_ ORM) {
				lc.setEntity(orm, operation.ID(), nil)
			})
		}
		rc, hasRedisCache := schema.GetRedisCache()
		if hasRedisCache {
			cacheKey := schema.getCacheKey() + ":" + strconv.FormatUint(operation.ID(), 10)
			orm.RedisPipeLine(rc.GetCode()).Del(cacheKey)
			orm.RedisPipeLine(rc.GetCode()).LPush(cacheKey, "")
		}
		for columnName := range schema.cachedReferences {
			if bind == nil {
				bind, err = deleteFlush.getOldBind()
				if err != nil {
					return err
				}
			}
			id := bind[columnName]
			if id == nil {
				continue
			}
			refColumn := columnName
			if schema.hasLocalCache {
				orm.flushPostActions = append(orm.flushPostActions, func(_ ORM) {
					lc.removeList(orm, refColumn, id.(uint64))
				})
			}
			idAsString := strconv.FormatUint(id.(uint64), 10)
			redisSetKey := schema.cacheKey + ":" + refColumn + ":" + idAsString
			orm.RedisPipeLine(schema.getForcedRedisCode()).SRem(redisSetKey, strconv.FormatUint(deleteFlush.ID(), 10))
		}
		if schema.cacheAll {
			if schema.hasLocalCache {
				orm.flushPostActions = append(orm.flushPostActions, func(_ ORM) {
					lc.removeList(orm, cacheAllFakeReferenceKey, 0)
				})
			}
			redisSetKey := schema.cacheKey + ":" + cacheAllFakeReferenceKey
			orm.RedisPipeLine(schema.getForcedRedisCode()).SRem(redisSetKey, strconv.FormatUint(deleteFlush.ID(), 10))
		}
		for indexName, def := range schema.cachedIndexes {
			if bind == nil {
				bind, err = deleteFlush.getOldBind()
				if err != nil {
					return err
				}
			}
			indexAttributes := make([]any, len(def.Columns))
			for j, indexColumn := range def.Columns {
				indexAttributes[j] = bind[indexColumn]
			}
			key := indexName
			id := hashIndexAttributes(indexAttributes)
			if schema.hasLocalCache {
				orm.flushPostActions = append(orm.flushPostActions, func(_ ORM) {
					lc.removeList(orm, key, id)
				})
			}
			idAsString := strconv.FormatUint(id, 10)
			redisSetKey := schema.cacheKey + ":" + key + ":" + idAsString
			orm.RedisPipeLine(schema.getForcedRedisCode()).SRem(redisSetKey, strconv.FormatUint(deleteFlush.ID(), 10))
		}
		logTableSchema, hasLogTable := orm.engine.registry.entityLogSchemas[schema.t]
		if hasLogTable {
			data := make([]any, 6)
			data[0] = "INSERT INTO `" + logTableSchema.tableName + "`(ID,EntityID,Date,Meta,`Before`) VALUES(?,?,?,?,?)"
			data[1] = strconv.FormatUint(logTableSchema.uuid(orm), 10)
			data[2] = strconv.FormatUint(operation.ID(), 10)
			data[3] = time.Now().Format(time.DateTime)
			if len(orm.meta) > 0 {
				asJSON, _ := jsoniter.ConfigFastest.MarshalToString(orm.meta)
				data[4] = asJSON
			} else {
				data[4] = nil
			}
			if bind == nil {
				bind, err = deleteFlush.getOldBind()
				if err != nil {
					return err
				}
			}
			asJSON, _ := jsoniter.ConfigFastest.MarshalToString(bind)
			data[5] = asJSON
			publishAsyncEvent(logTableSchema, data)
		}
		for _, p := range orm.engine.pluginFlush {
			if bind == nil {
				bind, err = deleteFlush.getOldBind()
				if err != nil {
					return err
				}
			}
			after, err := p.EntityFlush(schema, deleteFlush.getValue(), bind, nil, orm.engine)
			if err != nil {
				return err
			}
			if after != nil {
				orm.flushPostActions = append(orm.flushPostActions, after)
			}
		}
	}
	return nil
}

func (orm *ormImplementation) handleInserts(async bool, schema *entitySchema, operations []EntityFlush) error {
	columns := schema.GetColumns()
	sql := "INSERT INTO `" + schema.GetTableName() + "`(`ID`"
	for _, column := range columns[1:] {
		sql += ",`" + column + "`"
	}
	sql += ") VALUES"
	var args []any
	if !async {
		args = make([]any, 0, len(operations)*len(columns))
	}
	lc, hasLocalCache := schema.GetLocalCache()
	rc, hasRedisCache := schema.GetRedisCache()
	for i, operation := range operations {
		insert := operation.(entityFlushInsert)
		bind, err := insert.getBind()
		if err != nil {
			return err
		}
		if len(orm.engine.pluginFlush) > 0 {
			elem := insert.getValue().Elem()
			for _, p := range orm.engine.pluginFlush {
				after, err := p.EntityFlush(schema, elem, nil, bind, orm.engine)
				if err != nil {
					return err
				}
				if after != nil {
					orm.flushPostActions = append(orm.flushPostActions, after)
				}
			}
		}
		uniqueIndexes := schema.cachedUniqueIndexes
		if len(uniqueIndexes) > 0 {
			cache := orm.Engine().Redis(schema.getForcedRedisCode())
			for indexName, definition := range uniqueIndexes {
				hSetKey := schema.getCacheKey() + ":" + indexName
				hField, hasKey := buildUniqueKeyHSetField(schema, definition.Columns, bind, nil)
				if !hasKey {
					continue
				}
				orm.RedisPipeLine(cache.GetConfig().GetCode()).HSet(hSetKey, hField, strconv.FormatUint(insert.ID(), 10))
			}
		}
		var asyncData []any
		if async {
			asyncData = make([]any, len(columns)+1)
		}

		if i > 0 && !async {
			sql += ","
		}
		if !async || i == 0 {
			sql += "(?"
		}
		if !async {
			args = append(args, bind["ID"])
		} else {
			if async {
				asyncData[1] = strconv.FormatUint(bind["ID"].(uint64), 10)
			} else {
				asyncData[1] = bind["ID"]
			}
		}
		for j, column := range columns[1:] {
			v := bind[column]
			if async {
				vAsUint64, isUint64 := v.(uint64)
				if isUint64 {
					v = strconv.FormatUint(vAsUint64, 10)
				}
			}
			if !async {
				args = append(args, v)
			} else {
				asyncData[j+2] = v
			}
			if !async || i == 0 {
				sql += ",?"
			}
		}
		if !async || i == 0 {
			sql += ")"
		}
		if async {
			asyncData[0] = sql
			publishAsyncEvent(schema, asyncData)
		}
		logTableSchema, hasLogTable := orm.engine.registry.entityLogSchemas[schema.t]
		if hasLogTable {
			data := make([]any, 6)
			data[0] = "INSERT INTO `" + logTableSchema.tableName + "`(ID,EntityID,Date,Meta,`After`) VALUES(?,?,?,?,?)"
			data[1] = strconv.FormatUint(logTableSchema.uuid(orm), 10)
			data[2] = strconv.FormatUint(bind["ID"].(uint64), 10)
			data[3] = time.Now().Format(time.DateTime)
			if len(orm.meta) > 0 {
				asJSON, _ := jsoniter.ConfigFastest.MarshalToString(orm.meta)
				data[4] = asJSON
			} else {
				data[4] = nil
			}
			asJSON, _ := jsoniter.ConfigFastest.MarshalToString(bind)
			data[5] = asJSON
			publishAsyncEvent(logTableSchema, data)
		}
		if hasLocalCache {
			orm.flushPostActions = append(orm.flushPostActions, func(_ ORM) {
				lc.setEntity(orm, insert.ID(), insert.getEntity())
			})
		}
		for columnName := range schema.cachedReferences {
			id := bind[columnName]
			if id == nil {
				continue
			}
			refColumn := columnName
			if schema.hasLocalCache {
				orm.flushPostActions = append(orm.flushPostActions, func(_ ORM) {
					lc.removeList(orm, refColumn, id.(uint64))
				})
			}
			redisSetKey := schema.cacheKey + ":" + refColumn + ":" + strconv.FormatUint(id.(uint64), 10)
			orm.RedisPipeLine(schema.getForcedRedisCode()).SAdd(redisSetKey, strconv.FormatUint(insert.ID(), 10))
		}
		if schema.cacheAll {
			if schema.hasLocalCache {
				orm.flushPostActions = append(orm.flushPostActions, func(_ ORM) {
					lc.removeList(orm, cacheAllFakeReferenceKey, 0)
				})
			}
			redisSetKey := schema.cacheKey + ":" + cacheAllFakeReferenceKey
			orm.RedisPipeLine(schema.getForcedRedisCode()).SAdd(redisSetKey, strconv.FormatUint(insert.ID(), 10))
		}
		for indexName, def := range schema.cachedIndexes {
			indexAttributes := make([]any, len(def.Columns))
			for j, indexColumn := range def.Columns {
				indexAttributes[j] = bind[indexColumn]
			}
			key := indexName
			id := hashIndexAttributes(indexAttributes)
			if schema.hasLocalCache {
				orm.flushPostActions = append(orm.flushPostActions, func(_ ORM) {
					lc.removeList(orm, key, id)
				})
			}
			redisSetKey := schema.cacheKey + ":" + key + ":" + strconv.FormatUint(id, 10)
			orm.RedisPipeLine(schema.getForcedRedisCode()).SAdd(redisSetKey, strconv.FormatUint(insert.ID(), 10))
		}
		if hasRedisCache {
			idAsString := strconv.FormatUint(bind["ID"].(uint64), 10)
			orm.RedisPipeLine(rc.GetCode()).RPush(schema.getCacheKey()+":"+idAsString, convertBindToRedisValue(bind, schema)...)
		}
	}
	if !async {
		orm.appendDBAction(schema, func(db DBBase) {
			db.Exec(orm, sql, args...)
		})
	}

	return nil
}

func (orm *ormImplementation) handleUpdates(async bool, schema *entitySchema, operations []EntityFlush) error {
	var queryPrefix string
	for _, operation := range operations {
		update := operation.(entityFlushUpdate)
		newBind, oldBind, forcedNew, forcedOld, err := update.getBind()
		elem := update.getValue().Elem()
		if err != nil {
			return err
		}
		if len(newBind) == 0 {
			continue
		}
		if len(orm.engine.pluginFlush) > 0 {
			for _, p := range orm.engine.pluginFlush {
				after, err := p.EntityFlush(schema, elem, oldBind, newBind, orm.engine)
				if err != nil {
					return err
				}
				if after != nil {
					orm.flushPostActions = append(orm.flushPostActions, after)
				}
			}
		}
		if len(schema.cachedUniqueIndexes) > 0 {
			cache := orm.Engine().Redis(schema.getForcedRedisCode())
			for indexName, definition := range schema.cachedUniqueIndexes {
				indexChanged := false
				for _, column := range definition.Columns {
					_, changed := newBind[column]
					if changed {
						indexChanged = true
						break
					}
				}
				if !indexChanged {
					continue
				}
				hSetKey := schema.getCacheKey() + ":" + indexName
				hField, hasKey := buildUniqueKeyHSetField(schema, definition.Columns, newBind, forcedNew)
				if hasKey {
					orm.RedisPipeLine(cache.GetConfig().GetCode()).HSet(hSetKey, hField, strconv.FormatUint(update.ID(), 10))
				}
				hFieldOld, hasKey := buildUniqueKeyHSetField(schema, definition.Columns, oldBind, forcedOld)
				if hasKey {
					orm.RedisPipeLine(cache.GetConfig().GetCode()).HDel(hSetKey, hFieldOld)
				}
			}
		}

		if queryPrefix == "" {
			queryPrefix = "UPDATE `" + schema.GetTableName() + "` SET "
		}
		sql := queryPrefix
		k := 0
		var args []any
		var asyncArgs []any
		if async {
			asyncArgs = make([]any, len(newBind)+2)
		} else {
			args = make([]any, len(newBind)+1)
		}
		for column, value := range newBind {
			if k > 0 {
				sql += ","
			}
			sql += "`" + column + "`=?"
			if async {
				asUint64, isUint64 := value.(uint64)
				if isUint64 {
					value = strconv.FormatUint(asUint64, 10)
				}
				asyncArgs[k+1] = value
			} else {
				args[k] = value
			}
			k++
		}
		sql += " WHERE ID = ?"
		if async {
			asyncArgs[k+1] = strconv.FormatUint(update.ID(), 10)
		} else {
			args[k] = update.ID()
		}
		if async {
			asyncArgs[0] = sql
			publishAsyncEvent(schema, asyncArgs)
		} else {
			orm.appendDBAction(schema, func(db DBBase) {
				db.Exec(orm, sql, args...)
			})
		}

		logTableSchema, hasLogTable := orm.engine.registry.entityLogSchemas[schema.t]
		if hasLogTable {
			data := make([]any, 7)
			data[0] = "INSERT INTO `" + logTableSchema.tableName + "`(ID,EntityID,Date,Meta,`Before`,`After`) VALUES(?,?,?,?,?,?)"
			data[1] = strconv.FormatUint(logTableSchema.uuid(orm), 10)
			data[2] = strconv.FormatUint(update.ID(), 10)
			data[3] = time.Now().Format(time.DateTime)
			if len(orm.meta) > 0 {
				asJSON, _ := jsoniter.ConfigFastest.MarshalToString(orm.meta)
				data[4] = asJSON
			} else {
				data[4] = nil
			}
			asJSON, _ := jsoniter.ConfigFastest.MarshalToString(oldBind)
			data[5] = asJSON
			asJSON, _ = jsoniter.ConfigFastest.MarshalToString(newBind)
			data[6] = asJSON
			publishAsyncEvent(logTableSchema, data)
		}

		if update.getEntity() == nil {
			for field, newValue := range newBind {
				fSetter := schema.fieldSetters[field]
				if schema.hasLocalCache {
					func() {
						schema.localCache.mutex.Lock()
						defer schema.localCache.mutex.Unlock()
						fSetter(newValue, elem)
					}()
				} else {
					fSetter(newValue, elem)
				}
			}
		} else if schema.hasLocalCache {
			orm.flushPostActions = append(orm.flushPostActions, func(_ ORM) {
				sourceValue := update.getSourceValue()
				func() {
					schema.localCache.mutex.Lock()
					defer schema.localCache.mutex.Unlock()
					copyEntity(update.getValue().Elem(), sourceValue.Elem(), schema.fields, true)
				}()
				schema.localCache.setEntity(orm, operation.ID(), update.getEntity())
			})
		}

		if schema.hasRedisCache {
			p := orm.RedisPipeLine(schema.redisCache.GetCode())
			rKey := schema.getCacheKey() + ":" + strconv.FormatUint(update.ID(), 10)
			for column, val := range newBind {
				index := int64(schema.columnMapping[column] + 1)
				p.LSet(rKey, index, convertBindValueToRedisValue(val))
			}
		}
		for columnName := range schema.cachedReferences {
			id, has := newBind[columnName]
			if !has {
				continue
			}
			before := oldBind[columnName]
			refColumn := columnName

			newAsInt := uint64(0)
			oldAsInt := uint64(0)
			if id != nil {
				newAsInt, _ = id.(uint64)
			}
			if before != nil {
				oldAsInt, _ = before.(uint64)
			}
			if oldAsInt > 0 {
				if schema.hasLocalCache {
					orm.flushPostActions = append(orm.flushPostActions, func(_ ORM) {
						schema.localCache.removeList(orm, refColumn, oldAsInt)
					})
				}
				redisSetKey := schema.cacheKey + ":" + refColumn + ":" + strconv.FormatUint(oldAsInt, 10)
				orm.RedisPipeLine(schema.getForcedRedisCode()).SRem(redisSetKey, strconv.FormatUint(update.ID(), 10))
			}
			if newAsInt > 0 {
				if schema.hasLocalCache {
					orm.flushPostActions = append(orm.flushPostActions, func(_ ORM) {
						schema.localCache.removeList(orm, refColumn, newAsInt)
					})
				}
				redisSetKey := schema.cacheKey + ":" + refColumn + ":" + strconv.FormatUint(newAsInt, 10)
				orm.RedisPipeLine(schema.getForcedRedisCode()).SAdd(redisSetKey, strconv.FormatUint(update.ID(), 10))
			}
		}
		for indexName, def := range schema.cachedIndexes {
			indexChanged := false
			for _, indexColumn := range def.Columns {
				_, has := newBind[indexColumn]
				if has {
					indexChanged = true
					break
				}
			}
			if !indexChanged {
				continue
			}
			indexAttributes := make([]any, len(def.Columns))
			for j, indexColumn := range def.Columns {
				newVal, has := newBind[indexColumn]
				if !has {
					newVal, has = forcedNew[indexColumn]
				}
				indexAttributes[j] = newVal
				continue
			}
			key := indexName
			id := hashIndexAttributes(indexAttributes)
			if schema.hasLocalCache {
				orm.flushPostActions = append(orm.flushPostActions, func(_ ORM) {
					schema.localCache.removeList(orm, key, id)
				})
			}
			redisSetKey := schema.cacheKey + ":" + key + ":" + strconv.FormatUint(id, 10)
			idAsString := strconv.FormatUint(update.ID(), 10)
			orm.RedisPipeLine(schema.getForcedRedisCode()).SAdd(redisSetKey, idAsString)

			indexAttributes = indexAttributes[0:len(def.Columns)]
			for j, indexColumn := range def.Columns {
				oldVal, has := oldBind[indexColumn]
				if !has {
					oldVal, has = forcedOld[indexColumn]
				}
				indexAttributes[j] = oldVal
			}
			key2 := indexName
			id2 := hashIndexAttributes(indexAttributes)
			if schema.hasLocalCache {
				orm.flushPostActions = append(orm.flushPostActions, func(_ ORM) {
					schema.localCache.removeList(orm, key2, id2)
				})
			}
			redisSetKey = schema.cacheKey + ":" + key2 + ":" + strconv.FormatUint(id2, 10)
			orm.RedisPipeLine(schema.getForcedRedisCode()).SRem(redisSetKey, idAsString)
		}
	}
	return nil
}

func (orm *ormImplementation) groupSQLOperations() sqlOperations {
	sqlGroup := make(sqlOperations)
	orm.trackedEntities.Range(func(_ uint64, value *xsync.MapOf[uint64, EntityFlush]) bool {
		value.Range(func(_ uint64, flush EntityFlush) bool {
			schema := flush.Schema()
			db := orm.engine.DB(schema.mysqlPoolCode)
			poolSQLGroup, has := sqlGroup[db]
			if !has {
				poolSQLGroup = make(schemaSQLOperations)
				sqlGroup[db] = poolSQLGroup
			}
			tableSQLGroup, has := poolSQLGroup[schema]
			if !has {
				tableSQLGroup = make(map[FlushType][]EntityFlush)
				poolSQLGroup[schema] = tableSQLGroup
			}
			tableSQLGroup[flush.flushType()] = append(tableSQLGroup[flush.flushType()], flush)
			return true
		})
		return true
	})
	return sqlGroup
}

func (orm *ormImplementation) appendDBAction(schema EntitySchema, action dbAction) {
	if orm.flushDBActions == nil {
		orm.flushDBActions = make(map[string][]dbAction)
	}
	poolCode := schema.GetDB().GetConfig().GetCode()
	orm.flushDBActions[poolCode] = append(orm.flushDBActions[poolCode], action)
}

func buildUniqueKeyHSetField(schema *entitySchema, indexColumns []string, bind, forced Bind) (string, bool) {
	hField := ""
	hasNil := false
	hasInBind := false
	for _, column := range indexColumns {
		bindValue, has := bind[column]
		if !has {
			bindValue, has = forced[column]
		}
		if bindValue == nil {
			hasNil = true
			break
		}
		if has {
			hasInBind = true
		}
		asString, err := schema.columnAttrToStringSetters[column](bindValue, true)
		if err != nil {
			panic(err)
		}
		hField += asString
	}
	if hasNil || !hasInBind {
		return "", false
	}
	return hashString(hField), true
}
