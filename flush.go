package beeorm

import (
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
type PostFlushAction func(c Context)

func (c *contextImplementation) Flush() error {
	return c.flush(false)
}

func (c *contextImplementation) FlushAsync() error {
	return c.flush(true)
}

func (c *contextImplementation) flush(async bool) error {
	c.mutexFlush.Lock()
	defer c.mutexFlush.Unlock()
	if c.trackedEntities == nil || c.trackedEntities.Size() == 0 {
		return nil
	}
	sqlGroup := c.groupSQLOperations()
	for _, operations := range sqlGroup {
		for schema, queryOperations := range operations {
			deletes, has := queryOperations[Delete]
			if has {
				err := c.handleDeletes(async, schema, deletes)
				if err != nil {
					return err
				}
			}
			inserts, has := queryOperations[Insert]
			if has {
				err := c.handleInserts(async, schema, inserts)
				if err != nil {
					return err
				}
			}
			updates, has := queryOperations[Update]
			if has {
				err := c.handleUpdates(async, schema, updates)
				if err != nil {
					return err
				}
			}
		}
	}
	if !async {
		func() {
			var transactions []DBTransaction
			defer func() {
				for _, tx := range transactions {
					tx.Rollback(c)
				}
			}()
			for code, actions := range c.flushDBActions {
				var d DBBase
				d = c.Engine().DB(code)
				if len(actions) > 1 || len(c.flushDBActions) > 1 {
					tx := d.(DB).Begin(c)
					transactions = append(transactions, tx)
					d = tx
				}
				for _, action := range actions {
					action(d)
				}
			}
			for _, tx := range transactions {
				tx.Commit(c)
			}
		}()
	}
	for _, pipeline := range c.redisPipeLines {
		pipeline.Exec(c)
	}
	for _, action := range c.flushPostActions {
		action(c)
	}
	c.trackedEntities.Clear()
	c.flushDBActions = nil
	c.flushPostActions = c.flushPostActions[0:0]
	c.redisPipeLines = nil
	return nil
}

func (c *contextImplementation) ClearFlush() {
	c.mutexFlush.Lock()
	defer c.mutexFlush.Unlock()
	c.trackedEntities.Clear()
	c.flushDBActions = nil
	c.flushPostActions = c.flushPostActions[0:0]
	c.redisPipeLines = nil
}

func (c *contextImplementation) handleDeletes(async bool, schema *entitySchema, operations []EntityFlush) error {
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
		c.appendDBAction(schema, func(db DBBase) {
			db.Exec(c, sql, args...)
		})
	} else {
		data := `["` + sql + `"]"`
		c.RedisPipeLine(schema.getForcedRedisCode()).RPush(schema.asyncCacheKey, data)
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
			cache := c.Engine().Redis(schema.getForcedRedisCode())
			for indexName, indexColumns := range uniqueIndexes {
				hSetKey := schema.getCacheKey() + ":" + indexName
				hField, hasKey := buildUniqueKeyHSetField(schema, indexColumns, bind)
				if hasKey {
					c.RedisPipeLine(cache.GetConfig().GetCode()).HDel(hSetKey, hField)
				}
			}
		}
		if hasLocalCache {
			c.flushPostActions = append(c.flushPostActions, func(_ Context) {
				lc.setEntity(c, operation.ID(), nil)
			})
		}
		rc, hasRedisCache := schema.GetRedisCache()
		if hasRedisCache {
			cacheKey := schema.getCacheKey() + ":" + strconv.FormatUint(operation.ID(), 10)
			c.RedisPipeLine(rc.GetCode()).Del(cacheKey)
			c.RedisPipeLine(rc.GetCode()).LPush(cacheKey, "")
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
				c.flushPostActions = append(c.flushPostActions, func(_ Context) {
					lc.removeReference(c, refColumn, id.(uint64))
				})
			}
			idAsString := strconv.FormatUint(id.(uint64), 10)
			redisSetKey := schema.cacheKey + ":" + refColumn + ":" + idAsString
			c.RedisPipeLine(schema.getForcedRedisCode()).SRem(redisSetKey, strconv.FormatUint(deleteFlush.ID(), 10))
		}
		if schema.cacheAll {
			if schema.hasLocalCache {
				c.flushPostActions = append(c.flushPostActions, func(_ Context) {
					lc.removeReference(c, cacheAllFakeReferenceKey, 0)
				})
			}
			redisSetKey := schema.cacheKey + ":" + cacheAllFakeReferenceKey
			c.RedisPipeLine(schema.getForcedRedisCode()).SRem(redisSetKey, strconv.FormatUint(deleteFlush.ID(), 10))
		}
		logTableSchema, hasLogTable := c.engine.registry.entityLogSchemas[schema.t]
		if hasLogTable {
			data := make([]any, 6)
			data[0] = "INSERT INTO `" + logTableSchema.tableName + "`(ID,EntityID,Date,Meta,`Before`) VALUES(?,?,?,?,?)"
			data[1] = strconv.FormatUint(logTableSchema.uuid(), 10)
			data[2] = strconv.FormatUint(operation.ID(), 10)
			data[3] = time.Now().Format(time.DateTime)
			if len(c.meta) > 0 {
				asJSON, _ := jsoniter.ConfigFastest.MarshalToString(c.meta)
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
			asJSON, _ = jsoniter.ConfigFastest.MarshalToString(data)
			c.RedisPipeLine(schema.getForcedRedisCode()).RPush(logTableSchema.asyncCacheKey, asJSON)
		}
		for _, p := range c.engine.pluginFlush {
			if bind == nil {
				bind, err = deleteFlush.getOldBind()
				if err != nil {
					return err
				}
			}
			after, err := p.EntityFlush(schema, deleteFlush.getValue(), bind, nil, c.engine)
			if err != nil {
				return err
			}
			if after != nil {
				c.flushPostActions = append(c.flushPostActions, after)
			}
		}
	}
	return nil
}

func (c *contextImplementation) handleInserts(async bool, schema *entitySchema, operations []EntityFlush) error {
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
		if len(c.engine.pluginFlush) > 0 {
			elem := insert.getValue().Elem()
			for _, p := range c.engine.pluginFlush {
				after, err := p.EntityFlush(schema, elem, nil, bind, c.engine)
				if err != nil {
					return err
				}
				if after != nil {
					c.flushPostActions = append(c.flushPostActions, after)
				}
			}
		}
		uniqueIndexes := schema.GetUniqueIndexes()
		if len(uniqueIndexes) > 0 {
			cache := c.Engine().Redis(schema.getForcedRedisCode())
			for indexName, indexColumns := range uniqueIndexes {
				hSetKey := schema.getCacheKey() + ":" + indexName
				hField, hasKey := buildUniqueKeyHSetField(schema, indexColumns, bind)
				if !hasKey {
					continue
				}
				previousID, inUse := cache.HGet(c, hSetKey, hField)
				if inUse {
					idAsUint, _ := strconv.ParseUint(previousID, 10, 64)
					return &DuplicatedKeyBindError{Index: indexName, ID: idAsUint, Columns: indexColumns}
				}
				c.RedisPipeLine(cache.GetConfig().GetCode()).HSet(hSetKey, hField, strconv.FormatUint(insert.ID(), 10))
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
			asJSON, _ := jsoniter.ConfigFastest.MarshalToString(asyncData)
			c.RedisPipeLine(schema.getForcedRedisCode()).RPush(schema.asyncCacheKey, asJSON)
		}
		logTableSchema, hasLogTable := c.engine.registry.entityLogSchemas[schema.t]
		if hasLogTable {
			data := make([]any, 6)
			data[0] = "INSERT INTO `" + logTableSchema.tableName + "`(ID,EntityID,Date,Meta,`After`) VALUES(?,?,?,?,?)"
			data[1] = strconv.FormatUint(logTableSchema.uuid(), 10)
			data[2] = strconv.FormatUint(bind["ID"].(uint64), 10)
			data[3] = time.Now().Format(time.DateTime)
			if len(c.meta) > 0 {
				asJSON, _ := jsoniter.ConfigFastest.MarshalToString(c.meta)
				data[4] = asJSON
			} else {
				data[4] = nil
			}
			asJSON, _ := jsoniter.ConfigFastest.MarshalToString(bind)
			data[5] = asJSON
			asJSON, _ = jsoniter.ConfigFastest.MarshalToString(data)
			c.RedisPipeLine(schema.getForcedRedisCode()).RPush(logTableSchema.asyncCacheKey, asJSON)
		}
		if hasLocalCache {
			c.flushPostActions = append(c.flushPostActions, func(_ Context) {
				lc.setEntity(c, insert.ID(), insert.getEntity())
			})
		}
		for columnName := range schema.cachedReferences {
			id := bind[columnName]
			if id == nil {
				continue
			}
			refColumn := columnName
			if schema.hasLocalCache {
				c.flushPostActions = append(c.flushPostActions, func(_ Context) {
					lc.removeReference(c, refColumn, id.(uint64))
				})
			}
			redisSetKey := schema.cacheKey + ":" + refColumn + ":" + strconv.FormatUint(id.(uint64), 10)
			c.RedisPipeLine(schema.getForcedRedisCode()).SAdd(redisSetKey, strconv.FormatUint(insert.ID(), 10))
		}
		if schema.cacheAll {
			if schema.hasLocalCache {
				c.flushPostActions = append(c.flushPostActions, func(_ Context) {
					lc.removeReference(c, cacheAllFakeReferenceKey, 0)
				})
			}
			redisSetKey := schema.cacheKey + ":" + cacheAllFakeReferenceKey
			c.RedisPipeLine(schema.getForcedRedisCode()).SAdd(redisSetKey, strconv.FormatUint(insert.ID(), 10))
		}
		if hasRedisCache {
			idAsString := strconv.FormatUint(bind["ID"].(uint64), 10)
			c.RedisPipeLine(rc.GetCode()).RPush(schema.getCacheKey()+":"+idAsString, convertBindToRedisValue(bind, schema)...)
		}
	}
	if !async {
		c.appendDBAction(schema, func(db DBBase) {
			db.Exec(c, sql, args...)
		})
	}

	return nil
}

func (c *contextImplementation) handleUpdates(async bool, schema *entitySchema, operations []EntityFlush) error {
	var queryPrefix string
	for _, operation := range operations {
		update := operation.(entityFlushUpdate)
		newBind, oldBind, err := update.getBind()
		if err != nil {
			return err
		}
		if len(newBind) == 0 {
			continue
		}
		if len(c.engine.pluginFlush) > 0 {
			elem := update.getValue().Elem()
			for _, p := range c.engine.pluginFlush {
				after, err := p.EntityFlush(schema, elem, oldBind, newBind, c.engine)
				if err != nil {
					return err
				}
				if after != nil {
					c.flushPostActions = append(c.flushPostActions, after)
				}
			}
		}
		uniqueIndexes := schema.GetUniqueIndexes()
		if len(uniqueIndexes) > 0 {
			cache := c.Engine().Redis(schema.getForcedRedisCode())
			for indexName, indexColumns := range uniqueIndexes {
				indexChanged := false
				for _, column := range indexColumns {
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
				hField, hasKey := buildUniqueKeyHSetField(schema, indexColumns, newBind)
				if hasKey {
					previousID, inUse := cache.HGet(c, hSetKey, hField)
					if inUse {
						idAsUint, _ := strconv.ParseUint(previousID, 10, 64)
						return &DuplicatedKeyBindError{Index: indexName, ID: idAsUint, Columns: indexColumns}
					}
					c.RedisPipeLine(cache.GetConfig().GetCode()).HSet(hSetKey, hField, strconv.FormatUint(update.ID(), 10))
				}
				hFieldOld, hasKey := buildUniqueKeyHSetField(schema, indexColumns, oldBind)
				if hasKey {
					c.RedisPipeLine(cache.GetConfig().GetCode()).HDel(hSetKey, hFieldOld)
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
			asJSON, _ := jsoniter.ConfigFastest.MarshalToString(asyncArgs)
			c.RedisPipeLine(schema.getForcedRedisCode()).RPush(schema.asyncCacheKey, asJSON)
		} else {
			c.appendDBAction(schema, func(db DBBase) {
				db.Exec(c, sql, args...)
			})
		}

		logTableSchema, hasLogTable := c.engine.registry.entityLogSchemas[schema.t]
		if hasLogTable {
			data := make([]any, 7)
			data[0] = "INSERT INTO `" + logTableSchema.tableName + "`(ID,EntityID,Date,Meta,`Before`,`After`) VALUES(?,?,?,?,?,?)"
			data[1] = strconv.FormatUint(logTableSchema.uuid(), 10)
			data[2] = strconv.FormatUint(update.ID(), 10)
			data[3] = time.Now().Format(time.DateTime)
			if len(c.meta) > 0 {
				asJSON, _ := jsoniter.ConfigFastest.MarshalToString(c.meta)
				data[4] = asJSON
			} else {
				data[4] = nil
			}
			asJSON, _ := jsoniter.ConfigFastest.MarshalToString(oldBind)
			data[5] = asJSON
			asJSON, _ = jsoniter.ConfigFastest.MarshalToString(newBind)
			data[6] = asJSON
			asJSON, _ = jsoniter.ConfigFastest.MarshalToString(data)
			c.RedisPipeLine(schema.getForcedRedisCode()).RPush(logTableSchema.asyncCacheKey, asJSON)
		}

		if schema.hasLocalCache {
			c.flushPostActions = append(c.flushPostActions, func(_ Context) {
				sourceValue := update.getSourceValue()
				func() {
					schema.localCache.mutex.Lock()
					defer schema.localCache.mutex.Unlock()
					copyEntity(update.getValue().Elem(), sourceValue.Elem(), schema.fields)
				}()
				schema.localCache.setEntity(c, operation.ID(), update.getEntity())
			})
		}
		if schema.hasRedisCache {
			p := c.RedisPipeLine(schema.redisCache.GetCode())
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
					c.flushPostActions = append(c.flushPostActions, func(_ Context) {
						schema.localCache.removeReference(c, refColumn, oldAsInt)
					})
				}
				redisSetKey := schema.cacheKey + ":" + refColumn + ":" + strconv.FormatUint(oldAsInt, 10)
				c.RedisPipeLine(schema.getForcedRedisCode()).SRem(redisSetKey, strconv.FormatUint(update.ID(), 10))
			}
			if newAsInt > 0 {
				if schema.hasLocalCache {
					c.flushPostActions = append(c.flushPostActions, func(_ Context) {
						schema.localCache.removeReference(c, refColumn, newAsInt)
					})
				}
				redisSetKey := schema.cacheKey + ":" + refColumn + ":" + strconv.FormatUint(newAsInt, 10)
				c.RedisPipeLine(schema.getForcedRedisCode()).SAdd(redisSetKey, strconv.FormatUint(update.ID(), 10))
			}
		}
	}
	return nil
}

func (c *contextImplementation) groupSQLOperations() sqlOperations {
	sqlGroup := make(sqlOperations)
	c.trackedEntities.Range(func(_ uint64, value *xsync.MapOf[uint64, EntityFlush]) bool {
		value.Range(func(_ uint64, flush EntityFlush) bool {
			schema := flush.Schema()
			db := c.engine.DB(schema.mysqlPoolCode)
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

func (c *contextImplementation) appendDBAction(schema EntitySchema, action dbAction) {
	if c.flushDBActions == nil {
		c.flushDBActions = make(map[string][]dbAction)
	}
	poolCode := schema.GetDB().GetConfig().GetCode()
	c.flushDBActions[poolCode] = append(c.flushDBActions[poolCode], action)
}

func buildUniqueKeyHSetField(schema *entitySchema, indexColumns []string, bind Bind) (string, bool) {
	hField := ""
	hasNil := false
	hasInBind := false
	for _, column := range indexColumns {
		bindValue, has := bind[column]
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
