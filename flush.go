package beeorm

import (
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
)

type entitySQLOperations map[FlushType][]EntityFlush
type schemaSQLOperations map[*entitySchema]entitySQLOperations
type sqlOperations map[DB]schemaSQLOperations

func (c *contextImplementation) Flush() error {
	return c.flush(false)
}

func (c *contextImplementation) FlushAsync() error {
	return c.flush(true)
}

func (c *contextImplementation) flush(async bool) error {
	if len(c.trackedEntities) == 0 {
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
		action()
	}
	c.ClearFlush()
	return nil
}

func (c *contextImplementation) ClearFlush() {
	c.trackedEntities = c.trackedEntities[0:0]
	c.flushDBActions = nil
	c.flushPostActions = c.flushPostActions[0:0]
	c.redisPipeLines = nil
}

func (c *contextImplementation) handleDeletes(async bool, schema *entitySchema, operations []EntityFlush) error {
	var args []any
	if !async {
		args = make([]any, len(operations))
	}
	s := c.getStringBuilder2()
	s.WriteString("DELETE FROM `")
	s.WriteString(schema.GetTableName())
	s.WriteString("` WHERE ID IN (")
	if async {
		for i, operation := range operations {
			if i > 0 {
				s.WriteString(",")
			}
			s.WriteString(strconv.FormatUint(operation.ID(), 10))
		}
	} else {
		s.WriteString("?")
		s.WriteString(strings.Repeat(",?", len(operations)-1))
	}
	s.WriteString(")")
	sql := s.String()
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
				hField, hasKey := buildUniqueKeyHSetField(indexColumns, bind)
				if hasKey {
					c.RedisPipeLine(cache.GetConfig().GetCode()).HDel(hSetKey, hField)
				}
			}
		}
		if hasLocalCache {
			c.flushPostActions = append(c.flushPostActions, func() {
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
			if id == nullAsString {
				continue
			}
			refColumn := columnName
			if schema.hasLocalCache {
				c.flushPostActions = append(c.flushPostActions, func() {
					idAsInt, _ := strconv.ParseUint(id, 10, 64)
					lc.removeReference(c, refColumn, idAsInt)
				})
			}
			redisSetKey := schema.cacheKey + ":" + refColumn + ":" + id
			c.RedisPipeLine(schema.getForcedRedisCode()).SRem(redisSetKey, strconv.FormatUint(deleteFlush.ID(), 10))
		}
		if schema.cacheAll {
			if schema.hasLocalCache {
				c.flushPostActions = append(c.flushPostActions, func() {
					lc.removeReference(c, cacheAllFakeReferenceKey, 0)
				})
			}
			redisSetKey := schema.cacheKey + ":" + cacheAllFakeReferenceKey
			c.RedisPipeLine(schema.getForcedRedisCode()).SRem(redisSetKey, strconv.FormatUint(deleteFlush.ID(), 10))
		}
		logTableSchema, hasLogTable := c.engine.registry.entityLogSchemas[schema.t]
		if hasLogTable {
			data := make([]string, 6)
			data[0] = "INSERT INTO `" + logTableSchema.tableName + "`(ID,EntityID,Date,Meta,`Before`) VALUES(?,?,?,?,?)"
			data[1] = strconv.FormatUint(logTableSchema.uuid(), 10)
			data[2] = strconv.FormatUint(operation.ID(), 10)
			data[3] = time.Now().Format(time.DateTime)
			if len(c.meta) > 0 {
				asJSON, _ := jsoniter.ConfigFastest.MarshalToString(c.meta)
				data[4] = asJSON
			} else {
				data[4] = nullAsString
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
	}
	return nil
}

func (c *contextImplementation) handleInserts(async bool, schema *entitySchema, operations []EntityFlush) error {
	columns := schema.GetColumns()
	s := c.getStringBuilder2()
	s.WriteString("INSERT INTO `")
	s.WriteString(schema.GetTableName())
	s.WriteString("`(`ID`")
	for _, column := range columns[1:] {
		s.WriteString(",`")
		s.WriteString(column)
		s.WriteString("`")
	}
	s.WriteString(") VALUES")
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
		uniqueIndexes := schema.GetUniqueIndexes()
		if len(uniqueIndexes) > 0 {
			cache := c.Engine().Redis(schema.getForcedRedisCode())
			for indexName, indexColumns := range uniqueIndexes {
				hSetKey := schema.getCacheKey() + ":" + indexName
				hField, hasKey := buildUniqueKeyHSetField(indexColumns, bind)
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
		var asyncData []string
		if async {
			asyncData = make([]string, len(columns))
		}

		if i > 0 && !async {
			s.WriteString(",")
		}
		if !async || i == 0 {
			s.WriteString("(?")
		}
		if !async {
			args = append(args, bind["ID"])
		} else {
			asyncData[0] = bind["ID"]
		}
		for j, column := range columns[1:] {
			v := bind[column]
			if !async {
				if v == nullAsString {
					args = append(args, nil)
				} else {
					args = append(args, v)
				}
			} else {
				asyncData[j+1] = v
			}
			if !async || i == 0 {
				s.WriteString(",?")
			}
		}
		if !async || i == 0 {
			s.WriteString(")")
		}
		if async {
			data := make([]string, 0, len(asyncData)+1)
			data = append(data, s.String())
			data = append(data, asyncData...)
			asJSON, _ := jsoniter.ConfigFastest.MarshalToString(data)
			c.RedisPipeLine(schema.getForcedRedisCode()).RPush(schema.asyncCacheKey, asJSON)
		}
		logTableSchema, hasLogTable := c.engine.registry.entityLogSchemas[schema.t]
		if hasLogTable {
			data := make([]string, 6)
			data[0] = "INSERT INTO `" + logTableSchema.tableName + "`(ID,EntityID,Date,Meta,`After`) VALUES(?,?,?,?,?)"
			data[1] = strconv.FormatUint(logTableSchema.uuid(), 10)
			data[2] = bind["ID"]
			data[3] = time.Now().Format(time.DateTime)
			if len(c.meta) > 0 {
				asJSON, _ := jsoniter.ConfigFastest.MarshalToString(c.meta)
				data[4] = asJSON
			} else {
				data[4] = nullAsString
			}
			asJSON, _ := jsoniter.ConfigFastest.MarshalToString(bind)
			data[5] = asJSON
			asJSON, _ = jsoniter.ConfigFastest.MarshalToString(data)
			c.RedisPipeLine(schema.getForcedRedisCode()).RPush(logTableSchema.asyncCacheKey, asJSON)
		}
		if hasLocalCache {
			c.flushPostActions = append(c.flushPostActions, func() {
				lc.setEntity(c, insert.ID(), insert.getEntity())
			})
		}
		for columnName := range schema.cachedReferences {
			id := bind[columnName]
			if id == nullAsString {
				continue
			}
			refColumn := columnName
			if schema.hasLocalCache {
				c.flushPostActions = append(c.flushPostActions, func() {
					idAsInt, _ := strconv.ParseUint(id, 10, 64)
					lc.removeReference(c, refColumn, idAsInt)
				})
			}
			redisSetKey := schema.cacheKey + ":" + refColumn + ":" + id
			c.RedisPipeLine(schema.getForcedRedisCode()).SAdd(redisSetKey, strconv.FormatUint(insert.ID(), 10))
		}
		if schema.cacheAll {
			if schema.hasLocalCache {
				c.flushPostActions = append(c.flushPostActions, func() {
					lc.removeReference(c, cacheAllFakeReferenceKey, 0)
				})
			}
			redisSetKey := schema.cacheKey + ":" + cacheAllFakeReferenceKey
			c.RedisPipeLine(schema.getForcedRedisCode()).SAdd(redisSetKey, strconv.FormatUint(insert.ID(), 10))
		}
		if hasRedisCache {
			c.RedisPipeLine(rc.GetCode()).RPush(schema.getCacheKey()+":"+bind["ID"], convertBindToRedisValue(bind, schema)...)
		}
	}
	if !async {
		sql := s.String()
		c.appendDBAction(schema, func(db DBBase) {
			db.Exec(c, sql, args...)
		})
	}

	return nil
}

func (c *contextImplementation) handleUpdates(async bool, schema *entitySchema, operations []EntityFlush) error {
	var queryPrefix string
	lc, hasLocalCache := schema.GetLocalCache()
	rc, hasRedisCache := schema.GetRedisCache()
	for _, operation := range operations {
		update := operation.(entityFlushUpdate)
		newBind, oldBind, err := update.getBind()
		if err != nil {
			return err
		}
		if len(newBind) == 0 {
			continue
		}
		uniqueIndexes := schema.GetUniqueIndexes()
		if len(uniqueIndexes) > 0 {
			cache, hasRedis := schema.GetRedisCache()
			if !hasRedis {
				cache = c.Engine().Redis(DefaultPoolCode)
			}
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
				hField, hasKey := buildUniqueKeyHSetField(indexColumns, newBind)
				if hasKey {
					previousID, inUse := cache.HGet(c, hSetKey, hField)
					if inUse {
						idAsUint, _ := strconv.ParseUint(previousID, 10, 64)
						return &DuplicatedKeyBindError{Index: indexName, ID: idAsUint, Columns: indexColumns}
					}
					c.RedisPipeLine(cache.GetConfig().GetCode()).HSet(hSetKey, hField, strconv.FormatUint(update.ID(), 10))
				}
				hFieldOld, hasKey := buildUniqueKeyHSetField(indexColumns, oldBind)
				if hasKey {
					c.RedisPipeLine(cache.GetConfig().GetCode()).HDel(hSetKey, hFieldOld)
				}
			}
		}

		if queryPrefix == "" {
			s := c.getStringBuilder2()
			s.WriteString("UPDATE `")
			s.WriteString(schema.GetTableName())
			s.WriteString("` SET ")
			queryPrefix = s.String()
		}
		s := c.getStringBuilder2()
		s.WriteString(queryPrefix)
		k := 0
		var args []any
		var asyncArgs []string
		if async {
			asyncArgs = make([]string, len(newBind)+2)
		} else {
			args = make([]any, len(newBind)+1)
		}
		for column, value := range newBind {
			if k > 0 {
				s.WriteString(",")
			}
			s.WriteString("`" + column + "`=?")
			if async {
				asyncArgs[k+1] = value
			} else {
				if value == nullAsString {
					args[k] = nil
				} else {
					args[k] = value
				}
			}
			k++
		}
		s.WriteString(" WHERE ID = ?")
		if async {
			asyncArgs[k+1] = strconv.FormatUint(update.ID(), 10)
		} else {
			args[k] = update.ID()
		}
		sql := s.String()
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
			data := make([]string, 7)
			data[0] = "INSERT INTO `" + logTableSchema.tableName + "`(ID,EntityID,Date,Meta,`Before`,`After`) VALUES(?,?,?,?,?,?)"
			data[1] = strconv.FormatUint(logTableSchema.uuid(), 10)
			data[2] = strconv.FormatUint(update.ID(), 10)
			data[3] = time.Now().Format(time.DateTime)
			if len(c.meta) > 0 {
				asJSON, _ := jsoniter.ConfigFastest.MarshalToString(c.meta)
				data[4] = asJSON
			} else {
				data[4] = nullAsString
			}
			asJSON, _ := jsoniter.ConfigFastest.MarshalToString(oldBind)
			data[5] = asJSON
			asJSON, _ = jsoniter.ConfigFastest.MarshalToString(newBind)
			data[6] = asJSON
			asJSON, _ = jsoniter.ConfigFastest.MarshalToString(data)
			c.RedisPipeLine(schema.getForcedRedisCode()).RPush(logTableSchema.asyncCacheKey, asJSON)
		}

		if hasLocalCache {
			c.flushPostActions = append(c.flushPostActions, func() {
				sourceValue := update.getSourceValue()
				copyEntity(update.getValue().Elem(), sourceValue.Elem(), schema.fields)
				lc.setEntity(c, operation.ID(), update.getEntity())
			})
		}
		if hasRedisCache {
			p := c.RedisPipeLine(rc.GetCode())
			for column, val := range newBind {
				index := int64(schema.columnMapping[column] + 1)
				p.LSet(schema.getCacheKey()+":"+strconv.FormatUint(update.ID(), 10), index, convertBindValueToRedisValue(val))
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
			if id != nullAsString {
				newAsInt, _ = strconv.ParseUint(id, 10, 64)
			}
			if before != nullAsString {
				oldAsInt, _ = strconv.ParseUint(before, 10, 64)
			}
			if oldAsInt > 0 {
				if hasLocalCache {
					c.flushPostActions = append(c.flushPostActions, func() {
						lc.removeReference(c, refColumn, oldAsInt)
					})
				}
				redisSetKey := schema.cacheKey + ":" + refColumn + ":" + before
				c.RedisPipeLine(schema.getForcedRedisCode()).SRem(redisSetKey, strconv.FormatUint(update.ID(), 10))
			}
			if newAsInt > 0 {
				if hasLocalCache {
					c.flushPostActions = append(c.flushPostActions, func() {
						lc.removeReference(c, refColumn, newAsInt)
					})
				}
				redisSetKey := schema.cacheKey + ":" + refColumn + ":" + id
				c.RedisPipeLine(schema.getForcedRedisCode()).SAdd(redisSetKey, strconv.FormatUint(update.ID(), 10))
			}
		}
	}
	return nil
}

func (c *contextImplementation) groupSQLOperations() sqlOperations {
	sqlGroup := make(sqlOperations)
	for _, val := range c.trackedEntities {
		schema := val.Schema()
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
		tableSQLGroup[val.flushType()] = append(tableSQLGroup[val.flushType()], val)
	}
	return sqlGroup
}

func (c *contextImplementation) appendDBAction(schema EntitySchema, action func(db DBBase)) {
	if c.flushDBActions == nil {
		c.flushDBActions = make(map[string][]func(db DBBase))
	}
	poolCode := schema.GetDB().GetConfig().GetCode()
	c.flushDBActions[poolCode] = append(c.flushDBActions[poolCode], action)
}

func buildUniqueKeyHSetField(indexColumns []string, bind Bind) (string, bool) {
	hField := ""
	hasNil := false
	hasInBind := false
	for _, column := range indexColumns {
		bindValue, has := bind[column]
		if bindValue == nullAsString {
			hasNil = true
			break
		}
		if has {
			hasInBind = true
		}
		hField += bindValue
	}
	if hasNil || !hasInBind {
		return "", false
	}
	return hashString(hField), true
}
