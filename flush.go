package beeorm

import (
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"strconv"
	"strings"
)

type entitySqlOperations map[FlushType][]EntityFlush
type schemaSqlOperations map[*entitySchema]entitySqlOperations
type sqlOperations map[DB]schemaSqlOperations

func (c *contextImplementation) Flush(lazy bool) error {
	return c.flush(lazy)
}

func (c *contextImplementation) flush(lazy bool) error {
	if len(c.trackedEntities) == 0 {
		return nil
	}
	sqlGroup := c.groupSQLOperations()
	for _, operations := range sqlGroup {
		for schema, queryOperations := range operations {
			deletes, has := queryOperations[Delete]
			if has {
				err := c.handleDeletes(lazy, schema, deletes)
				if err != nil {
					return err
				}
			}
			inserts, has := queryOperations[Insert]
			if has {
				err := c.handleInserts(lazy, schema, inserts)
				if err != nil {
					return err
				}
			}
			updates, has := queryOperations[Update]
			if has {
				err := c.handleUpdates(lazy, schema, updates)
				if err != nil {
					return err
				}
			}
		}
	}
	if !lazy {
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
	for _, action := range c.flushPostActions {
		action()
	}
	for _, pipeline := range c.redisPipeLines {
		pipeline.Exec(c)
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

func (c *contextImplementation) handleDeletes(lazy bool, schema *entitySchema, operations []EntityFlush) error {
	var args []interface{}
	if !lazy {
		args = make([]interface{}, len(operations))
	}
	s := c.getStringBuilder2()
	s.WriteString("DELETE FROM `")
	s.WriteString(schema.GetTableName())
	s.WriteString("` WHERE ID IN (")
	if lazy {
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
	if !lazy {
		for i, operation := range operations {
			args[i] = operation.ID()
		}
		c.appendDBAction(schema, func(db DBBase) {
			db.Exec(c, sql, args...)
		})
	} else {
		data := `["` + sql + `"]"`
		c.RedisPipeLine(schema.getLazyRedisCode()).RPush(schema.lazyCacheKey, data)
	}

	lc, hasLocalCache := schema.GetLocalCache()
	for _, operation := range operations {
		uniqueIndexes := schema.GetUniqueIndexes()
		if len(uniqueIndexes) > 0 {
			deleteFlush := operation.(entityFlushDelete)
			bind, err := deleteFlush.getOldBind()
			if err != nil {
				return err
			}
			cache, hasRedis := schema.GetRedisCache()
			if !hasRedis {
				cache = c.Engine().Redis(DefaultPoolCode)
			}
			for indexName, indexColumns := range uniqueIndexes {
				hSetKey := schema.GetCacheKey() + ":" + indexName
				hField, hasKey := buildUniqueKeyHSetField(indexColumns, bind)
				if hasKey {
					c.RedisPipeLine(cache.GetPoolConfig().GetCode()).HDel(hSetKey, hField)
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
			cacheKey := schema.GetCacheKey() + ":" + strconv.FormatUint(operation.ID(), 10)
			c.RedisPipeLine(rc.GetCode()).Del(cacheKey)
			c.RedisPipeLine(rc.GetCode()).LPush(cacheKey, "")
		}
	}
	return nil
}

func (c *contextImplementation) handleInserts(lazy bool, schema *entitySchema, operations []EntityFlush) error {
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
	var args []interface{}
	if !lazy {
		args = make([]interface{}, 0, len(operations)*len(columns))
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
			cache, hasRedis := schema.GetRedisCache()
			if !hasRedis {
				cache = c.Engine().Redis(DefaultPoolCode)
			}
			for indexName, indexColumns := range uniqueIndexes {
				hSetKey := schema.GetCacheKey() + ":" + indexName
				hField, hasKey := buildUniqueKeyHSetField(indexColumns, bind)
				if !hasKey {
					continue
				}
				previousID, inUse := cache.HGet(c, hSetKey, hField)
				if inUse {
					idAsUint, _ := strconv.ParseUint(previousID, 10, 64)
					return &DuplicatedKeyBindError{Index: indexName, ID: idAsUint, Columns: indexColumns}
				}
				c.RedisPipeLine(cache.GetPoolConfig().GetCode()).HSet(hSetKey, hField, strconv.FormatUint(insert.ID(), 10))
			}
		}
		var lazyData []string
		if lazy {
			lazyData = make([]string, len(columns))
		}

		if i > 0 && !lazy {
			s.WriteString(",")
		}
		if !lazy || i == 0 {
			s.WriteString("(?")
		}
		if !lazy {
			args = append(args, bind["ID"])
		} else {
			lazyData[0] = bind["ID"]
		}
		for j, column := range columns[1:] {
			v := bind[column]
			if !lazy {
				if v == nullAsString {
					args = append(args, nil)
				} else {
					args = append(args, v)
				}
			} else {
				lazyData[j+1] = v
			}
			if !lazy || i == 0 {
				s.WriteString(",?")
			}
		}
		if !lazy || i == 0 {
			s.WriteString(")")
		}
		if lazy {
			data := make([]string, 0, len(lazyData)+1)
			data = append(data, s.String())
			data = append(data, lazyData...)
			asJson, _ := jsoniter.ConfigFastest.MarshalToString(data)
			c.RedisPipeLine(schema.getLazyRedisCode()).RPush(schema.lazyCacheKey, asJson)
		}
		if hasLocalCache {
			c.flushPostActions = append(c.flushPostActions, func() {
				lc.setEntity(c, insert.ID(), insert.getEntity())
			})
			for columnName := range schema.cachedReferences {
				id := bind[columnName]
				if id == nullAsString {
					continue
				}
				//zapisanie do references cache
				lc.setReference(c, id, "TODO")
				fmt.Printf("YES %s\n", cacheKey)
			}

		}
		if hasRedisCache {
			c.RedisPipeLine(rc.GetCode()).RPush(schema.GetCacheKey()+":"+bind["ID"], convertBindToRedisValue(bind, schema)...)
		}
	}
	if !lazy {
		sql := s.String()
		c.appendDBAction(schema, func(db DBBase) {
			db.Exec(c, sql, args...)
		})
	}

	return nil
}

func (c *contextImplementation) handleUpdates(lazy bool, schema *entitySchema, operations []EntityFlush) error {
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
				hSetKey := schema.GetCacheKey() + ":" + indexName
				hField, hasKey := buildUniqueKeyHSetField(indexColumns, newBind)
				if hasKey {
					previousID, inUse := cache.HGet(c, hSetKey, hField)
					if inUse {
						idAsUint, _ := strconv.ParseUint(previousID, 10, 64)
						return &DuplicatedKeyBindError{Index: indexName, ID: idAsUint, Columns: indexColumns}
					}
					c.RedisPipeLine(cache.GetPoolConfig().GetCode()).HSet(hSetKey, hField, strconv.FormatUint(update.ID(), 10))
				}
				hFieldOld, hasKey := buildUniqueKeyHSetField(indexColumns, oldBind)
				if hasKey {
					c.RedisPipeLine(cache.GetPoolConfig().GetCode()).HDel(hSetKey, hFieldOld)
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
		var args []interface{}
		var lazyArgs []string
		if lazy {
			lazyArgs = make([]string, len(newBind)+2)
		} else {
			args = make([]interface{}, len(newBind)+1)
		}
		for column, value := range newBind {
			if k > 0 {
				s.WriteString(",")
			}
			s.WriteString("`" + column + "`=?")
			if lazy {
				lazyArgs[k+1] = value
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
		if lazy {
			lazyArgs[k+1] = strconv.FormatUint(update.ID(), 10)
		} else {
			args[k] = update.ID()
		}
		sql := s.String()
		if lazy {
			lazyArgs[0] = sql
			asJson, _ := jsoniter.ConfigFastest.MarshalToString(lazyArgs)
			c.RedisPipeLine(schema.getLazyRedisCode()).RPush(schema.lazyCacheKey, asJson)
		} else {
			c.appendDBAction(schema, func(db DBBase) {
				db.Exec(c, sql, args...)
			})
		}

		if hasLocalCache {
			c.flushPostActions = append(c.flushPostActions, func() {
				sourceValue := update.getSourceValue()
				copyEntity(update.getValue().Elem(), sourceValue.Elem(), schema.getFields())
				lc.setEntity(c, operation.ID(), update.getEntity())
			})
		}
		if hasRedisCache {
			p := c.RedisPipeLine(rc.GetCode())
			for column, val := range newBind {
				index := int64(schema.columnMapping[column] + 1)
				p.LSet(schema.GetCacheKey()+":"+strconv.FormatUint(update.ID(), 10), index, convertBindValueToRedisValue(val))
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
			poolSQLGroup = make(schemaSqlOperations)
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
	poolCode := schema.GetDB().GetPoolConfig().GetCode()
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
