package beeorm

import (
	"strconv"
	"strings"
)

type entitySqlOperations map[FlushType][]EntityFlush
type schemaSqlOperations map[EntitySchema]entitySqlOperations
type sqlOperations map[DB]schemaSqlOperations

type flushData struct {
	SQL    string
	Args   []interface{}
	Schema EntitySchema
}

func (c *contextImplementation) Flush() error {
	return c.flush(false)
}

func (c *contextImplementation) FlushLazy() error {
	return c.flush(true)
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
				err := c.handleDeletes(schema, deletes)
				if err != nil {
					return err
				}
			}
			inserts, has := queryOperations[Insert]
			if has {
				err := c.handleInserts(schema, inserts)
				if err != nil {
					return err
				}
			}
			updates, has := queryOperations[Update]
			if has {
				err := c.handleUpdates(schema, updates)
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
				var d db
				d = c.Engine().DB(code)
				if len(actions) > 1 || len(c.flushDBActions) > 1 {
					tx := d.(DB).Begin(c)
					transactions = append(transactions, tx)
					d = tx
				}
				for _, action := range actions {
					d.Exec(c, action.SQL, action.Args...)
				}
			}
			for _, tx := range transactions {
				tx.Commit(c)
			}
		}()
	} else {
		// TODO lazy
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

func (c *contextImplementation) handleDeletes(schema EntitySchema, operations []EntityFlush) error {
	args := make([]interface{}, len(operations))
	s := c.getStringBuilder2()
	s.WriteString("DELETE FROM `")
	s.WriteString(schema.GetTableName())
	s.WriteString("` WHERE ID IN (?")
	s.WriteString(strings.Repeat(",?", len(operations)-1))
	s.WriteString(")")
	for i, operation := range operations {
		args[i] = operation.ID()
	}
	sql := s.String()
	c.appendDBAction(schema, sql, args)
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
					c.RedisPipeLine(cache.GetPoolConfig().GetCode()).HDel(c, hSetKey, hField)
				}
			}
		}
		if hasLocalCache {
			c.flushPostActions = append(c.flushPostActions, func() {
				lc.removeEntity(c, operation.ID())
			})
		}
		rc, hasRedisCache := schema.GetRedisCache()
		if hasRedisCache {
			c.RedisPipeLine(rc.GetCode()).Del(c, schema.GetCacheKey()+":"+strconv.FormatUint(operation.ID(), 10))
		}
	}
	return nil
}

func (c *contextImplementation) handleInserts(schema EntitySchema, operations []EntityFlush) error {
	columns := schema.GetColumns()
	args := make([]interface{}, 0, len(operations)*len(columns))
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
				c.RedisPipeLine(cache.GetPoolConfig().GetCode()).HSet(c, hSetKey, hField, strconv.FormatUint(insert.ID(), 10))
			}
		}

		if i > 0 {
			s.WriteString(",")
		}
		s.WriteString("(?")
		args = append(args, bind["ID"])
		for _, column := range columns[1:] {
			v := bind[column]
			if v == nullAsString {
				args = append(args, nil)
			} else {
				args = append(args, v)
			}
			s.WriteString(",?")
		}
		s.WriteString(")")

		if hasLocalCache {
			c.flushPostActions = append(c.flushPostActions, func() {
				lc.setEntity(c, insert.ID(), insert.getValue())
			})
		}
		if hasRedisCache {
			c.RedisPipeLine(rc.GetCode()).RPush(c, schema.GetCacheKey()+":"+bind["ID"], convertBindToRedisValue(bind, schema)...)
		}
	}
	sql := s.String()
	c.appendDBAction(schema, sql, args)
	return nil
}

func (c *contextImplementation) handleUpdates(schema EntitySchema, operations []EntityFlush) error {
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
					c.RedisPipeLine(cache.GetPoolConfig().GetCode()).HSet(c, hSetKey, hField, strconv.FormatUint(update.ID(), 10))
				}
				hFieldOld, hasKey := buildUniqueKeyHSetField(indexColumns, oldBind)
				if hasKey {
					c.RedisPipeLine(cache.GetPoolConfig().GetCode()).HDel(c, hSetKey, hFieldOld)
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
		args := make([]interface{}, len(newBind)+1)
		for column, value := range newBind {
			if k > 0 {
				s.WriteString(",")
			}
			s.WriteString("`" + column + "`=?")
			if value == nullAsString {
				args[k] = nil
			} else {
				args[k] = value
			}
			k++
		}
		s.WriteString(" WHERE ID = ?")
		args[k] = update.ID()
		sql := s.String()
		c.appendDBAction(schema, sql, args)

		if hasLocalCache {
			c.flushPostActions = append(c.flushPostActions, func() {
				sourceValue := update.getSourceValue()
				copyEntity(update.getValue().Elem(), sourceValue.Elem(), schema.getFields())
				lc.setEntity(c, operation.ID(), sourceValue)
			})
		}
		if hasRedisCache {
			p := c.RedisPipeLine(rc.GetCode())
			for column, val := range newBind {
				index := int64(schema.(*entitySchema).columnMapping[column] + 1)
				p.LSet(c, schema.GetCacheKey()+":"+strconv.FormatUint(update.ID(), 10), index, convertBindValueToRedisValue(val))
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

func (c *contextImplementation) appendDBAction(schema EntitySchema, sql string, args []interface{}) {
	if c.flushDBActions == nil {
		c.flushDBActions = make(map[string][]flushData)
	}
	poolCode := schema.GetDB().GetPoolConfig().GetCode()
	c.flushDBActions[poolCode] = append(c.flushDBActions[poolCode], flushData{SQL: sql, Args: args, Schema: schema})
}

func buildUniqueKeyHSetField(indexColumns []string, bind Bind) (string, bool) {
	hField := ""
	hasNil := false
	for _, column := range indexColumns {
		bindValue := bind[column]
		if bindValue == nullAsString {
			hasNil = true
			break
		}
		hField += bindValue
	}
	if hasNil {
		return "", false
	}
	return hashString(hField), true
}
