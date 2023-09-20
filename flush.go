package beeorm

import (
	"strconv"
	"strings"
)

type entitySqlOperations map[FlushType][]EntityFlush
type schemaSqlOperations map[EntitySchema]entitySqlOperations
type sqlOperations map[DB]schemaSqlOperations

func (c *contextImplementation) Flush() error {
	if len(c.trackedEntities) == 0 {
		return nil
	}
	sqlGroup := c.groupSQLOperations()
	for db, operations := range sqlGroup {
		for schema, queryOperations := range operations {
			deletes, has := queryOperations[Delete]
			if has {
				err := c.executeDeletes(db, schema, deletes)
				if err != nil {
					return err
				}
			}
			inserts, has := queryOperations[Insert]
			if has {
				err := c.executeInserts(db, schema, inserts)
				if err != nil {
					return err
				}
			}
			updates, has := queryOperations[Update]
			if has {
				err := c.executeUpdates(db, schema, updates)
				if err != nil {
					return err
				}
			}
		}
	}
	for _, pipeline := range c.redisPipeLines {
		pipeline.Exec(c)
	}
	c.ClearFlush()
	return nil
}

func (c *contextImplementation) FlushLazy() error {
	return nil
}

func (c *contextImplementation) ClearFlush() {
	c.trackedEntities = c.trackedEntities[0:0]
	c.redisPipeLines = nil
}

func (c *contextImplementation) executeDeletes(db DB, schema EntitySchema, operations []EntityFlush) error {
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
	db.Exec(c, s.String(), args...)
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
					c.PipeLine(cache.GetPoolConfig().GetCode()).HDel(c, hSetKey, hField)
				}
			}
		}
	}
	return nil
}

func (c *contextImplementation) executeInserts(db DB, schema EntitySchema, operations []EntityFlush) error {
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
				c.PipeLine(cache.GetPoolConfig().GetCode()).HSet(c, hSetKey, hField, strconv.FormatUint(insert.ID(), 10))
			}
		}

		if i > 0 {
			s.WriteString(",(?")
		}
		s.WriteString("(?")
		args = append(args, bind["ID"])
		for _, column := range columns[1:] {
			args = append(args, bind[column])
			s.WriteString(",?")
		}
		s.WriteString(")")
	}
	db.Exec(c, s.String(), args...)
	return nil
}

func (c *contextImplementation) executeUpdates(db DB, schema EntitySchema, operations []EntityFlush) error {
	var queryPrefix string
	for _, operation := range operations {
		update := operation.(entityFlushUpdate)
		_, newBind, err := update.getBind()
		if err != nil {
			return err
		}
		if len(newBind) == 0 {
			continue
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
			args[k] = value
			k++
		}
		s.WriteString(" WHERE ID = ?")
		args[k] = update.ID()
		db.Exec(c, s.String(), args...)
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

func buildUniqueKeyHSetField(indexColumns []string, bind Bind) (string, bool) {
	hField := ""
	hasNil := false
	for _, column := range indexColumns {
		bindValue := bind[column]
		if bindValue == nil {
			hasNil = true
			break
		}
		hField += convertBindValueToString(bindValue)
	}
	if hasNil {
		return "", false
	}
	return hField, true
}
