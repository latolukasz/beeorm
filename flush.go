package beeorm

import "fmt"

type entitySqlOperations map[FlushType][]EntityFlush
type schemaSqlOperations map[EntitySchema]entitySqlOperations
type sqlOperations map[DB]schemaSqlOperations

func (c *contextImplementation) Flush() {
	if len(c.trackedEntities) == 0 {
		return
	}
	sqlGroup := c.groupSQLOperations()
	for db, operations := range sqlGroup {
		for schema, queryOperations := range operations {
			deletes, has := queryOperations[Delete]
			if has {
				c.executeInserts(db, schema, deletes)
			}
			inserts, has := queryOperations[Insert]
			if has {
				c.executeInserts(db, schema, inserts)
			}
			updates, has := queryOperations[Update]
			if has {
				c.executeUpdates(db, schema, updates)
			}
		}
	}

}

func (c *contextImplementation) executeDeletes(db DB, schema EntitySchema, operations []EntityFlush) {
	//TODO
}

func (c *contextImplementation) executeInserts(db DB, schema EntitySchema, operations []EntityFlush) {
	columns := schema.GetColumns()
	args := make([]interface{}, 0, len(operations)*len(columns)+len(operations))
	s := c.getStringBuilder2()
	s.WriteString("INSERT INTO `")
	s.WriteString(schema.GetTableName())
	s.WriteString("`(`ID`")
	for _, column := range columns {
		s.WriteString(",`")
		s.WriteString(column)
		s.WriteString("`")
	}
	s.WriteString(") VALUES")
	for i, operation := range operations {
		insert := operation.(EntityFlushInsert)
		bind := insert.GetBind()
		fmt.Printf("%v\n", bind)
		if i > 0 {
			s.WriteString(",(?")
		}
		s.WriteString("(?")
		args = append(args, bind["ID"])
		for _, column := range columns {
			args = append(args, bind[column])
			s.WriteString(",?")
		}
		s.WriteString(")")
	}
	fmt.Println(s.String())
	db.Exec(c, s.String(), args...)
}

func (c *contextImplementation) executeUpdates(db DB, schema EntitySchema, operations []EntityFlush) {
	//TODO
}

func (c *contextImplementation) FlushLazy() {
	//TODO
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
		tableSQLGroup[val.FlushType()] = append(tableSQLGroup[val.FlushType()], val)
	}
	return sqlGroup
}
