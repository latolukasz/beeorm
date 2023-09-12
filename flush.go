package beeorm

import "fmt"

type sqlOperations map[sqlPool]map[tableName]map[FlushType][]EntityFlush
type sqlPool string
type tableName string

func (c *contextImplementation) Flush() {
	if len(c.trackedEntities) == 0 {
		return
	}
	fmt.Printf("1\n")
	sqlGroup := c.groupSQLOperations()
	fmt.Printf("2 %v\n", sqlGroup)
}

func (c *contextImplementation) FlushLazy() {
	//TODO
}

func (c *contextImplementation) groupSQLOperations() sqlOperations {
	sqlGroup := make(sqlOperations)
	for _, val := range c.trackedEntities {
		schema := val.Schema()
		mysqlPoolCode := sqlPool(schema.mysqlPoolCode)
		poolSQLGroup, has := sqlGroup[mysqlPoolCode]
		if !has {
			poolSQLGroup = make(map[tableName]map[FlushType][]EntityFlush)
			sqlGroup[mysqlPoolCode] = poolSQLGroup
		}
		table := tableName(schema.tableName)
		tableSQLGroup, has := poolSQLGroup[table]
		if !has {
			tableSQLGroup = make(map[FlushType][]EntityFlush)
			poolSQLGroup[table] = tableSQLGroup
		}
		tableSQLGroup[val.FlushType()] = append(tableSQLGroup[val.FlushType()], val)
	}
	return sqlGroup
}
