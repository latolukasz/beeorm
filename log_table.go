package beeorm

import (
	"reflect"
	"time"
)

type LogEntity[Entity any] struct {
	ID       uint64 `orm:"archived"`
	EntityID uint64
	Date     time.Time `orm:"time"`
	Meta     []byte
	Before   []byte
	After    []byte
}

type logEntityInterface interface {
	getLogEntityTarget() reflect.Type
}

func (l *LogEntity[Entity]) getLogEntityTarget() reflect.Type {
	var e Entity
	return reflect.TypeOf(e)
}

//func (p *Plugin) PluginInterfaceTableSQLSchemaDefinition(c beeorm.Context, sqlSchema *beeorm.TableSQLSchemaDefinition) error {
//	poolName := sqlSchema.EntitySchema.GetPluginOption(PluginCode, poolOption)
//	if poolName == nil {
//		return nil
//	}
//	tableName := sqlSchema.EntitySchema.GetPluginOption(PluginCode, tableNameOption)
//	db := c.Engine().DB(poolName.(string))
//	var tableDef string
//	hasLogTable := db.QueryRow(c, beeorm.NewWhere(fmt.Sprintf("SHOW TABLES LIKE '%s'", tableName)), &tableDef)
//	var logEntitySchema string
//	if db.GetPoolConfig().GetVersion() == 5 {
//		logEntitySchema = fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n  `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT,\n  "+
//			"`entity_id` int(10) unsigned NOT NULL,\n  `added_at` datetime NOT NULL,\n  `meta` json DEFAULT NULL,\n  `before` json DEFAULT NULL,\n  `changes` json DEFAULT NULL,\n  "+
//			"PRIMARY KEY (`id`),\n  KEY `entity_id` (`entity_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;",
//			db.GetPoolConfig().GetDatabase(), tableName)
//	} else {
//		logEntitySchema = fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  "+
//			"`entity_id` int unsigned NOT NULL,\n  `added_at` datetime NOT NULL,\n  `meta` json DEFAULT NULL,\n  `before` json DEFAULT NULL,\n  `changes` json DEFAULT NULL,\n  "+
//			"PRIMARY KEY (`id`),\n  KEY `entity_id` (`entity_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_%s ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;",
//			db.GetPoolConfig().GetDatabase(), tableName, c.Engine().Registry().DefaultDBCollate())
//	}
//
//	if !hasLogTable {
//		sqlSchema.PostAlters = append(sqlSchema.PostAlters, beeorm.Alter{SQL: logEntitySchema, Safe: true, Pool: poolName.(string)})
//	} else {
//		var skip, createTableDB string
//		db.QueryRow(c, beeorm.NewWhere(fmt.Sprintf("SHOW CREATE TABLE `%s`", tableName)), &skip, &createTableDB)
//		createTableDB = strings.Replace(createTableDB, "CREATE TABLE ", fmt.Sprintf("CREATE TABLE `%s`.", db.GetPoolConfig().GetDatabase()), 1) + ";"
//		re := regexp.MustCompile(" AUTO_INCREMENT=[0-9]+ ")
//		createTableDB = re.ReplaceAllString(createTableDB, " ")
//		if logEntitySchema != createTableDB {
//			db.QueryRow(c, beeorm.NewWhere("1"))
//			isEmpty := !db.QueryRow(c, beeorm.NewWhere(fmt.Sprintf("SELECT ID FROM `%s`", tableName)))
//			dropTableSQL := fmt.Sprintf("DROP TABLE `%s`.`%s`;", db.GetPoolConfig().GetDatabase(), tableName)
//			sqlSchema.PostAlters = append(sqlSchema.PostAlters, beeorm.Alter{SQL: dropTableSQL, Safe: isEmpty, Pool: poolName.(string)})
//			sqlSchema.PostAlters = append(sqlSchema.PostAlters, beeorm.Alter{SQL: logEntitySchema, Safe: true, Pool: poolName.(string)})
//		}
//	}
//	return nil
//}
