package beeorm

import (
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type Alter struct {
	SQL    string
	Safe   bool
	Pool   string
	engine *engineImplementation
}

type indexDB struct {
	Skip      sql.NullString
	NonUnique uint8
	KeyName   string
	Seq       int
	Column    string
}

type index struct {
	Unique  bool
	Columns map[int]string
}

type foreignIndex struct {
	Column         string
	Table          string
	ParentDatabase string
	OnDelete       string
}

type foreignKeyDB struct {
	ConstraintName        string
	ColumnName            string
	ReferencedTableName   string
	ReferencedTableSchema string
	OnDelete              string
}

func (a Alter) Exec() {
	a.engine.GetMysql(a.Pool).Exec(a.SQL)
}

func getAlters(engine *engineImplementation) (alters []Alter) {
	tablesInDB := make(map[string]map[string]bool)
	tablesInEntities := make(map[string]map[string]bool)

	if engine.registry.mySQLServers != nil {
		for _, pool := range engine.registry.mySQLServers {
			poolName := pool.GetCode()
			tablesInDB[poolName] = make(map[string]bool)
			pool := engine.GetMysql(poolName)
			tables := getAllTables(pool.client)
			for _, table := range tables {
				tablesInDB[poolName][table] = true
			}
			tablesInEntities[poolName] = make(map[string]bool)
		}
	}
	alters = make([]Alter, 0)
	if engine.registry.entities != nil {
		for _, t := range engine.registry.entities {
			tableSchema := getTableSchema(engine.registry, t)
			tablesInEntities[tableSchema.mysqlPoolName][tableSchema.tableName] = true
			has, newAlters := tableSchema.GetSchemaChanges(engine)
			if tableSchema.hasLog {
				logPool := engine.GetMysql(tableSchema.logPoolName)
				var tableDef string
				hasLogTable := logPool.QueryRow(NewWhere(fmt.Sprintf("SHOW TABLES LIKE '%s'", tableSchema.logTableName)), &tableDef)
				var logTableSchema string
				if logPool.GetPoolConfig().GetVersion() == 5 {
					logTableSchema = fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n  `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT,\n  "+
						"`entity_id` int(10) unsigned NOT NULL,\n  `added_at` datetime NOT NULL,\n  `meta` json DEFAULT NULL,\n  `before` json DEFAULT NULL,\n  `changes` json DEFAULT NULL,\n  "+
						"PRIMARY KEY (`id`),\n  KEY `entity_id` (`entity_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;",
						logPool.GetPoolConfig().GetDatabase(), tableSchema.logTableName)
				} else {
					logTableSchema = fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  "+
						"`entity_id` int unsigned NOT NULL,\n  `added_at` datetime NOT NULL,\n  `meta` json DEFAULT NULL,\n  `before` json DEFAULT NULL,\n  `changes` json DEFAULT NULL,\n  "+
						"PRIMARY KEY (`id`),\n  KEY `entity_id` (`entity_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_%s ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;",
						logPool.GetPoolConfig().GetDatabase(), tableSchema.logTableName, engine.registry.registry.defaultCollate)
				}

				if !hasLogTable {
					alters = append(alters, Alter{SQL: logTableSchema, Safe: true, Pool: tableSchema.logPoolName, engine: engine})
				} else {
					var skip, createTableDB string
					logPool.QueryRow(NewWhere(fmt.Sprintf("SHOW CREATE TABLE `%s`", tableSchema.logTableName)), &skip, &createTableDB)
					createTableDB = strings.Replace(createTableDB, "CREATE TABLE ", fmt.Sprintf("CREATE TABLE `%s`.", logPool.GetPoolConfig().GetDatabase()), 1) + ";"
					re := regexp.MustCompile(" AUTO_INCREMENT=[0-9]+ ")
					createTableDB = re.ReplaceAllString(createTableDB, " ")
					if logTableSchema != createTableDB {
						isEmpty := isTableEmptyInPool(engine, tableSchema.logPoolName, tableSchema.logTableName)
						dropTableSQL := fmt.Sprintf("DROP TABLE `%s`.`%s`;", logPool.GetPoolConfig().GetDatabase(), tableSchema.logTableName)
						alters = append(alters, Alter{SQL: dropTableSQL, Safe: isEmpty, Pool: tableSchema.logPoolName, engine: engine})
						alters = append(alters, Alter{SQL: logTableSchema, Safe: true, Pool: tableSchema.logPoolName, engine: engine})
					}
				}
				tablesInEntities[tableSchema.logPoolName][tableSchema.logTableName] = true
			}
			if !has {
				continue
			}
			alters = append(alters, newAlters...)
		}
	}

	for poolName, tables := range tablesInDB {
		for tableName := range tables {
			_, has := tablesInEntities[poolName][tableName]
			if !has {
				dropForeignKeyAlter := getDropForeignKeysAlter(engine, tableName, poolName)
				if dropForeignKeyAlter != "" {
					alters = append(alters, Alter{SQL: dropForeignKeyAlter, Safe: true, Pool: poolName, engine: engine})
				}
				pool := engine.GetMysql(poolName)
				dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`;", pool.GetPoolConfig().GetDatabase(), tableName)
				isEmpty := isTableEmptyInPool(engine, poolName, tableName)
				alters = append(alters, Alter{SQL: dropSQL, Safe: isEmpty, Pool: poolName, engine: engine})
			}
		}
	}
	sortedNormal := make([]Alter, 0)
	sortedDropForeign := make([]Alter, 0)
	sortedAddForeign := make([]Alter, 0)
	for _, alter := range alters {
		hasDropForeignKey := strings.Index(alter.SQL, "DROP FOREIGN KEY") > 0
		hasAddForeignKey := strings.Index(alter.SQL, "ADD CONSTRAINT") > 0
		if !hasDropForeignKey && !hasAddForeignKey {
			sortedNormal = append(sortedNormal, alter)
		}
	}
	for _, alter := range alters {
		hasDropForeignKey := strings.Index(alter.SQL, "DROP FOREIGN KEY") > 0
		if hasDropForeignKey {
			sortedDropForeign = append(sortedDropForeign, alter)
		}
	}
	for _, alter := range alters {
		hasAddForeignKey := strings.Index(alter.SQL, "ADD CONSTRAINT") > 0
		if hasAddForeignKey {
			sortedAddForeign = append(sortedAddForeign, alter)
		}
	}
	sort.Slice(sortedNormal, func(i int, j int) bool {
		return len(sortedNormal[i].SQL) < len(sortedNormal[j].SQL)
	})
	final := sortedDropForeign
	final = append(final, sortedNormal...)
	final = append(final, sortedAddForeign...)
	return final
}

func isTableEmptyInPool(engine *engineImplementation, poolName string, tableName string) bool {
	return isTableEmpty(engine.GetMysql(poolName).client, tableName)
}

func getAllTables(db sqlClient) []string {
	tables := make([]string, 0)
	results, err := db.Query("SHOW FULL TABLES WHERE Table_Type = 'BASE TABLE'")
	checkError(err)
	defer func() {
		_ = results.Close()
	}()
	var skip string
	for results.Next() {
		var row string
		err = results.Scan(&row, &skip)
		checkError(err)
		tables = append(tables, row)
	}
	err = results.Err()
	checkError(err)
	return tables
}

func getSchemaChanges(engine *engineImplementation, tableSchema *tableSchema) (has bool, alters []Alter) {
	indexes := make(map[string]*index)
	foreignKeys := make(map[string]*foreignIndex)
	columns, _ := checkStruct(tableSchema, engine, tableSchema.t, indexes, foreignKeys, nil, "")
	var newIndexes []string
	var newForeignKeys []string
	pool := engine.GetMysql(tableSchema.mysqlPoolName)
	createTableSQL := fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n", pool.GetPoolConfig().GetDatabase(), tableSchema.tableName)
	createTableForeignKeysSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s`\n", pool.GetPoolConfig().GetDatabase(), tableSchema.tableName)
	if !tableSchema.hasUUID {
		columns[0][1] += " AUTO_INCREMENT"
	}
	for _, value := range columns {
		createTableSQL += fmt.Sprintf("  %s,\n", value[1])
	}
	for keyName, indexEntity := range indexes {
		newIndexes = append(newIndexes, buildCreateIndexSQL(keyName, indexEntity))
	}
	sort.Strings(newIndexes)
	for _, value := range newIndexes {
		createTableSQL += fmt.Sprintf("  %s,\n", value[4:])
	}
	for keyName, foreignKey := range foreignKeys {
		newForeignKeys = append(newForeignKeys, buildCreateForeignKeySQL(keyName, foreignKey))
	}
	sort.Strings(newForeignKeys)
	for _, value := range newForeignKeys {
		createTableForeignKeysSQL += fmt.Sprintf("  %s,\n", value)
	}

	createTableSQL += "  PRIMARY KEY (`ID`)\n"
	collate := ""
	if pool.GetPoolConfig().GetVersion() == 8 {
		collate += " COLLATE=" + engine.registry.registry.defaultEncoding + "_" + engine.registry.registry.defaultCollate
	}
	createTableSQL += fmt.Sprintf(") ENGINE=InnoDB DEFAULT CHARSET=%s%s;", engine.registry.registry.defaultEncoding, collate)

	var skip string
	hasTable := pool.QueryRow(NewWhere(fmt.Sprintf("SHOW TABLES LIKE '%s'", tableSchema.tableName)), &skip)

	if !hasTable {
		alters = []Alter{{SQL: createTableSQL, Safe: true, Pool: tableSchema.mysqlPoolName, engine: engine}}
		if len(newForeignKeys) > 0 {
			createTableForeignKeysSQL = strings.TrimRight(createTableForeignKeysSQL, ",\n") + ";"
			alters = append(alters, Alter{SQL: createTableForeignKeysSQL, Safe: true, Pool: tableSchema.mysqlPoolName, engine: engine})
		}
		has = true
		return
	}
	newIndexes = make([]string, 0)
	newForeignKeys = make([]string, 0)

	var tableDBColumns = make([][2]string, 0)
	var createTableDB string
	pool.QueryRow(NewWhere(fmt.Sprintf("SHOW CREATE TABLE `%s`", tableSchema.tableName)), &skip, &createTableDB)

	hasAlters := false
	hasAlterNormal := false
	hasAlterEngineCharset := false
	lines := strings.Split(createTableDB, "\n")
	for x := 1; x < len(lines); x++ {
		if lines[x][2] != 96 {
			for _, field := range strings.Split(lines[x], " ") {
				if strings.HasPrefix(field, "CHARSET=") {
					if field[8:] != engine.registry.registry.defaultEncoding {
						hasAlters = true
						hasAlterEngineCharset = true
					}
				}
			}
			continue
		}
		var line = strings.TrimRight(lines[x], ",")
		line = strings.TrimLeft(line, " ")
		var columnName = strings.Split(line, "`")[1]
		tableDBColumns = append(tableDBColumns, [2]string{columnName, line})
	}

	var rows []indexDB
	/* #nosec */
	results, def := pool.Query(fmt.Sprintf("SHOW INDEXES FROM `%s`", tableSchema.tableName))
	defer def()
	for results.Next() {
		var row indexDB
		if pool.GetPoolConfig().GetVersion() == 5 {
			results.Scan(&row.Skip, &row.NonUnique, &row.KeyName, &row.Seq, &row.Column, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip)
		} else {
			results.Scan(&row.Skip, &row.NonUnique, &row.KeyName, &row.Seq, &row.Column, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip)
		}
		rows = append(rows, row)
	}
	def()
	var indexesDB = make(map[string]*index)
	for _, value := range rows {
		current, has := indexesDB[value.KeyName]
		if !has {
			current = &index{Unique: value.NonUnique == 0, Columns: map[int]string{value.Seq: value.Column}}
			indexesDB[value.KeyName] = current
		} else {
			current.Columns[value.Seq] = value.Column
		}
	}

	foreignKeysDB := getForeignKeys(engine, createTableDB, tableSchema.tableName, tableSchema.mysqlPoolName)

	var newColumns []string
	var changedColumns [][2]string

	for key, value := range columns {
		var tableColumn string
		if key < len(tableDBColumns) {
			tableColumn = tableDBColumns[key][1]
		}
		if tableColumn == value[1] {
			continue
		}
		hasName := -1
		hasDefinition := -1
		for z, v := range tableDBColumns {
			if v[1] == value[1] {
				hasDefinition = z
			}
			if v[0] == value[0] {
				hasName = z
			}
		}
		if hasName == -1 {
			alter := fmt.Sprintf("ADD COLUMN %s", value[1])
			if key > 0 {
				alter += fmt.Sprintf(" AFTER `%s`", columns[key-1][0])
			}
			newColumns = append(newColumns, alter)
			hasAlters = true
		} else {
			if hasDefinition == -1 {
				alter := fmt.Sprintf("CHANGE COLUMN `%s` %s", value[0], value[1])
				if key > 0 {
					/* #nosec */
					alter += fmt.Sprintf(" AFTER `%s`", columns[key-1][0])
				}
				/* #nosec */
				changedColumns = append(changedColumns, [2]string{alter, fmt.Sprintf("CHANGED FROM %s", tableDBColumns[hasName][1])})
				hasAlters = true
			} else {
				alter := fmt.Sprintf("CHANGE COLUMN `%s` %s", value[0], value[1])
				if key > 0 {
					alter += fmt.Sprintf(" AFTER `%s`", columns[key-1][0])
				}
				changedColumns = append(changedColumns, [2]string{alter, "CHANGED ORDER"})
				hasAlters = true
			}
		}
	}
	droppedColumns := make([]string, 0)
OUTER:
	for _, value := range tableDBColumns {
		for _, v := range columns {
			if v[0] == value[0] {
				continue OUTER
			}
		}
		droppedColumns = append(droppedColumns, fmt.Sprintf("DROP COLUMN `%s`", value[0]))
		hasAlters = true
	}

	var droppedIndexes []string
	for keyName, indexEntity := range indexes {
		indexDB, has := indexesDB[keyName]
		if !has {
			newIndexes = append(newIndexes, buildCreateIndexSQL(keyName, indexEntity))
			hasAlters = true
		} else {
			addIndexSQLEntity := buildCreateIndexSQL(keyName, indexEntity)
			addIndexSQLDB := buildCreateIndexSQL(keyName, indexDB)
			if addIndexSQLEntity != addIndexSQLDB {
				droppedIndexes = append(droppedIndexes, fmt.Sprintf("DROP INDEX `%s`", keyName))
				newIndexes = append(newIndexes, addIndexSQLEntity)
				hasAlters = true
			}
		}
	}

	var droppedForeignKeys []string
	for keyName, indexEntity := range foreignKeys {
		indexDB, has := foreignKeysDB[keyName]
		if !has {
			newForeignKeys = append(newForeignKeys, buildCreateForeignKeySQL(keyName, indexEntity))
			hasAlters = true
		} else {
			addIndexSQLEntity := buildCreateForeignKeySQL(keyName, indexEntity)
			addIndexSQLDB := buildCreateForeignKeySQL(keyName, indexDB)
			if addIndexSQLEntity != addIndexSQLDB {
				droppedForeignKeys = append(droppedForeignKeys, fmt.Sprintf("DROP FOREIGN KEY `%s`", keyName))
				newForeignKeys = append(newForeignKeys, addIndexSQLEntity)
				hasAlters = true
			}
		}
	}
	for keyName := range indexesDB {
		_, has := indexes[keyName]
		if !has && keyName != "PRIMARY" {
			_, has = foreignKeys[keyName]
			if !has {
				droppedIndexes = append(droppedIndexes, fmt.Sprintf("DROP INDEX `%s`", keyName))
				hasAlters = true
			}
		}
	}
	for keyName := range foreignKeysDB {
		_, has := foreignKeys[keyName]
		if !has {
			droppedForeignKeys = append(droppedForeignKeys, fmt.Sprintf("DROP FOREIGN KEY `%s`", keyName))
			hasAlters = true
		}
	}
	if !hasAlters {
		return
	}

	alterSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s`\n", pool.GetPoolConfig().GetDatabase(), tableSchema.tableName)
	newAlters := make([]string, 0)
	comments := make([]string, 0)
	hasAlterAddForeignKey := false
	hasAlterRemoveForeignKey := false

	alterSQLAddForeignKey := fmt.Sprintf("ALTER TABLE `%s`.`%s`\n", pool.GetPoolConfig().GetDatabase(), tableSchema.tableName)
	newAltersAddForeignKey := make([]string, 0)
	alterSQLRemoveForeignKey := fmt.Sprintf("ALTER TABLE `%s`.`%s`\n", pool.GetPoolConfig().GetDatabase(), tableSchema.tableName)
	newAltersRemoveForeignKey := make([]string, 0)

	for _, value := range droppedColumns {
		newAlters = append(newAlters, fmt.Sprintf("    %s", value))
		comments = append(comments, "")
		hasAlterNormal = true
	}
	for _, value := range newColumns {
		newAlters = append(newAlters, fmt.Sprintf("    %s", value))
		comments = append(comments, "")
		hasAlterNormal = true
	}
	for _, value := range changedColumns {
		newAlters = append(newAlters, fmt.Sprintf("    %s", value[0]))
		comments = append(comments, value[1])
	}
	sort.Strings(droppedIndexes)
	for _, value := range droppedIndexes {
		newAlters = append(newAlters, fmt.Sprintf("    %s", value))
		comments = append(comments, "")
		hasAlterNormal = true
	}
	sort.Strings(droppedForeignKeys)
	for _, value := range droppedForeignKeys {
		newAltersRemoveForeignKey = append(newAltersRemoveForeignKey, fmt.Sprintf("    %s", value))
		hasAlterRemoveForeignKey = true
	}
	sort.Strings(newIndexes)
	for _, value := range newIndexes {
		newAlters = append(newAlters, fmt.Sprintf("    %s", value))
		comments = append(comments, "")
		hasAlterNormal = true
	}
	sort.Strings(newForeignKeys)
	for _, value := range newForeignKeys {
		newAltersAddForeignKey = append(newAltersAddForeignKey, fmt.Sprintf("    %s", value))
		hasAlterAddForeignKey = true
	}

	for x := 0; x < len(newAlters)-1; x++ {
		hasAlterNormal = true
		alterSQL += newAlters[x] + ","
		if comments[x] != "" {
			alterSQL += fmt.Sprintf("/*%s*/", comments[x])
		}
		alterSQL += "\n"
	}
	lastIndex := len(newAlters) - 1
	if lastIndex >= 0 {
		hasAlterNormal = true
		alterSQL += newAlters[lastIndex] + ";"
		if comments[lastIndex] != "" {
			alterSQL += fmt.Sprintf("/*%s*/", comments[lastIndex])
		}
	}

	for x := 0; x < len(newAltersAddForeignKey); x++ {
		alterSQLAddForeignKey += newAltersAddForeignKey[x] + ","
		alterSQLAddForeignKey += "\n"
	}
	for x := 0; x < len(newAltersRemoveForeignKey); x++ {
		alterSQLRemoveForeignKey += newAltersRemoveForeignKey[x] + ","
		alterSQLRemoveForeignKey += "\n"
	}

	alters = make([]Alter, 0)
	if hasAlterNormal {
		safe := false
		if len(droppedColumns) == 0 && len(changedColumns) == 0 {
			safe = true
		} else {
			db := tableSchema.GetMysql(engine)
			isEmpty := isTableEmpty(db.client, tableSchema.tableName)
			safe = isEmpty
		}
		alters = append(alters, Alter{SQL: alterSQL, Safe: safe, Pool: tableSchema.mysqlPoolName, engine: engine})
	} else if hasAlterEngineCharset {
		collate := ""
		if pool.GetPoolConfig().GetVersion() == 8 {
			collate += " COLLATE=" + engine.registry.registry.defaultEncoding + "_" + engine.registry.registry.defaultCollate
		}
		alterSQL += fmt.Sprintf(" ENGINE=InnoDB DEFAULT CHARSET=%s%s;", engine.registry.registry.defaultEncoding, collate)
		alters = append(alters, Alter{SQL: alterSQL, Safe: true, Pool: tableSchema.mysqlPoolName, engine: engine})
	}
	if hasAlterRemoveForeignKey {
		alterSQLRemoveForeignKey = strings.TrimRight(alterSQLRemoveForeignKey, ",\n") + ";"
		alters = append(alters, Alter{SQL: alterSQLRemoveForeignKey, Safe: true, Pool: tableSchema.mysqlPoolName, engine: engine})
	}
	if hasAlterAddForeignKey {
		alterSQLAddForeignKey = strings.TrimRight(alterSQLAddForeignKey, ",\n") + ";"
		alters = append(alters, Alter{SQL: alterSQLAddForeignKey, Safe: true, Pool: tableSchema.mysqlPoolName, engine: engine})
	}

	has = true
	return has, alters
}

func getForeignKeys(engine *engineImplementation, createTableDB string, tableName string, poolName string) map[string]*foreignIndex {
	var rows2 []foreignKeyDB
	query := "SELECT CONSTRAINT_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_TABLE_SCHEMA " +
		"FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_SCHEMA IS NOT NULL " +
		"AND TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'"
	pool := engine.GetMysql(poolName)
	results, def := pool.Query(fmt.Sprintf(query, pool.GetPoolConfig().GetDatabase(), tableName))
	defer def()
	for results.Next() {
		var row foreignKeyDB
		results.Scan(&row.ConstraintName, &row.ColumnName, &row.ReferencedTableName, &row.ReferencedTableSchema)
		row.OnDelete = "RESTRICT"
		for _, line := range strings.Split(createTableDB, "\n") {
			line = strings.TrimSpace(strings.TrimRight(line, ","))
			if strings.Index(line, fmt.Sprintf("CONSTRAINT `%s`", row.ConstraintName)) == 0 {
				words := strings.Split(line, " ")
				if strings.ToUpper(words[len(words)-2]) == "DELETE" {
					row.OnDelete = strings.ToUpper(words[len(words)-1])
				}
			}
		}
		rows2 = append(rows2, row)
	}
	def()
	var foreignKeysDB = make(map[string]*foreignIndex)
	for _, value := range rows2 {
		foreignKey := &foreignIndex{ParentDatabase: value.ReferencedTableSchema, Table: value.ReferencedTableName,
			Column: value.ColumnName, OnDelete: value.OnDelete}
		foreignKeysDB[value.ConstraintName] = foreignKey
	}
	return foreignKeysDB
}

func getDropForeignKeysAlter(engine *engineImplementation, tableName string, poolName string) string {
	var skip string
	var createTableDB string
	pool := engine.GetMysql(poolName)
	pool.QueryRow(NewWhere(fmt.Sprintf("SHOW CREATE TABLE `%s`", tableName)), &skip, &createTableDB)
	alter := fmt.Sprintf("ALTER TABLE `%s`.`%s`\n", pool.GetPoolConfig().GetDatabase(), tableName)
	foreignKeysDB := getForeignKeys(engine, createTableDB, tableName, poolName)
	if len(foreignKeysDB) == 0 {
		return ""
	}
	droppedForeignKeys := make([]string, 0)
	for keyName := range foreignKeysDB {
		droppedForeignKeys = append(droppedForeignKeys, fmt.Sprintf("DROP FOREIGN KEY `%s`", keyName))
	}
	alter += strings.Join(droppedForeignKeys, ",\t\n")
	alter = strings.TrimRight(alter, ",") + ";"
	return alter
}

func isTableEmpty(db sqlClient, tableName string) bool {
	/* #nosec */
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM `%s` LIMIT 1", tableName))
	defer func() {
		_ = rows.Close()
	}()
	checkError(err)
	return !rows.Next()
}

func buildCreateForeignKeySQL(keyName string, definition *foreignIndex) string {
	/* #nosec */
	return fmt.Sprintf("ADD CONSTRAINT `%s` FOREIGN KEY (`%s`) REFERENCES `%s`.`%s` (`ID`) ON DELETE %s",
		keyName, definition.Column, definition.ParentDatabase, definition.Table, definition.OnDelete)
}

func checkColumn(engine *engineImplementation, schema *tableSchema, field *reflect.StructField, indexes map[string]*index,
	foreignKeys map[string]*foreignIndex, prefix string) ([][2]string, error) {
	var definition string
	var addNotNullIfNotSet bool
	addDefaultNullIfNullable := true
	defaultValue := "nil"
	var typeAsString = field.Type.String()
	columnName := prefix + field.Name

	attributes := schema.tags[columnName]
	version := schema.GetMysql(engine).GetPoolConfig().GetVersion()

	_, has := attributes["ignore"]
	if has {
		return nil, nil
	}

	keys := []string{"index", "unique"}
	var refOneSchema *tableSchema
	for _, key := range keys {
		indexAttribute, has := attributes[key]
		unique := key == "unique"
		if key == "index" && field.Type.Kind() == reflect.Ptr {
			refOneSchema = getTableSchema(engine.registry, field.Type.Elem())
			if refOneSchema != nil && !refOneSchema.hasUUID {
				_, hasSkipFK := attributes["skip_FK"]
				if !hasSkipFK {
					pool := refOneSchema.GetMysql(engine)
					foreignKey := &foreignIndex{Column: prefix + field.Name, Table: refOneSchema.tableName,
						ParentDatabase: pool.GetPoolConfig().GetDatabase(), OnDelete: "RESTRICT"}
					name := fmt.Sprintf("%s:%s:%s", pool.GetPoolConfig().GetDatabase(), schema.tableName, prefix+field.Name)
					foreignKeys[name] = foreignKey
				}
			}
		}

		if has {
			indexColumns := strings.Split(indexAttribute, ",")
			for _, value := range indexColumns {
				indexColumn := strings.Split(value, ":")
				location := 1
				if len(indexColumn) > 1 {
					userLocation, err := strconv.Atoi(indexColumn[1])
					if err != nil {
						return nil, fmt.Errorf("invalid index position '%s' in index '%s'", indexColumn[1], indexColumn[0])
					}
					location = userLocation
				}
				current, has := indexes[indexColumn[0]]
				if !has {
					current = &index{Unique: unique, Columns: map[int]string{location: prefix + field.Name}}
					indexes[indexColumn[0]] = current
				} else {
					current.Columns[location] = prefix + field.Name
				}
			}
		}
	}

	if refOneSchema != nil && !refOneSchema.hasUUID {
		hasValidIndex := false
		for _, i := range indexes {
			if i.Columns[1] == columnName {
				hasValidIndex = true
				break
			}
		}
		if !hasValidIndex {
			_, hasSkipFK := attributes["skip_FK"]
			if !hasSkipFK {
				indexes[columnName] = &index{Unique: false, Columns: map[int]string{1: columnName}}
			}
		}
	}

	required, hasRequired := attributes["required"]
	isRequired := hasRequired && required == "true"

	var err error
	switch typeAsString {
	case "uint",
		"uint8",
		"uint32",
		"uint64",
		"int8",
		"int16",
		"int32",
		"int64",
		"int":
		definition, addNotNullIfNotSet, defaultValue = handleInt(version, typeAsString, attributes, false)
	case "*uint",
		"*uint8",
		"*uint32",
		"*uint64",
		"*int8",
		"*int16",
		"*int32",
		"*int64",
		"*int":
		definition, addNotNullIfNotSet, defaultValue = handleInt(version, typeAsString, attributes, true)
	case "uint16":
		if attributes["year"] == "true" {
			if version == 5 {
				return [][2]string{{columnName, fmt.Sprintf("`%s` year(4) NOT NULL DEFAULT '0000'", columnName)}}, nil
			}
			return [][2]string{{columnName, fmt.Sprintf("`%s` year NOT NULL DEFAULT '0000'", columnName)}}, nil
		}
		definition, addNotNullIfNotSet, defaultValue = handleInt(version, typeAsString, attributes, false)
	case "*uint16":
		if attributes["year"] == "true" {
			if version == 5 {
				return [][2]string{{columnName, fmt.Sprintf("`%s` year(4) DEFAULT NULL", columnName)}}, nil
			}
			return [][2]string{{columnName, fmt.Sprintf("`%s` year DEFAULT NULL", columnName)}}, nil
		}
		definition, addNotNullIfNotSet, defaultValue = handleInt(version, typeAsString, attributes, true)
	case "bool":
		if columnName == "FakeDelete" {
			return nil, nil
		}
		definition, addNotNullIfNotSet, defaultValue = "tinyint(1)", true, "'0'"
	case "*bool":
		definition, addNotNullIfNotSet, defaultValue = "tinyint(1)", false, "nil"
	case "string", "[]string":
		definition, addNotNullIfNotSet, addDefaultNullIfNullable, defaultValue, err = handleString(version, engine.registry, attributes, !isRequired)
		if err != nil {
			return nil, err
		}
	case "float32":
		definition, addNotNullIfNotSet, defaultValue = handleFloat("float", attributes, false)
	case "float64":
		definition, addNotNullIfNotSet, defaultValue = handleFloat("double", attributes, false)
	case "*float32":
		definition, addNotNullIfNotSet, defaultValue = handleFloat("float", attributes, true)
	case "*float64":
		definition, addNotNullIfNotSet, defaultValue = handleFloat("double", attributes, true)
	case "time.Time":
		definition, addNotNullIfNotSet, addDefaultNullIfNullable, defaultValue = handleTime(attributes, false)
	case "*time.Time":
		definition, addNotNullIfNotSet, addDefaultNullIfNullable, defaultValue = handleTime(attributes, true)
	case "[]uint8":
		definition, addDefaultNullIfNullable = handleBlob(attributes)
	case "*beeorm.CachedQuery":
		return nil, nil
	default:
		kind := field.Type.Kind().String()
		if kind == "struct" {
			subFieldPrefix := prefix
			//if !field.Anonymous {
			//	subFieldPrefix += field.Name
			//}
			structFields, err := checkStruct(schema, engine, field.Type, indexes, foreignKeys, field, subFieldPrefix)
			checkError(err)
			return structFields, nil
		} else if kind == "ptr" {
			subSchema := getTableSchema(engine.registry, field.Type.Elem())
			if subSchema != nil {
				definition = handleReferenceOne(version, subSchema, attributes)
				addNotNullIfNotSet = false
				addDefaultNullIfNullable = true
			} else {
				definition = "json"
			}
		} else {
			definition = "json"
		}
	}
	isNotNull := false
	if addNotNullIfNotSet || isRequired {
		definition += " NOT NULL"
		isNotNull = true
	}
	if defaultValue != "nil" && columnName != "ID" {
		definition += " DEFAULT " + defaultValue
	} else if !isNotNull && addDefaultNullIfNullable {
		definition += " DEFAULT NULL"
	}
	return [][2]string{{columnName, fmt.Sprintf("`%s` %s", columnName, definition)}}, nil
}

func handleInt(version int, typeAsString string, attributes map[string]string, nullable bool) (string, bool, string) {
	if nullable {
		typeAsString = typeAsString[1:]
		return convertIntToSchema(version, typeAsString, attributes), false, "nil"
	}
	return convertIntToSchema(version, typeAsString, attributes), true, "'0'"
}

func handleFloat(floatDefinition string, attributes map[string]string, nullable bool) (string, bool, string) {
	decimal, hasDecimal := attributes["decimal"]
	var definition string
	defaultValue := "'0'"
	if hasDecimal {
		decimalArgs := strings.Split(decimal, ",")
		definition = fmt.Sprintf("decimal(%s,%s)", decimalArgs[0], decimalArgs[1])
		defaultValue = fmt.Sprintf("'%s'", fmt.Sprintf("%."+decimalArgs[1]+"f", float32(0)))
	} else {
		definition = floatDefinition
	}
	unsigned, hasUnsigned := attributes["unsigned"]
	if hasUnsigned && unsigned == "true" {
		definition += " unsigned"
	}
	if nullable {
		return definition, false, "nil"
	}
	return definition, true, defaultValue
}

func handleBlob(attributes map[string]string) (string, bool) {
	definition := "blob"
	if attributes["mediumblob"] == "true" {
		definition = "mediumblob"
	}
	if attributes["longblob"] == "true" {
		definition = "longblob"
	}

	return definition, false
}

func handleString(version int, registry *validatedRegistry, attributes map[string]string, nullable bool) (string, bool, bool, string, error) {
	var definition string
	enum, hasEnum := attributes["enum"]
	if hasEnum {
		return handleSetEnum(version, registry, "enum", enum, nullable)
	}
	set, haSet := attributes["set"]
	if haSet {
		return handleSetEnum(version, registry, "set", set, nullable)
	}
	length, hasLength := attributes["length"]
	if !hasLength {
		length = "255"
	}
	addDefaultNullIfNullable := true
	defaultValue := "nil"
	if !nullable {
		defaultValue = "''"
	}
	if length == "max" {
		definition = "mediumtext"
		if version == 8 {
			encoding := registry.registry.defaultEncoding
			definition += " CHARACTER SET " + encoding + " COLLATE " + encoding + "_" + registry.registry.defaultCollate
		}
		addDefaultNullIfNullable = false
		defaultValue = "nil"
	} else {
		i, err := strconv.Atoi(length)
		if err != nil || i > 65535 {
			return "", false, false, "", fmt.Errorf("invalid max string: %s", length)
		}
		if version == 5 {
			definition = fmt.Sprintf("varchar(%s)", strconv.Itoa(i))
		} else {
			definition = fmt.Sprintf("varchar(%s) CHARACTER SET %s COLLATE %s_"+registry.registry.defaultCollate, strconv.Itoa(i),
				registry.registry.defaultEncoding, registry.registry.defaultEncoding)
		}
	}
	return definition, !nullable, addDefaultNullIfNullable, defaultValue, nil
}

func handleSetEnum(version int, registry *validatedRegistry, fieldType string, attribute string, nullable bool) (string, bool, bool, string, error) {
	if registry.enums == nil || registry.enums[attribute] == nil {
		return "", false, false, "", fmt.Errorf("unregistered enum %s", attribute)
	}
	enum := registry.enums[attribute]
	var definition = fieldType + "("
	for key, value := range enum.GetFields() {
		if key > 0 {
			definition += ","
		}
		definition += fmt.Sprintf("'%s'", value)
	}
	definition += ")"
	if version == 8 {
		encoding := registry.registry.defaultEncoding
		definition += " CHARACTER SET " + encoding + " COLLATE " + encoding + "_0900_ai_ci"
	}
	defaultValue := "nil"
	if !nullable {
		defaultValue = fmt.Sprintf("'%s'", enum.GetDefault())
	}
	return definition, !nullable, true, defaultValue, nil
}

func handleTime(attributes map[string]string, nullable bool) (string, bool, bool, string) {
	t := attributes["time"]
	defaultValue := "nil"
	if t == "true" {
		if !nullable {
			defaultValue = "'1000-01-01 00:00:00'"
		}
		return "datetime", !nullable, true, defaultValue
	}
	if !nullable {
		defaultValue = "'0001-01-01'"
	}
	return "date", !nullable, true, defaultValue
}

func handleReferenceOne(version int, schema *tableSchema, attributes map[string]string) string {
	return convertIntToSchema(version, schema.t.Field(1).Type.String(), attributes)
}

func convertIntToSchema(version int, typeAsString string, attributes map[string]string) string {
	switch typeAsString {
	case "uint":
		if version == 8 {
			return "int unsigned"
		}
		return "int(10) unsigned"
	case "uint8":
		if version == 8 {
			return "tinyint unsigned"
		}
		return "tinyint(3) unsigned"
	case "uint16":
		if version == 8 {
			return "smallint unsigned"
		}
		return "smallint(5) unsigned"
	case "uint32":
		if attributes["mediumint"] == "true" {
			if version == 8 {
				return "mediumint unsigned"
			}
			return "mediumint(8) unsigned"
		}
		if version == 8 {
			return "int unsigned"
		}
		return "int(10) unsigned"
	case "uint64":
		if version == 8 {
			return "bigint unsigned"
		}
		return "bigint(20) unsigned"
	case "int8":
		if version == 8 {
			return "tinyint"
		}
		return "tinyint(4)"
	case "int16":
		if version == 8 {
			return "smallint"
		}
		return "smallint(6)"
	case "int32":
		if attributes["mediumint"] == "true" {
			if version == 8 {
				return "mediumint"
			}
			return "mediumint(9)"
		}
		if version == 8 {
			return "int"
		}
		return "int(11)"
	case "int64":
		if version == 8 {
			return "bigint"
		}
		return "bigint(20)"
	default:
		if version == 8 {
			return "int"
		}
		return "int(11)"
	}
}

func checkStruct(tableSchema *tableSchema, engine *engineImplementation, t reflect.Type, indexes map[string]*index,
	foreignKeys map[string]*foreignIndex, subField *reflect.StructField, subFieldPrefix string) ([][2]string, error) {
	columns := make([][2]string, 0, t.NumField())
	max := t.NumField() - 1
	for i := 0; i <= max; i++ {
		field := t.Field(i)
		if i == 0 && subField == nil {
			for k, v := range tableSchema.uniqueIndicesGlobal {
				current := &index{Unique: true, Columns: map[int]string{}}
				for i, l := range v {
					current.Columns[i+1] = l
				}
				indexes[k] = current
			}
			continue
		}
		prefix := subFieldPrefix
		if subField != nil && !subField.Anonymous {
			prefix += subField.Name
		}
		fieldColumns, err := checkColumn(engine, tableSchema, &field, indexes, foreignKeys, prefix)
		if err != nil {
			return nil, err
		}
		if fieldColumns != nil {
			columns = append(columns, fieldColumns...)
		}
	}
	if tableSchema.hasFakeDelete && subField == nil {
		def := fmt.Sprintf("`FakeDelete` %s unsigned NOT NULL DEFAULT '0'", strings.Split(columns[0][1], " ")[1])
		columns = append(columns, [2]string{"FakeDelete", def})
	}
	return columns, nil
}

func buildCreateIndexSQL(keyName string, definition *index) string {
	var indexColumns []string
	for i := 1; i <= 100; i++ {
		value, has := definition.Columns[i]
		if has {
			indexColumns = append(indexColumns, fmt.Sprintf("`%s`", value))
		} else {
			break
		}
	}
	indexType := "INDEX"
	if definition.Unique {
		indexType = "UNIQUE " + indexType
	}
	return fmt.Sprintf("ADD %s `%s` (%s)", indexType, keyName, strings.Join(indexColumns, ","))
}
