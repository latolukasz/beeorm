package beeorm

import (
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type Alter struct {
	SQL  string
	Safe bool
	Pool string
}

type TableSQLSchemaDefinition struct {
	engine         *engineImplementation
	EntitySchema   EntitySchema
	EntityColumns  []*ColumnSchemaDefinition
	EntityIndexes  []*IndexSchemaDefinition
	DBTableColumns []*ColumnSchemaDefinition
	DBIndexes      []*IndexSchemaDefinition
	DBCreateSchema string
	DBEncoding     string
}

func (td *TableSQLSchemaDefinition) CreateTableSQL() string {
	pool := td.EntitySchema.GetMysql(td.engine)
	createTableSQL := fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n", pool.GetPoolConfig().GetDatabase(), td.EntitySchema.GetTableName())
	for _, value := range td.EntityColumns {
		createTableSQL += fmt.Sprintf("  %s,\n", value.Definition)
	}
	var indexDefinitions []string
	for _, indexEntity := range td.EntityIndexes {
		indexDefinitions = append(indexDefinitions, buildCreateIndexSQL(indexEntity))
	}
	sort.Strings(indexDefinitions)
	for _, value := range indexDefinitions {
		createTableSQL += fmt.Sprintf("  %s,\n", value[4:])
	}

	createTableSQL += " PRIMARY KEY (`ID`)\n"
	collate := ""
	if pool.GetPoolConfig().GetVersion() == 8 {
		collate += " COLLATE=" + td.engine.registry.registry.defaultEncoding + "_" + td.engine.registry.registry.defaultCollate
	}
	createTableSQL += fmt.Sprintf(") ENGINE=InnoDB DEFAULT CHARSET=%s%s;", td.engine.registry.registry.defaultEncoding, collate)
	return createTableSQL
}

type IndexSchemaDefinition struct {
	Name       string
	Unique     bool
	columnsMap map[int]string
}

type indexDB struct {
	Skip      sql.NullString
	NonUnique uint8
	KeyName   string
	Seq       int
	Column    string
}

func (ti *IndexSchemaDefinition) GetColumns() []string {
	columns := make([]string, len(ti.columnsMap))
	for i := 1; i <= len(columns); i++ {
		columns[i-1] = ti.columnsMap[i]
	}
	return columns
}

func (ti *IndexSchemaDefinition) SetColumns(columns []string) {
	ti.columnsMap = make(map[int]string)
	for i, column := range columns {
		ti.columnsMap[i+1] = column
	}
}

func (a Alter) Exec(engine Engine) {
	engine.GetMysql(a.Pool).Exec(a.SQL)
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
			entitySchema := getEntitySchema(engine.registry, t)
			tablesInEntities[entitySchema.mysqlPoolName][entitySchema.tableName] = true
			has, newAlters := entitySchema.GetSchemaChanges(engine)
			for _, plugin := range engine.registry.plugins {
				pluginInterfaceSchemaCheck, isPluginInterfaceSchemaCheck := plugin.(PluginInterfaceSchemaCheck)
				if isPluginInterfaceSchemaCheck {
					extraAlters, skippedTables := pluginInterfaceSchemaCheck.PluginInterfaceSchemaCheck(engine, entitySchema)
					if len(extraAlters) > 0 {
						alters = append(alters, extraAlters...)
					}
					for pool, tableNames := range skippedTables {
						for _, tableName := range tableNames {
							tablesInEntities[pool][tableName] = true
						}
					}
				}
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
				pool := engine.GetMysql(poolName)
				dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`;", pool.GetPoolConfig().GetDatabase(), tableName)
				isEmpty := isTableEmptyInPool(engine, poolName, tableName)
				alters = append(alters, Alter{SQL: dropSQL, Safe: isEmpty, Pool: poolName})
			}
		}
	}
	sort.Slice(alters, func(i int, j int) bool {
		return len(alters[i].SQL) < len(alters[j].SQL)
	})
	return alters
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

func getSchemaChanges(engine *engineImplementation, entitySchema *entitySchema) (has bool, alters []Alter) {
	indexes := make(map[string]*IndexSchemaDefinition)
	columns, err := checkStruct(entitySchema, engine, entitySchema.t, indexes, nil, "")
	checkError(err)
	indexesSlice := make([]*IndexSchemaDefinition, 0)
	for _, index := range indexes {
		indexesSlice = append(indexesSlice, index)
	}
	pool := engine.GetMysql(entitySchema.mysqlPoolName)
	var skip string
	hasTable := pool.QueryRow(NewWhere(fmt.Sprintf("SHOW TABLES LIKE '%s'", entitySchema.tableName)), &skip)
	sqlSchema := &TableSQLSchemaDefinition{
		engine:        engine,
		EntitySchema:  entitySchema,
		EntityIndexes: indexesSlice,
		DBEncoding:    engine.registry.registry.defaultEncoding,
		EntityColumns: columns}
	if hasTable {
		sqlSchema.DBTableColumns = make([]*ColumnSchemaDefinition, 0)
		pool.QueryRow(NewWhere(fmt.Sprintf("SHOW CREATE TABLE `%s`", entitySchema.tableName)), &skip, &sqlSchema.DBCreateSchema)
		lines := strings.Split(sqlSchema.DBCreateSchema, "\n")
		for x := 1; x < len(lines); x++ {
			if lines[x][2] != 96 {
				for _, field := range strings.Split(lines[x], " ") {
					if strings.HasPrefix(field, "CHARSET=") {
						sqlSchema.DBEncoding = field[8:]
					}
				}
				continue
			}
			var line = strings.TrimRight(lines[x], ",")
			line = strings.TrimLeft(line, " ")
			var columnName = strings.Split(line, "`")[1]
			sqlSchema.DBTableColumns = append(sqlSchema.DBTableColumns, &ColumnSchemaDefinition{columnName, line})
		}

		var rows []indexDB
		/* #nosec */
		results, def := pool.Query(fmt.Sprintf("SHOW INDEXES FROM `%s`", entitySchema.tableName))
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
		for _, value := range rows {
			hasCurrent := false
			for _, current := range sqlSchema.DBIndexes {
				if current.Name == value.KeyName {
					hasCurrent = true
					current.columnsMap[value.Seq] = value.Column
					break
				}
			}
			if !hasCurrent {
				current := &IndexSchemaDefinition{Name: value.KeyName, Unique: value.NonUnique == 0, columnsMap: map[int]string{value.Seq: value.Column}}
				sqlSchema.DBIndexes = append(sqlSchema.DBIndexes, current)
			}
		}
	}

	for _, plugin := range engine.registry.plugins {
		pluginInterfaceTableSQLSchemaDefinition, isPluginInterfaceTableSQLSchemaDefinition := plugin.(PluginInterfaceTableSQLSchemaDefinition)
		if isPluginInterfaceTableSQLSchemaDefinition {
			err = pluginInterfaceTableSQLSchemaDefinition.PluginInterfaceTableSQLSchemaDefinition(engine, sqlSchema)
			checkError(err)
		}
	}

	if !hasTable {
		alters = []Alter{{SQL: sqlSchema.CreateTableSQL(), Safe: true, Pool: entitySchema.mysqlPoolName}}
		has = true
		return
	}
	hasAlterEngineCharset := sqlSchema.DBEncoding != engine.registry.registry.defaultEncoding
	hasAlters := hasAlterEngineCharset
	hasAlterNormal := false

	var newColumns []string
	var changedColumns [][2]string

	for key, value := range columns {
		var tableColumn string
		if key < len(sqlSchema.DBTableColumns) {
			tableColumn = sqlSchema.DBTableColumns[key].Definition
		}
		if tableColumn == value.Definition {
			continue
		}
		hasName := -1
		hasDefinition := -1
		for z, v := range sqlSchema.DBTableColumns {
			if v.Definition == value.Definition {
				hasDefinition = z
			}
			if v.ColumnName == value.ColumnName {
				hasName = z
			}
		}
		if hasName == -1 {
			alter := fmt.Sprintf("ADD COLUMN %s", value.Definition)
			if key > 0 {
				alter += fmt.Sprintf(" AFTER `%s`", columns[key-1].ColumnName)
			}
			newColumns = append(newColumns, alter)
			hasAlters = true
		} else {
			if hasDefinition == -1 {
				alter := fmt.Sprintf("CHANGE COLUMN `%s` %s", value.ColumnName, value.Definition)
				if key > 0 {
					/* #nosec */
					alter += fmt.Sprintf(" AFTER `%s`", columns[key-1].ColumnName)
				}
				/* #nosec */
				changedColumns = append(changedColumns, [2]string{alter, fmt.Sprintf("CHANGED FROM %s", sqlSchema.DBTableColumns[hasName].Definition)})
				hasAlters = true
			} else {
				alter := fmt.Sprintf("CHANGE COLUMN `%s` %s", value.ColumnName, value.Definition)
				if key > 0 {
					alter += fmt.Sprintf(" AFTER `%s`", columns[key-1].ColumnName)
				}
				changedColumns = append(changedColumns, [2]string{alter, "CHANGED ORDER"})
				hasAlters = true
			}
		}
	}
	droppedColumns := make([]string, 0)
OUTER:
	for _, value := range sqlSchema.DBTableColumns {
		for _, v := range columns {
			if v.ColumnName == value.ColumnName {
				continue OUTER
			}
		}
		droppedColumns = append(droppedColumns, fmt.Sprintf("DROP COLUMN `%s`", value.ColumnName))
		hasAlters = true
	}

	var droppedIndexes []string
	var newIndexes []string
	for _, indexEntity := range sqlSchema.EntityIndexes {
		hasIndex := false
		for _, index := range sqlSchema.DBIndexes {
			if index.Name == indexEntity.Name {
				hasIndex = true
				addIndexSQLEntity := buildCreateIndexSQL(indexEntity)
				addIndexSQLDB := buildCreateIndexSQL(index)
				if addIndexSQLEntity != addIndexSQLDB {
					droppedIndexes = append(droppedIndexes, fmt.Sprintf("DROP INDEX `%s`", indexEntity.Name))
					newIndexes = append(newIndexes, addIndexSQLEntity)
					hasAlters = true
				}
				break
			}
		}
		if !hasIndex {
			newIndexes = append(newIndexes, buildCreateIndexSQL(indexEntity))
			hasAlters = true
		}
	}

	for _, key := range sqlSchema.DBIndexes {
		if key.Name == "PRIMARY" {
			continue
		}
		hasIndex := false
		for _, index := range sqlSchema.EntityIndexes {
			if index.Name == key.Name {
				hasIndex = true
				break
			}
		}
		if !hasIndex {
			droppedIndexes = append(droppedIndexes, fmt.Sprintf("DROP INDEX `%s`", key.Name))
			hasAlters = true
		}
	}
	if !hasAlters {
		return
	}

	alterSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s`\n", pool.GetPoolConfig().GetDatabase(), entitySchema.tableName)
	newAlters := make([]string, 0)
	comments := make([]string, 0)

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
	sort.Strings(newIndexes)
	for _, value := range newIndexes {
		newAlters = append(newAlters, fmt.Sprintf("    %s", value))
		comments = append(comments, "")
		hasAlterNormal = true
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

	alters = make([]Alter, 0)
	if hasAlterNormal {
		safe := false
		if len(droppedColumns) == 0 && len(changedColumns) == 0 {
			safe = true
		} else {
			db := entitySchema.GetMysql(engine)
			isEmpty := isTableEmpty(db.client, entitySchema.tableName)
			safe = isEmpty
		}
		alters = append(alters, Alter{SQL: alterSQL, Safe: safe, Pool: entitySchema.mysqlPoolName})
	} else if hasAlterEngineCharset {
		collate := ""
		if pool.GetPoolConfig().GetVersion() == 8 {
			collate += " COLLATE=" + engine.registry.registry.defaultEncoding + "_" + engine.registry.registry.defaultCollate
		}
		alterSQL += fmt.Sprintf(" ENGINE=InnoDB DEFAULT CHARSET=%s%s;", engine.registry.registry.defaultEncoding, collate)
		alters = append(alters, Alter{SQL: alterSQL, Safe: true, Pool: entitySchema.mysqlPoolName})
	}
	has = true
	return has, alters
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

func checkColumn(engine *engineImplementation, schema *entitySchema, field *reflect.StructField, indexes map[string]*IndexSchemaDefinition, prefix string) ([]*ColumnSchemaDefinition, error) {
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
	for _, key := range keys {
		indexAttribute, has := attributes[key]
		unique := key == "unique"
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
					current = &IndexSchemaDefinition{Name: indexColumn[0], Unique: unique, columnsMap: map[int]string{location: prefix + field.Name}}
					indexes[indexColumn[0]] = current
				} else {
					current.columnsMap[location] = prefix + field.Name
				}
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
				return []*ColumnSchemaDefinition{{columnName, fmt.Sprintf("`%s` year(4) NOT NULL DEFAULT '0000'", columnName)}}, nil
			}
			return []*ColumnSchemaDefinition{{columnName, fmt.Sprintf("`%s` year NOT NULL DEFAULT '0000'", columnName)}}, nil
		}
		definition, addNotNullIfNotSet, defaultValue = handleInt(version, typeAsString, attributes, false)
	case "*uint16":
		if attributes["year"] == "true" {
			if version == 5 {
				return []*ColumnSchemaDefinition{{columnName, fmt.Sprintf("`%s` year(4) DEFAULT NULL", columnName)}}, nil
			}
			return []*ColumnSchemaDefinition{{columnName, fmt.Sprintf("`%s` year DEFAULT NULL", columnName)}}, nil
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
			structFields, err := checkStruct(schema, engine, field.Type, indexes, field, subFieldPrefix)
			checkError(err)
			return structFields, nil
		} else if kind == "ptr" {
			subSchema := getEntitySchema(engine.registry, field.Type.Elem())
			if subSchema != nil {
				definition = handleReferenceOne(version, subSchema)
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
	return []*ColumnSchemaDefinition{{columnName, fmt.Sprintf("`%s` %s", columnName, definition)}}, nil
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

func handleReferenceOne(version int, schema *entitySchema) string {
	idType, idAttributes := schema.getIDType()
	return convertIntToSchema(version, idType, idAttributes)
}

func convertIntToSchema(version int, typeAsString string, attributes Bind) string {
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

func (entitySchema *entitySchema) getIDType() (idType string, idAttributes Bind) {
	idAttributes = Bind{}
	idType = "uint64"
	switch entitySchema.getTag("id", "uint", "uint") {
	case "tinyint":
		idType = "uint8"
		break
	case "smallint":
		idType = "uint16"
		break
	case "mediumint":
		idType = "uint32"
		idAttributes["mediumint"] = "true"
		break
	case "int":
		idType = "uint32"
		break
	}
	return idType, idAttributes
}

type ColumnSchemaDefinition struct {
	ColumnName string
	Definition string
}

func checkStruct(entitySchema *entitySchema, engine *engineImplementation, t reflect.Type, indexes map[string]*IndexSchemaDefinition,
	subField *reflect.StructField, subFieldPrefix string) ([]*ColumnSchemaDefinition, error) {
	columns := make([]*ColumnSchemaDefinition, 0)
	if subField == nil {
		version := entitySchema.GetMysql(engine).GetPoolConfig().GetVersion()
		idType, idAttributes := entitySchema.getIDType()
		idColumnSchema := convertIntToSchema(version, idType, idAttributes) + " NOT NULL"
		columns = append(columns, &ColumnSchemaDefinition{"ID", "`ID` " + idColumnSchema + " AUTO_INCREMENT"})
		_, hasID := t.FieldByName("ID")
		if hasID {
			return nil, errors.New("field with name ID not allowed")
		}
	}
	max := t.NumField() - 1
	for i := 0; i <= max; i++ {
		field := t.Field(i)
		if i == 0 && subField == nil {
			for k, v := range entitySchema.uniqueIndicesGlobal {
				current := &IndexSchemaDefinition{Name: k, Unique: true, columnsMap: map[int]string{}}
				for i, l := range v {
					current.columnsMap[i+1] = l
				}
				indexes[k] = current
			}
			continue
		}
		prefix := subFieldPrefix
		if subField != nil && !subField.Anonymous {
			prefix += subField.Name
		}
		fieldColumns, err := checkColumn(engine, entitySchema, &field, indexes, prefix)
		if err != nil {
			return nil, err
		}
		if fieldColumns != nil {
			columns = append(columns, fieldColumns...)
		}
	}
	if entitySchema.hasFakeDelete && subField == nil {
		def := fmt.Sprintf("`FakeDelete` %s unsigned NOT NULL DEFAULT '0'", strings.Split(columns[0].Definition, " ")[1])
		columns = append(columns, &ColumnSchemaDefinition{"FakeDelete", def})
	}
	return columns, nil
}

func buildCreateIndexSQL(index *IndexSchemaDefinition) string {
	var indexColumns []string
	for i := 1; i <= 100; i++ {
		value, has := index.columnsMap[i]
		if has {
			indexColumns = append(indexColumns, fmt.Sprintf("`%s`", value))
		} else {
			break
		}
	}
	indexType := "INDEX"
	if index.Unique {
		indexType = "UNIQUE " + indexType
	}
	return fmt.Sprintf("ADD %s `%s` (%s)", indexType, index.Name, strings.Join(indexColumns, ","))
}
