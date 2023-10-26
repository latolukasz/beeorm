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
	context        Context
	EntitySchema   EntitySchema
	EntityColumns  []*ColumnSchemaDefinition
	EntityIndexes  []*IndexSchemaDefinition
	DBTableColumns []*ColumnSchemaDefinition
	DBIndexes      []*IndexSchemaDefinition
	DBCreateSchema string
	DBEncoding     string
	Engine         string
	PreAlters      []Alter
	PostAlters     []Alter
}

func GetAlters(c Context) (alters []Alter) {
	pre, alters, post := getAlters(c)
	final := pre
	final = append(final, alters...)
	final = append(final, post...)
	return final
}

func (td *TableSQLSchemaDefinition) CreateTableSQL() string {
	pool := td.EntitySchema.GetDB()
	createTableSQL := fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n", pool.GetConfig().GetDatabaseName(), td.EntitySchema.GetTableName())
	archived := td.EntitySchema.(*entitySchema).archived
	for i, value := range td.EntityColumns {
		if archived && i == 0 {
			createTableSQL += fmt.Sprintf("  %s AUTO_INCREMENT,\n", value.Definition)
			continue
		}
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
	collate := " COLLATE=" + pool.GetConfig().GetOptions().DefaultEncoding + "_" +
		pool.GetConfig().GetOptions().DefaultCollate
	engine := "InnoDB"
	if archived {
		engine = "ARCHIVE"
	}
	createTableSQL += fmt.Sprintf(") ENGINE=%s DEFAULT CHARSET=%s%s", engine, pool.GetConfig().GetOptions().DefaultEncoding, collate)
	if archived {
		createTableSQL += " ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8"
	}
	createTableSQL += ";"
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

func (a Alter) Exec(c Context) {
	c.Engine().DB(a.Pool).Exec(c, a.SQL)
}

func getAlters(c Context) (preAlters, alters, postAlters []Alter) {
	tablesInDB := make(map[string]map[string]bool)
	tablesInEntities := make(map[string]map[string]bool)

	for poolName, pool := range c.Engine().Registry().DBPools() {
		tablesInDB[poolName] = make(map[string]bool)
		tables := getAllTables(pool.GetDBClient())
		for _, table := range tables {
			tablesInDB[poolName][table] = true
		}
		tablesInEntities[poolName] = make(map[string]bool)
	}
	alters = make([]Alter, 0)
	for _, t := range c.Engine().Registry().Entities() {
		schema := c.Engine().Registry().EntitySchema(t).(*entitySchema)
		db := schema.GetDB()
		tablesInEntities[db.GetConfig().GetCode()][schema.GetTableName()] = true
		pre, middle, post := getSchemaChanges(c, schema)
		preAlters = append(preAlters, pre...)
		alters = append(alters, middle...)
		postAlters = append(postAlters, post...)
	}
	for poolName, tables := range tablesInDB {
		for tableName := range tables {
			_, has := tablesInEntities[poolName][tableName]
			if !has {
				_, has = c.Engine().Registry().getDBTables()[poolName][tableName]
				if !has {
					pool := c.Engine().DB(poolName)
					dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`;", pool.GetConfig().GetDatabaseName(), tableName)
					isEmpty := isTableEmptyInPool(c, poolName, tableName)
					alters = append(alters, Alter{SQL: dropSQL, Safe: isEmpty, Pool: poolName})
				}
			}
		}
	}
	sort.Slice(alters, func(i int, j int) bool {
		return len(alters[i].SQL) < len(alters[j].SQL)
	})
	return
}

func isTableEmptyInPool(c Context, poolName string, tableName string) bool {
	return isTableEmpty(c.Engine().DB(poolName).GetDBClient(), tableName)
}

func getAllTables(db DBClient) []string {
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

func getSchemaChanges(c Context, entitySchema *entitySchema) (preAlters, alters, postAlters []Alter) {
	indexes := make(map[string]*IndexSchemaDefinition)
	columns, err := checkStruct(c.Engine(), entitySchema, entitySchema.GetType(), indexes, nil, "", -1)
	checkError(err)
	indexesSlice := make([]*IndexSchemaDefinition, 0)
	for _, index := range indexes {
		indexesSlice = append(indexesSlice, index)
	}
	pool := entitySchema.GetDB()
	var skip string
	hasTable := pool.QueryRow(c, fmt.Sprintf("SHOW TABLES LIKE '%s'", entitySchema.GetTableName()), []any{&skip})
	sqlSchema := &TableSQLSchemaDefinition{
		context:       c,
		EntitySchema:  entitySchema,
		EntityIndexes: indexesSlice,
		DBEncoding:    pool.GetConfig().GetOptions().DefaultEncoding,
		EntityColumns: columns}
	if hasTable {
		sqlSchema.DBTableColumns = make([]*ColumnSchemaDefinition, 0)
		pool.QueryRow(c, fmt.Sprintf("SHOW CREATE TABLE `%s`", entitySchema.GetTableName()), []any{&skip, &sqlSchema.DBCreateSchema})
		lines := strings.Split(sqlSchema.DBCreateSchema, "\n")
		for x := 1; x < len(lines); x++ {
			if lines[x][2] != 96 {
				for _, field := range strings.Split(lines[x], " ") {
					if strings.HasPrefix(field, "CHARSET=") {
						sqlSchema.DBEncoding = field[8:]
					} else if strings.HasPrefix(field, "ENGINE=") {
						sqlSchema.Engine = field[7:]
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
		results, def := pool.Query(c, fmt.Sprintf("SHOW INDEXES FROM `%s`", entitySchema.GetTableName()))
		defer def()
		for results.Next() {
			var row indexDB
			results.Scan(&row.Skip, &row.NonUnique, &row.KeyName, &row.Seq, &row.Column, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip)
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
	if sqlSchema.PreAlters != nil {
		preAlters = append(preAlters, sqlSchema.PreAlters...)
	}
	if !hasTable {
		alters = append(alters, Alter{SQL: sqlSchema.CreateTableSQL(), Safe: true, Pool: entitySchema.GetDB().GetConfig().GetCode()})
		if sqlSchema.PostAlters != nil {
			postAlters = append(postAlters, sqlSchema.PostAlters...)
		}
		return
	}
	hasAlterEngineCharset := sqlSchema.DBEncoding != pool.GetConfig().GetOptions().DefaultEncoding
	hasAlterEngine := (!entitySchema.archived && sqlSchema.Engine != "InnoDB") || (entitySchema.archived && sqlSchema.Engine != "ARCHIVE")
	hasAlters := hasAlterEngineCharset || hasAlterEngine
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
		if sqlSchema.PostAlters != nil {
			postAlters = append(postAlters, sqlSchema.PostAlters...)
		}
		return
	}

	alterSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s`\n", pool.GetConfig().GetDatabaseName(), entitySchema.GetTableName())
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

	if hasAlterNormal {
		safe := false
		if len(droppedColumns) == 0 && len(changedColumns) == 0 {
			safe = true
		} else {
			db := entitySchema.GetDB()
			isEmpty := isTableEmpty(db.GetDBClient(), entitySchema.GetTableName())
			safe = isEmpty
		}
		alters = append(alters, Alter{SQL: alterSQL, Safe: safe, Pool: entitySchema.GetDB().GetConfig().GetCode()})
	} else if hasAlterEngineCharset || hasAlterEngine {
		collate := " COLLATE=" + pool.GetConfig().GetOptions().DefaultEncoding + "_" + pool.GetConfig().GetOptions().DefaultCollate
		alterSQL += " ENGINE="
		if entitySchema.archived {
			alterSQL += "ARCHIVE"
		} else {
			alterSQL += "InnoDB"
		}
		alterSQL += fmt.Sprintf(" DEFAULT CHARSET=%s%s;", pool.GetConfig().GetOptions().DefaultEncoding, collate)
		alters = append(alters, Alter{SQL: alterSQL, Safe: true, Pool: entitySchema.GetDB().GetConfig().GetCode()})
	}
	if sqlSchema.PostAlters != nil {
		postAlters = append(postAlters, sqlSchema.PostAlters...)
	}
	return
}

func isTableEmpty(db DBClient, tableName string) bool {
	/* #nosec */
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM `%s` LIMIT 1", tableName))
	defer func() {
		_ = rows.Close()
	}()
	checkError(err)
	return !rows.Next()
}

func checkColumn(engine Engine, schema *entitySchema, field *reflect.StructField, indexes map[string]*IndexSchemaDefinition, prefix string) ([]*ColumnSchemaDefinition, error) {
	var definition string
	var addNotNullIfNotSet bool
	addDefaultNullIfNullable := true
	defaultValue := "nil"
	columnName := prefix + field.Name
	attributes := schema.tags[columnName]
	_, has := attributes["ignore"]
	if has {
		return nil, nil
	}
	var columns []*ColumnSchemaDefinition
	isArray := false
	arrayLen := 0
	fieldType := field.Type
	if field.Type.Kind().String() == "array" {
		fieldType = fieldType.Elem()
		isArray = true
		arrayLen = field.Type.Len()
		if arrayLen > 100 {
			return nil, fmt.Errorf("array len for column %s exceeded limit of 100", columnName)
		}
	}

	for i := 0; i <= arrayLen; i++ {
		columnName = prefix + field.Name
		if isArray {
			if i == arrayLen {
				break
			}
			columnName += "_" + strconv.Itoa(i+1)
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
		typeAsString := fieldType.String()
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
			definition, addNotNullIfNotSet, defaultValue = handleInt(typeAsString, attributes, false)
		case "*uint",
			"*uint8",
			"*uint32",
			"*uint64",
			"*int8",
			"*int16",
			"*int32",
			"*int64",
			"*int":
			definition, addNotNullIfNotSet, defaultValue = handleInt(typeAsString, attributes, true)
		case "uint16":
			definition, addNotNullIfNotSet, defaultValue = handleInt(typeAsString, attributes, false)
		case "*uint16":
			definition, addNotNullIfNotSet, defaultValue = handleInt(typeAsString, attributes, true)
		case "bool":
			definition, addNotNullIfNotSet, defaultValue = "tinyint(1)", true, "'0'"
		case "*bool":
			definition, addNotNullIfNotSet, defaultValue = "tinyint(1)", false, "nil"
		case "string":
			definition, addNotNullIfNotSet, addDefaultNullIfNullable, defaultValue, err = handleString(schema, attributes, !isRequired)
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
		default:
			kind := fieldType.Kind().String()
			if kind == "struct" {
				subFieldPrefix := prefix
				arrayIndex := -1
				if isArray {
					arrayIndex = i + 1
				}
				structFields, err := checkStruct(engine, schema, fieldType, indexes, field, subFieldPrefix, arrayIndex)
				checkError(err)
				columns = append(columns, structFields...)
				continue
			} else if fieldType.Implements(reflect.TypeOf((*referenceInterface)(nil)).Elem()) {
				definition, addNotNullIfNotSet, defaultValue = handleInt("uint64", attributes, !isRequired)
			} else if fieldType.Implements(reflect.TypeOf((*EnumValues)(nil)).Elem()) {
				def := reflect.New(fieldType).Interface().(EnumValues)
				definition, addNotNullIfNotSet, addDefaultNullIfNullable, defaultValue, err = handleSetEnum("ENUM", schema, def, !isRequired)
				if err != nil {
					return nil, err
				}
			} else if kind == "slice" && fieldType.Elem().Implements(reflect.TypeOf((*EnumValues)(nil)).Elem()) {
				def := reflect.New(fieldType.Elem()).Interface().(EnumValues)
				definition, addNotNullIfNotSet, addDefaultNullIfNullable, defaultValue, err = handleSetEnum("SET", schema, def, !isRequired)
			} else {
				return nil, fmt.Errorf("field type %s is not supported, consider adding  tag `ignore`", field.Type.String())
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
		if schema.archived && prefix == "" && columnName == "ID" {
			definition += " AUTO_INCREMENT"
		}
		columns = append(columns, &ColumnSchemaDefinition{columnName, fmt.Sprintf("`%s` %s", columnName, definition)})
	}
	return columns, nil
}

func handleInt(typeAsString string, attributes map[string]string, nullable bool) (string, bool, string) {
	if nullable {
		typeAsString = typeAsString[1:]
		return convertIntToSchema(typeAsString, attributes), false, "nil"
	}
	return convertIntToSchema(typeAsString, attributes), true, "'0'"
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

func handleString(schema *entitySchema, attributes map[string]string, nullable bool) (string, bool, bool, string, error) {
	dbOptions := schema.GetDB().GetConfig().GetOptions()
	var definition string
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
		encoding := dbOptions.DefaultEncoding
		definition += " CHARACTER SET " + encoding + " COLLATE " + encoding + "_" + dbOptions.DefaultCollate
		addDefaultNullIfNullable = false
		defaultValue = "nil"
	} else {
		i, err := strconv.Atoi(length)
		if err != nil || i > 65535 {
			return "", false, false, "", fmt.Errorf("invalid max string: %s", length)
		}
		definition = fmt.Sprintf("varchar(%s) CHARACTER SET %s COLLATE %s_%s", strconv.Itoa(i),
			dbOptions.DefaultEncoding, dbOptions.DefaultEncoding, dbOptions.DefaultCollate)
	}
	return definition, !nullable, addDefaultNullIfNullable, defaultValue, nil
}
func handleSetEnum(fieldType string, schema *entitySchema, def EnumValues, nullable bool) (string, bool, bool, string, error) {
	enumDef := initEnumDefinition(def.EnumValues(), !nullable)
	if len(enumDef.GetFields()) == 0 {
		return "", false, false, "", errors.New("empty enum not allowed")
	}
	var definition = fieldType + "("
	for key, value := range enumDef.GetFields() {
		if key > 0 {
			definition += ","
		}
		definition += fmt.Sprintf("'%s'", value)
	}
	definition += ")"
	encoding := schema.GetDB().GetConfig().GetOptions().DefaultEncoding
	definition += " CHARACTER SET " + encoding + " COLLATE " + encoding + "_0900_ai_ci"
	defaultValue := "nil"
	if !nullable {
		defaultValue = fmt.Sprintf("'%s'", enumDef.GetFields()[0])
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

func convertIntToSchema(typeAsString string, attributes Meta) string {
	switch typeAsString {
	case "uint":
		return "int unsigned"
	case "uint8":
		return "tinyint unsigned"
	case "uint16":
		return "smallint unsigned"
	case "uint32":
		if attributes["mediumint"] == "true" {
			return "mediumint unsigned"
		}
		return "int unsigned"
	case "uint64":
		return "bigint unsigned"
	case "int8":
		return "tinyint"
	case "int16":
		return "smallint"
	case "int32":
		if attributes["mediumint"] == "true" {
			return "mediumint"
		}
		return "int"
	case "int64":
		return "bigint"
	default:
		return "int"
	}
}

type ColumnSchemaDefinition struct {
	ColumnName string
	Definition string
}

func checkStruct(engine Engine, entitySchema *entitySchema, t reflect.Type, indexes map[string]*IndexSchemaDefinition,
	subField *reflect.StructField, subFieldPrefix string, arrayIndex int) ([]*ColumnSchemaDefinition, error) {
	columns := make([]*ColumnSchemaDefinition, 0)
	if subField == nil {
		f, hasID := t.FieldByName("ID")
		if !hasID || len(f.Index) != 1 || f.Index[0] != 0 {
			return nil, errors.New("field ID on position 1 is missing")
		}
		idType := f.Type.String()
		if idType != "uint64" {
			return nil, errors.New("ID column must be unit64")
		}
	}
	maxFields := t.NumField() - 1
	for i := 0; i <= maxFields; i++ {
		field := t.Field(i)
		prefix := subFieldPrefix
		if subField != nil && !subField.Anonymous {
			prefix += subField.Name
			if arrayIndex > 0 {
				prefix += "_" + strconv.Itoa(arrayIndex) + "_"
			}
		}
		fieldColumns, err := checkColumn(engine, entitySchema, &field, indexes, prefix)
		if err != nil {
			return nil, err
		}
		if fieldColumns != nil {
			columns = append(columns, fieldColumns...)
		}
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
