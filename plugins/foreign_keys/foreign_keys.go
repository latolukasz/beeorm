package foreign_keys

import (
	"fmt"
	"sort"
	"strings"

	"github.com/latolukasz/beeorm/v2"
)

const PluginCode = "github.com/latolukasz/beeorm/plugins/foreign_keys"
const defaultTagName = "fk"
const fkColumnsOption = "fk-columns"

type Plugin struct {
	options *Options
}
type Options struct {
	TagName string
}

type ForeignKeyError struct {
	Message    string
	Constraint string
}

func (err *ForeignKeyError) Error() string {
	return err.Message
}

type foreignIndex struct {
	Column         string
	Table          string
	ParentDatabase string
	OnDelete       string
}

type foreignKeyDB struct {
	ConstraintName         string
	ColumnName             string
	ReferencedTableName    string
	ReferencedEntitySchema string
	OnDelete               string
}

func Init(options *Options) *Plugin {
	if options == nil {
		options = &Options{}
	}
	if options.TagName == "" {
		options.TagName = defaultTagName
	}
	return &Plugin{options}
}

func (p *Plugin) GetCode() string {
	return PluginCode
}

func (p *Plugin) InterfaceInitEntitySchema(schema beeorm.SettableEntitySchema, _ *beeorm.Registry) error {
	refs := schema.GetReferences()
	if len(refs) == 0 {
		return nil
	}
	globalFK := schema.GetTag("ORM", p.options.TagName, "true", "") == "true"
	fkList := make([]string, 0)
	for _, column := range refs {
		columnTag := schema.GetTag(column, p.options.TagName, "true", "")
		if globalFK && columnTag != "skip" {
			fkList = append(fkList, column)
		} else if columnTag == "true" {
			fkList = append(fkList, column)
		}
	}
	if len(fkList) > 0 {
		schema.SetPluginOption(PluginCode, fkColumnsOption, fkList)
	}
	return nil
}

func (p *Plugin) PluginInterfaceTableSQLSchemaDefinition(engine beeorm.Engine, sqlSchema beeorm.TableSQLSchemaDefinition) error {
	refs := sqlSchema.EntitySchema.GetPluginOption(PluginCode, fkColumnsOption)
	if refs == nil {
		return nil
	}
	refsMap := refs.([]string)
	foreignKeys := make(map[string]*foreignIndex)
	for _, refColumn := range refsMap {
		field, _ := sqlSchema.EntitySchema.GetType().FieldByName(refColumn)
		refOneSchema := engine.GetRegistry().GetEntitySchema(field.Type.Name())
		pool := refOneSchema.GetMysql(engine)
		foreignKey := &foreignIndex{Column: refColumn, Table: refOneSchema.GetTableName(),
			ParentDatabase: pool.GetPoolConfig().GetDatabase(), OnDelete: "RESTRICT"}
		name := fmt.Sprintf("%s:%s:%s", pool.GetPoolConfig().GetDatabase(), sqlSchema.EntitySchema.GetType(), refColumn)
		foreignKeys[name] = foreignKey // TODO only if not exists
	}
	if len(foreignKeys) == 0 {
		return nil
	}
	newForeignKeys := make([]string, 0)
	createTableForeignKeysSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s`\n", sqlSchema.EntitySchema.GetMysql(engine).GetPoolConfig().GetDatabase(), sqlSchema.EntitySchema.GetTableName())

	for keyName, foreignKey := range foreignKeys {
		newForeignKeys = append(newForeignKeys, buildCreateForeignKeySQL(keyName, foreignKey))
	}
	sort.Strings(newForeignKeys)
	for _, value := range newForeignKeys {
		createTableForeignKeysSQL += fmt.Sprintf("  %s,\n", value)
	}
	//TODO add to alter

	// TODO gdy jest usuwana tabela nalezy wpierw zrobic:
	//dropForeignKeyAlter := getDropForeignKeysAlter(engine, tableName, poolName)
	//if dropForeignKeyAlter != "" {
	//	alters = append(alters, Alter{SQL: dropForeignKeyAlter, Safe: true, Pool: poolName})
	//}
	return nil
}

func buildCreateForeignKeySQL(keyName string, definition *foreignIndex) string {
	/* #nosec */
	return fmt.Sprintf("ADD CONSTRAINT `%s` FOREIGN KEY (`%s`) REFERENCES `%s`.`%s` (`ID`) ON DELETE %s",
		keyName, definition.Column, definition.ParentDatabase, definition.Table, definition.OnDelete)
}

// TODO
func getForeignKeys(engine beeorm.Engine, createTableDB string, tableName string, poolName string) map[string]*foreignIndex {
	var rows2 []foreignKeyDB
	query := "SELECT CONSTRAINT_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_TABLE_SCHEMA " +
		"FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_SCHEMA IS NOT NULL " +
		"AND TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'"
	pool := engine.GetMysql(poolName)
	results, def := pool.Query(fmt.Sprintf(query, pool.GetPoolConfig().GetDatabase(), tableName))
	defer def()
	for results.Next() {
		var row foreignKeyDB
		results.Scan(&row.ConstraintName, &row.ColumnName, &row.ReferencedTableName, &row.ReferencedEntitySchema)
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
		foreignKey := &foreignIndex{ParentDatabase: value.ReferencedEntitySchema, Table: value.ReferencedTableName,
			Column: value.ColumnName, OnDelete: value.OnDelete}
		foreignKeysDB[value.ConstraintName] = foreignKey
	}
	return foreignKeysDB
}

// TODO pobrac
func getDropForeignKeysAlter(engine beeorm.Engine, tableName string, poolName string) string {
	var skip string
	var createTableDB string
	pool := engine.GetMysql(poolName)
	pool.QueryRow(beeorm.NewWhere(fmt.Sprintf("SHOW CREATE TABLE `%s`", tableName)), &skip, &createTableDB)
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
