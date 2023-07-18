package fake_delete

import (
	"strconv"
	"strings"

	"github.com/latolukasz/beeorm/v3"
)

const PluginCode = "github.com/latolukasz/beeorm/plugins/fake_delete"
const hasFakeDeleteOption = "has-fake-delete"
const defaultFieldName = "FakeDelete"
const forceDeleteMetaKey = "force-fake-delete"

type Plugin struct {
	options *Options
}
type Options struct {
	FieldName string
}

func Init(options *Options) *Plugin {
	if options == nil {
		options = &Options{}
	}
	if options.FieldName == "" {
		options.FieldName = defaultFieldName
	}
	return &Plugin{options}
}

func (p *Plugin) GetCode() string {
	return PluginCode
}

func ForceDelete(entity beeorm.Entity) {
	entity.SetMetaData(forceDeleteMetaKey, "1")
}

func (p *Plugin) InterfaceInitEntitySchema(schema beeorm.SettableEntitySchema, _ *beeorm.Registry) error {
	fakeDeleteField, has := schema.GetType().FieldByName(p.options.FieldName)
	if !has || fakeDeleteField.Type.String() != "bool" {
		return nil
	}
	ormTag := fakeDeleteField.Tag.Get("orm")
	if ormTag == "ignore" {
		return nil
	}
	schema.SetPluginOption(PluginCode, hasFakeDeleteOption, true)
	return nil
}

func (p *Plugin) PluginInterfaceTableSQLSchemaDefinition(_ beeorm.Engine, sqlSchema *beeorm.TableSQLSchemaDefinition) error {
	if sqlSchema.EntitySchema.GetPluginOption(PluginCode, hasFakeDeleteOption) != true {
		return nil
	}
	for _, column := range sqlSchema.EntityColumns {
		if column.ColumnName == "ID" {
			newDefinition := "`" + p.options.FieldName + "` "
			newDefinition += strings.Trim(column.Definition[4:], " AUTO_INCREMENT")
			for _, column2 := range sqlSchema.EntityColumns {
				if column2.ColumnName == p.options.FieldName {
					column2.Definition = newDefinition
					break
				}
			}
			break
		}
	}
	hasFirstFakeDelete := false
	for _, index := range sqlSchema.EntityIndexes {
		hasFakeDelete := false
		columns := index.GetColumns()
		if !hasFirstFakeDelete && columns[0] == p.options.FieldName {
			hasFirstFakeDelete = true
		}
		for _, column := range columns {
			if column == p.options.FieldName {
				hasFakeDelete = true
				break
			}
		}
		if !hasFakeDelete {
			columns = append(columns, p.options.FieldName)
		}
		index.SetColumns(columns)
	}
	if !hasFirstFakeDelete {
		index := &beeorm.IndexSchemaDefinition{
			Name:   p.options.FieldName,
			Unique: false,
		}
		index.SetColumns([]string{p.options.FieldName})
		sqlSchema.EntityIndexes = append(sqlSchema.EntityIndexes, index)
	}
	return nil
}

func (p *Plugin) PluginInterfaceEntityFlushing(engine beeorm.Engine, event beeorm.EventEntityFlushing) {
	schema := engine.Registry().GetEntitySchema(event.EntityName())
	if schema.GetPluginOption(PluginCode, hasFakeDeleteOption) != true {
		return
	}
	if event.Type().Is(beeorm.Delete) {
		if event.MetaData()[forceDeleteMetaKey] == "1" {
			delete(event.MetaData(), forceDeleteMetaKey)
			return
		}
		event.SetType(beeorm.Update)
		event.SetBefore(beeorm.Bind{p.options.FieldName: "0"})
		event.SetAfter(beeorm.Bind{p.options.FieldName: strconv.FormatUint(event.EntityID(), 10)})
		return
	}
	fakeDelete, has := event.After()[p.options.FieldName]
	if !has {
		return
	}
	if fakeDelete == "0" {
		event.After()[p.options.FieldName] = "0"
	} else {
		event.Before()[p.options.FieldName] = "1"
		event.After()[p.options.FieldName] = strconv.FormatUint(event.EntityID(), 10)
	}
}

func (p *Plugin) PluginInterfaceEntitySearch(_ beeorm.Engine, schema beeorm.EntitySchema, where *beeorm.Where) *beeorm.Where {
	if schema.GetPluginOption(PluginCode, hasFakeDeleteOption) != true {
		return where
	}
	fieldSQL := "`" + p.options.FieldName + "`"
	if strings.Contains(where.String(), fieldSQL) {
		return where
	}
	oldWhere := where.String()
	newWhere := fieldSQL + " = 0"
	if oldWhere != "" {
		newWhere += " AND " + oldWhere
	}
	return beeorm.NewWhere(newWhere, where.GetParameters()...)
}
