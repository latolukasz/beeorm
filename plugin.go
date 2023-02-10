package beeorm

import "reflect"

type Plugin interface {
	GetCode() string
}

type PluginInterfaceInitRegistry interface {
	PluginInterfaceInitRegistry(registry *Registry)
}

type PluginInterfaceInitEntitySchema interface {
	InterfaceInitEntitySchema(schema SettableEntitySchema, registry *Registry) error
}

type PluginInterfaceSchemaStructCheck interface {
	PluginInterfaceSchemaStructCheck(engine Engine, schema EntitySchema, columns []*ColumnSchemaDefinition,
		t reflect.Type, subField *reflect.StructField, subFieldPrefix string) []*ColumnSchemaDefinition
}

type PluginInterfaceSchemaCheck interface {
	PluginInterfaceSchemaCheck(engine Engine, schema EntitySchema) (alters []Alter, keepTables map[string][]string)
}

type PluginInterfaceEntityFlushing interface {
	PluginInterfaceEntityFlushing(engine Engine, event EventEntityFlushing)
}

type PluginInterfaceEntityFlushed interface {
	PluginInterfaceEntityFlushed(engine Engine, event EventEntityFlushed, cacheFlusher FlusherCacheSetter)
}
