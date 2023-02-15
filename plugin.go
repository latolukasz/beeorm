package beeorm

type Plugin interface {
	GetCode() string
}

type PluginInterfaceInitRegistry interface {
	PluginInterfaceInitRegistry(registry *Registry)
}

type PluginInterfaceInitEntitySchema interface {
	InterfaceInitEntitySchema(schema SettableEntitySchema, registry *Registry) error
}

type PluginInterfaceTableSQLSchemaDefinition interface {
	PluginInterfaceTableSQLSchemaDefinition(engine Engine, sqlSchema *TableSQLSchemaDefinition) error
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
