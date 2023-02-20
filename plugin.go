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

type PluginInterfaceEntityFlushing interface {
	PluginInterfaceEntityFlushing(engine Engine, event EventEntityFlushing)
}
type PluginInterfaceEntityFlushed interface {
	PluginInterfaceEntityFlushed(engine Engine, event EventEntityFlushed, cacheFlusher FlusherCacheSetter)
}

type PluginInterfaceEntitySearch interface {
	PluginInterfaceEntitySearch(engine Engine, schema EntitySchema, where *Where) *Where
}
