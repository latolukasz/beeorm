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
	PluginInterfaceTableSQLSchemaDefinition(c Context, sqlSchema *TableSQLSchemaDefinition) error
}

type PluginInterfaceEntityFlushing interface {
	PluginInterfaceEntityFlushing(c Context, event EntityFlushEvent)
}
type PluginInterfaceEntityFlushed interface {
	PluginInterfaceEntityFlushed(c Context, event EntityFlushedEvent)
}

type PluginInterfaceEntitySearch interface {
	PluginInterfaceEntitySearch(c Context, schema EntitySchema, where *Where) *Where
}

type PluginInterfaceContextCreated interface {
	PluginInterfaceContextCreated(c Context)
}
