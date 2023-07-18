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
	PluginInterfaceEntityFlushing(c Context, event EventEntityFlushing)
}
type PluginInterfaceEntityFlushed interface {
	PluginInterfaceEntityFlushed(c Context, event EventEntityFlushed, cacheFlusher FlusherCacheSetter)
}

type PluginInterfaceEntitySearch interface {
	PluginInterfaceEntitySearch(c Context, schema EntitySchema, where *Where) *Where
}

type PluginInterfaceContentCreated interface {
	PluginInterfaceContentCreated(c Context)
}
