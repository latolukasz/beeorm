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

type PluginInterfaceSchemaCheck interface {
	PluginInterfaceSchemaCheck(engine Engine, schema EntitySchema) (alters []Alter, keepTables map[string][]string)
}

type PluginInterfaceEntityFlushed interface {
	PluginInterfaceEntityFlushed(engine Engine, data *EntitySQLFlush, cacheFlusher FlusherCacheSetter)
}
