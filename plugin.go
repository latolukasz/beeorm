package beeorm

type Plugin interface {
	GetCode() string
}

type PluginInterfaceInitTableSchema interface {
	InterfaceInitTableSchema(schema SettableTableSchema, registry *Registry) error
}

type PluginInterfaceRegistryValidate interface {
	InterfaceRegistryValidate(registry *Registry, validatedRegistry ValidatedRegistry) error
}

type PluginInterfaceSchemaCheck interface {
	PluginInterfaceSchemaCheck(engine Engine, schema TableSchema) (alters []Alter, keepTables map[string][]string)
}

type PluginInterfaceEntityFlushed interface {
	PluginInterfaceEntityFlushed(engine Engine, data *EntitySQLFlush, cacheFlusher FlusherCacheSetter)
}
