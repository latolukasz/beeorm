package beeorm

type PluginInterfaceValidateRegistry interface {
	ValidateRegistry(engine EngineSetter, registry Registry) error
}

type PluginInterfaceInitRegistryFromYaml interface {
	InitRegistryFromYaml(registry Registry, yaml map[string]any) error
}

type PluginInterfaceValidateEntitySchema interface {
	ValidateEntitySchema(schema EntitySchemaSetter) error
}
