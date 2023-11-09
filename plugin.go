package beeorm

import "reflect"

type PluginInterfaceValidateRegistry interface {
	ValidateRegistry(engine EngineSetter, registry Registry) error
}

type PluginInterfaceInitRegistryFromYaml interface {
	InitRegistryFromYaml(registry Registry, yaml map[string]any) error
}

type PluginInterfaceValidateEntitySchema interface {
	ValidateEntitySchema(schema EntitySchemaSetter) error
}

type PluginInterfaceEntityFlush interface {
	EntityFlush(schema EntitySchema, entity reflect.Value, before, after Bind, engine Engine) (AfterDBCommitAction, error)
}
