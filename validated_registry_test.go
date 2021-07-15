package beeorm

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

type validatedRegistryEntity struct {
	ORM
	ID uint
}

type validatedRegistryNotRegisteredEntity struct {
	ORM
	ID uint
}

func TestValidatedRegistry(t *testing.T) {
	registry := &Registry{}
	registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test")
	registry.RegisterEnum("enum_map", []string{"a", "b"}, "b")
	entity := &validatedRegistryEntity{}
	registry.RegisterEntity(entity)
	ctx := context.Background()
	validated, err := registry.Validate(ctx)
	assert.NoError(t, err)
	source := validated.GetSourceRegistry()
	assert.NotNil(t, source)
	entities := validated.GetEntities()
	assert.Len(t, entities, 1)
	assert.Equal(t, reflect.TypeOf(validatedRegistryEntity{}), entities["beeorm.validatedRegistryEntity"])
	assert.Nil(t, validated.GetTableSchema("invalid"))

	enum := validated.GetEnum("enum_map")
	assert.Equal(t, []string{"a", "b"}, enum.GetFields())
	assert.Equal(t, "b", enum.GetDefault())
	assert.True(t, enum.Has("a"))
	assert.False(t, enum.Has("c"))

	registry.RegisterEnum("enum_map", []string{"a", "b"})
	validated, err = registry.Validate(ctx)
	assert.NoError(t, err)
	enum = validated.GetEnum("enum_map")
	assert.Equal(t, []string{"a", "b"}, enum.GetFields())
	assert.Equal(t, "a", enum.GetDefault())
	assert.True(t, enum.Has("a"))
	assert.False(t, enum.Has("c"))

	assert.PanicsWithError(t, "entity 'beeorm.validatedRegistryNotRegisteredEntity' is not registered", func() {
		validated.GetTableSchemaForEntity(&validatedRegistryNotRegisteredEntity{})
	})
}
