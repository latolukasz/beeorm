package beeorm

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

type validatedRegistryEntity struct {
	ORM
	ID      uint
	TestSub validatedRegistryStruct
	Ref     *validatedRegistryEntity
}

type validatedRegistryStruct struct {
	Sub *validatedRegistryEntity
}

type validatedRegistryNotRegisteredEntity struct {
	ORM
	ID uint
}

func TestValidatedRegistry(t *testing.T) {
	registry := &Registry{}
	registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test")
	registry.RegisterLocalCache(100)
	registry.RegisterLocalCache(50, "another")
	registry.RegisterEnum("enum_map", []string{"a", "b"}, "b")
	entity := &validatedRegistryEntity{}
	registry.RegisterEntity(entity)
	validated, def, err := registry.Validate()
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
	def()
	validated, def, err = registry.Validate()
	assert.NoError(t, err)
	defer def()
	enum = validated.GetEnum("enum_map")
	assert.Equal(t, []string{"a", "b"}, enum.GetFields())
	assert.Equal(t, "a", enum.GetDefault())
	assert.True(t, enum.Has("a"))
	assert.False(t, enum.Has("c"))

	mysqlPools := validated.GetMySQLPools()
	assert.Len(t, mysqlPools, 1)
	assert.NotNil(t, mysqlPools["default"])
	assert.Equal(t, "default", mysqlPools["default"].GetCode())
	assert.Equal(t, "test", mysqlPools["default"].GetDatabase())
	assert.Equal(t, 5, mysqlPools["default"].GetVersion())
	assert.Equal(t, "root:root@tcp(localhost:3311)/test?multiStatements=true", mysqlPools["default"].GetDataSourceURI())

	localCachePools := validated.GetLocalCachePools()
	assert.Len(t, localCachePools, 2)
	assert.NotNil(t, localCachePools["default"])
	assert.NotNil(t, localCachePools["another"])
	assert.Equal(t, "default", localCachePools["default"].GetCode())
	assert.Equal(t, 100, localCachePools["default"].GetLimit())
	assert.Equal(t, "another", localCachePools["another"].GetCode())
	assert.Equal(t, 50, localCachePools["another"].GetLimit())

	assert.PanicsWithError(t, "entity 'beeorm.validatedRegistryNotRegisteredEntity' is not registered", func() {
		validated.GetTableSchemaForEntity(&validatedRegistryNotRegisteredEntity{})
	})

	usage := validated.GetTableSchemaForEntity(entity).GetUsage(validated)
	assert.NotNil(t, usage)
	assert.Len(t, usage, 1)
	for _, data := range usage {
		assert.Len(t, data, 2)
		assert.Equal(t, "TestSub/Sub", data[0])
		assert.Equal(t, "Ref", data[1])
	}
}
