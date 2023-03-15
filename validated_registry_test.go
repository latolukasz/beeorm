package beeorm

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

type validatedRegistryEntity struct {
	ORM
	ID      uint64
	TestSub validatedRegistryStruct
	Ref     *validatedRegistryEntity
}

type validatedRegistryStruct struct {
	Sub *validatedRegistryEntity
}

type validatedRegistryNotRegisteredEntity struct {
	ORM
	ID uint64
}

func TestValidatedRegistry(t *testing.T) {
	registry := &Registry{}
	registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test", MySQLPoolOptions{})
	registry.RegisterLocalCache(100)
	registry.RegisterLocalCache(50, "another")
	registry.RegisterEnum("enum_map", []string{"a", "b"}, "b")
	entity := &validatedRegistryEntity{}
	registry.RegisterEntity(entity)
	validated, err := registry.Validate()
	assert.NoError(t, err)
	source := validated.GetSourceRegistry()
	assert.NotNil(t, source)
	entities := validated.GetEntities()
	assert.Len(t, entities, 1)
	assert.Equal(t, reflect.TypeOf(validatedRegistryEntity{}), entities["beeorm.validatedRegistryEntity"])
	assert.Nil(t, validated.GetEntitySchema("invalid"))

	enum := validated.GetEnum("enum_map")
	assert.Equal(t, []string{"a", "b"}, enum.GetFields())
	assert.Equal(t, "b", enum.GetDefault())
	assert.True(t, enum.Has("a"))
	assert.False(t, enum.Has("c"))

	registry.RegisterEnum("enum_map", []string{"a", "b"})
	validated, err = registry.Validate()
	assert.NoError(t, err)
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
	assert.Equal(t, "root:root@tcp(localhost:3311)/test", mysqlPools["default"].GetDataSourceURI())

	localCachePools := validated.GetLocalCachePools()
	assert.Len(t, localCachePools, 2)
	assert.NotNil(t, localCachePools["default"])
	assert.NotNil(t, localCachePools["another"])
	assert.Equal(t, "default", localCachePools["default"].GetCode())
	assert.Equal(t, 100, localCachePools["default"].GetLimit())
	assert.Equal(t, "another", localCachePools["another"].GetCode())
	assert.Equal(t, 50, localCachePools["another"].GetLimit())

	assert.PanicsWithError(t, "entity 'beeorm.validatedRegistryNotRegisteredEntity' is not registered", func() {
		validated.GetEntitySchemaForEntity(&validatedRegistryNotRegisteredEntity{})
	})

	usage := validated.GetEntitySchemaForEntity(entity).GetUsage(validated)
	assert.NotNil(t, usage)
	assert.Len(t, usage, 1)
	for _, data := range usage {
		assert.Len(t, data, 2)
		assert.Equal(t, "TestSubSub", data[1])
		assert.Equal(t, "Ref", data[0])
	}
}
