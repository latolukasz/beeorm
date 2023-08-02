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
	source := validated.Registry()
	assert.NotNil(t, source)
	entities := validated.Registry().Entities()
	assert.Len(t, entities, 1)
	assert.Equal(t, reflect.TypeOf(validatedRegistryEntity{}), entities["beeorm.validatedRegistryEntity"])
	assert.Nil(t, validated.Registry().EntitySchema("invalid"))

	enum := validated.Registry().Enum("enum_map")
	assert.Equal(t, []string{"a", "b"}, enum.GetFields())
	assert.Equal(t, "b", enum.GetDefault())
	assert.True(t, enum.Has("a"))
	assert.False(t, enum.Has("c"))

	registry.RegisterEnum("enum_map", []string{"a", "b"})
	validated, err = registry.Validate()
	assert.NoError(t, err)
	enum = validated.Registry().Enum("enum_map")
	assert.Equal(t, []string{"a", "b"}, enum.GetFields())
	assert.Equal(t, "a", enum.GetDefault())
	assert.True(t, enum.Has("a"))
	assert.False(t, enum.Has("c"))

	mysqlPools := validated.Registry().DBPools()
	assert.Len(t, mysqlPools, 1)
	assert.NotNil(t, mysqlPools["default"])
	assert.Equal(t, "default", mysqlPools["default"].GetPoolConfig().GetCode())
	assert.Equal(t, "test", mysqlPools["default"].GetPoolConfig().GetDatabase())
	assert.Equal(t, 5, mysqlPools["default"].GetPoolConfig().GetVersion())
	assert.Equal(t, "root:root@tcp(localhost:3311)/test", mysqlPools["default"].GetPoolConfig().GetDataSourceURI())

	localCachePools := validated.Registry().LocalCachePools()
	assert.Len(t, localCachePools, 2)
	assert.NotNil(t, localCachePools["default"])
	assert.NotNil(t, localCachePools["another"])
	assert.Equal(t, "default", localCachePools["default"].GetPoolConfig().GetCode())
	assert.Equal(t, 100, localCachePools["default"].GetPoolConfig().GetLimit())
	assert.Equal(t, "another", localCachePools["another"].GetPoolConfig().GetCode())
	assert.Equal(t, 50, localCachePools["another"].GetPoolConfig().GetLimit())

	assert.PanicsWithError(t, "entity 'beeorm.validatedRegistryNotRegisteredEntity' is not registered", func() {
		validated.Registry().EntitySchema(&validatedRegistryNotRegisteredEntity{})
	})

	usage := validated.Registry().EntitySchema(entity).GetUsage(validated)
	assert.NotNil(t, usage)
	assert.Len(t, usage, 1)
	for _, data := range usage {
		assert.Len(t, data, 2)
		assert.Equal(t, "TestSubSub", data[1])
		assert.Equal(t, "Ref", data[0])
	}
}
