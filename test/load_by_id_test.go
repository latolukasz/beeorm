package test

import (
	"testing"

	"github.com/latolukasz/beeorm"

	"github.com/stretchr/testify/assert"
)

type loadByIDEntity struct {
	beeorm.ORM      `orm:"localCache;redisCache"`
	Name            string `orm:"max=100"`
	ReferenceOne    *loadByIDReference
	ReferenceSecond *loadByIDReference
	ReferenceThird  *loadByIDReference2
}

type loadByIDRedisEntity struct {
	beeorm.ORM `orm:"redisCache"`
}

type loadByIDLocalEntity struct {
	beeorm.ORM `orm:"localCache"`
}

type loadByIDNoCacheEntity struct {
	beeorm.ORM
	Name string
}

type loadByIDReference struct {
	beeorm.ORM     `orm:"localCache;redisCache"`
	Name           string
	ReferenceTwo   *loadByIDSubReference
	ReferenceThree *loadByIDSubReference2
}

type loadByIDReference2 struct {
	beeorm.ORM `orm:"localCache;redisCache"`
	Name       string
}

type loadByIDSubReference struct {
	beeorm.ORM `orm:"localCache;redisCache"`
	Name       string
}

type loadByIDSubReference2 struct {
	beeorm.ORM   `orm:"localCache"`
	Name         string
	ReferenceTwo *loadByIDSubReference
}

func TestLoadByIdNoCache(t *testing.T) {
	testLoadByID(t, false, false)
}

func TestLoadByIdLocalCache(t *testing.T) {
	testLoadByID(t, true, false)
}

func TestLoadByIdRedisCache(t *testing.T) {
	testLoadByID(t, false, true)
}

func TestLoadByIdLocalRedisCache(t *testing.T) {
	testLoadByID(t, true, true)
}

func testLoadByID(t *testing.T, local, redis bool) {
	var entity *loadByIDEntity
	var entityRedis *loadByIDRedisEntity
	var entityLocal *loadByIDLocalEntity
	var entityNoCache *loadByIDNoCacheEntity
	var reference *loadByIDReference
	var reference2 *loadByIDReference2
	var subReference2 *loadByIDSubReference2
	var subReference *loadByIDSubReference
	engine := PrepareTables(t, &beeorm.Registry{}, 5, 6, "", entity, entityRedis, entityLocal, entityNoCache, reference, subReference,
		subReference2, reference2)

	schemas := make([]beeorm.TableSchema, 0)
	registry := engine.GetRegistry()
	schemas = append(schemas, registry.GetTableSchemaForEntity(entity))

	schemas = append(schemas, registry.GetTableSchemaForEntity(entityRedis))
	schemas = append(schemas, registry.GetTableSchemaForEntity(entityLocal))
	schemas = append(schemas, registry.GetTableSchemaForEntity(entityNoCache))
	schemas = append(schemas, registry.GetTableSchemaForEntity(reference))
	schemas = append(schemas, registry.GetTableSchemaForEntity(reference2))
	schemas = append(schemas, registry.GetTableSchemaForEntity(subReference2))
	schemas = append(schemas, registry.GetTableSchemaForEntity(subReference))

	for _, schema := range schemas {
		schema.DisableCache(!local, !redis)
	}

	e := &loadByIDEntity{Name: "a", ReferenceOne: &loadByIDReference{Name: "r1", ReferenceTwo: &loadByIDSubReference{Name: "s1"}}}
	e.ReferenceSecond = &loadByIDReference{Name: "r11", ReferenceTwo: &loadByIDSubReference{Name: "s1"},
		ReferenceThree: &loadByIDSubReference2{Name: "s11", ReferenceTwo: &loadByIDSubReference{Name: "hello"}}}
	e.ReferenceThird = &loadByIDReference2{Name: "r2A"}
	engine.Flush(e,
		&loadByIDEntity{Name: "b", ReferenceOne: &loadByIDReference{Name: "r2", ReferenceTwo: &loadByIDSubReference{Name: "s2"}}},
		&loadByIDEntity{Name: "c"}, &loadByIDNoCacheEntity{Name: "a"}, &loadByIDLocalEntity{})
	engine.GetLocalCache().Clear()

	entity = &loadByIDEntity{}
	id := e.GetID()
	found := engine.LoadByID(id, entity, "ReferenceOne/ReferenceTwo",
		"ReferenceSecond/ReferenceTwo", "ReferenceSecond/ReferenceThree/ReferenceTwo")
	assert.True(t, found)
	assert.True(t, entity.IsLoaded())
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())
	assert.True(t, entity.ReferenceSecond.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceTwo.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceThree.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceThree.ReferenceTwo.IsLoaded())

	schema := engine.GetRegistry().GetTableSchemaForCachePrefix("528af")
	assert.NotNil(t, schema)
	assert.Equal(t, "loadByIDEntity", schema.GetTableName())
	schema = engine.GetRegistry().GetTableSchemaForCachePrefix("invalid")
	assert.Nil(t, schema)

	entity = &loadByIDEntity{}
	found = engine.LoadByID(id, entity, "ReferenceThird", "ReferenceOne")
	assert.True(t, found)
	assert.Equal(t, "a", entity.Name)
	assert.Equal(t, "r2A", entity.ReferenceThird.Name)
	assert.Equal(t, "r1", entity.ReferenceOne.Name)

	entity = &loadByIDEntity{}
	found = engine.LoadByID(id, entity, "ReferenceOne/ReferenceTwo")
	assert.True(t, found)
	assert.Equal(t, id, entity.GetID())
	assert.Equal(t, "a", entity.Name)
	assert.Equal(t, "r1", entity.ReferenceOne.Name)
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.Equal(t, "s1", entity.ReferenceOne.ReferenceTwo.Name)
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())

	entity = &loadByIDEntity{}
	entity.SetID(id)
	engine.Load(entity, "ReferenceOne/ReferenceTwo")
	assert.Equal(t, "a", entity.Name)
	assert.Equal(t, "r1", entity.ReferenceOne.Name)
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.Equal(t, "s1", entity.ReferenceOne.ReferenceTwo.Name)
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())
	engine.Load(entity, "ReferenceOne/ReferenceTwo")
	assert.Equal(t, "a", entity.Name)
	assert.Equal(t, "r1", entity.ReferenceOne.Name)
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.Equal(t, "s1", entity.ReferenceOne.ReferenceTwo.Name)
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())

	entityNoCache = &loadByIDNoCacheEntity{}
	found = engine.LoadByID(1, entityNoCache, "*")
	assert.True(t, found)
	assert.Equal(t, uint64(1), entityNoCache.GetID())
	assert.Equal(t, "a", entityNoCache.Name)

	found = engine.LoadByID(100, entity, "*")
	assert.False(t, found)
	found = engine.LoadByID(100, entity, "*")
	assert.False(t, found)
	entityRedis = &loadByIDRedisEntity{}
	found = engine.LoadByID(100, entityRedis, "*")
	assert.False(t, found)
	found = engine.LoadByID(100, entityRedis, "*")
	assert.False(t, found)

	entityLocalCache := &loadByIDLocalEntity{}
	found = engine.LoadByID(1, entityLocalCache)
	assert.True(t, found)

	if local && redis {
		engine.LoadByID(999, entityLocalCache)
		engine.GetLocalCache().Clear()
		assert.True(t, engine.LoadByID(1, entityLocalCache))
		assert.False(t, engine.LoadByID(999, entityLocalCache))
	}

	engine = PrepareTables(t, &beeorm.Registry{}, 5, 6, "")
	entity = &loadByIDEntity{}
	assert.PanicsWithError(t, "entity 'test.loadByIDEntity' is not registered", func() {
		engine.LoadByID(1, entity)
	})
}
