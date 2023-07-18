package beeorm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type loadByIDEntity struct {
	ORM             `orm:"localCache;redisCache"`
	ID              uint64
	Name            string `orm:"max=100"`
	ReferenceOne    *loadByIDReference
	ReferenceSecond *loadByIDReference
	ReferenceThird  *loadByIDReference2
}

type loadByIDRedisEntity struct {
	ORM `orm:"redisCache"`
	ID  uint64
}

type loadByIDLocalEntity struct {
	ORM `orm:"localCache"`
	ID  uint64
}

type loadByIDNoCacheEntity struct {
	ORM
	ID   uint64
	Name string
}

type loadByIDReference struct {
	ORM            `orm:"localCache;redisCache"`
	ID             uint64
	Name           string
	ReferenceTwo   *loadByIDSubReference
	ReferenceThree *loadByIDSubReference2
}

type loadByIDReference2 struct {
	ORM  `orm:"localCache;redisCache"`
	ID   uint64
	Name string
}

type loadByIDSubReference struct {
	ORM  `orm:"localCache;redisCache"`
	ID   uint64
	Name string
}

type loadByIDSubReference2 struct {
	ORM          `orm:"localCache"`
	ID           uint64
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
	engine := PrepareTables(t, &Registry{}, 5, 6, "", entity, entityRedis, entityLocal, entityNoCache, reference, subReference,
		subReference2, reference2)

	schemas := make([]EntitySchema, 0)
	registry := engine.Registry()
	schemas = append(schemas, registry.GetEntitySchemaForEntity(entity))

	schemas = append(schemas, registry.GetEntitySchemaForEntity(entityRedis))
	schemas = append(schemas, registry.GetEntitySchemaForEntity(entityLocal))
	schemas = append(schemas, registry.GetEntitySchemaForEntity(entityNoCache))
	schemas = append(schemas, registry.GetEntitySchemaForEntity(reference))
	schemas = append(schemas, registry.GetEntitySchemaForEntity(reference2))
	schemas = append(schemas, registry.GetEntitySchemaForEntity(subReference2))
	schemas = append(schemas, registry.GetEntitySchemaForEntity(subReference))

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
	engine.EnableQueryDebug()
	found := engine.LoadByID(id, entity, "ReferenceOne/ReferenceTwo",
		"ReferenceSecond/ReferenceTwo", "ReferenceSecond/ReferenceThree/ReferenceTwo")
	return
	assert.True(t, found)
	assert.True(t, entity.IsLoaded())
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())
	assert.True(t, entity.ReferenceSecond.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceTwo.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceThree.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceThree.ReferenceTwo.IsLoaded())

	schema := engine.Registry().GetEntitySchemaForCachePrefix("6e009")
	assert.NotNil(t, schema)
	assert.Equal(t, "loadByIDEntity", schema.GetTableName())
	schema = engine.Registry().GetEntitySchemaForCachePrefix("invalid")
	assert.Nil(t, schema)

	entity = &loadByIDEntity{}
	engine.EnableQueryDebug()
	fmt.Printf("ID %d %v\n", id, engine.Registry().GetEntitySchemaForEntity(reference).(*entitySchema).cachePrefix)
	fmt.Printf("ID %d %v\n", id, engine.Registry().GetEntitySchemaForEntity(reference2).(*entitySchema).cachePrefix)
	found = engine.LoadByID(id, entity, "ReferenceThird", "ReferenceOne")
	return
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

	entity = &loadByIDEntity{ID: id}
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

	engine = PrepareTables(t, &Registry{}, 5, 6, "")
	entity = &loadByIDEntity{}
	assert.PanicsWithError(t, "entity 'beeorm.loadByIDEntity' is not registered", func() {
		engine.LoadByID(1, entity)
	})
}
