package beeorm

import (
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
	c := PrepareTables(t, &Registry{}, 5, 6, "", entity, entityRedis, entityLocal, entityNoCache, reference, subReference,
		subReference2, reference2)

	schemas := make([]EntitySchema, 0)
	registry := c.Engine()
	schemas = append(schemas, registry.GetEntitySchema(entity))
	schemas = append(schemas, registry.GetEntitySchema(entityRedis))
	schemas = append(schemas, registry.GetEntitySchema(entityLocal))
	schemas = append(schemas, registry.GetEntitySchema(entityNoCache))
	schemas = append(schemas, registry.GetEntitySchema(reference))
	schemas = append(schemas, registry.GetEntitySchema(reference2))
	schemas = append(schemas, registry.GetEntitySchema(subReference2))
	schemas = append(schemas, registry.GetEntitySchema(subReference))

	for _, schema := range schemas {
		schema.DisableCache(!local, !redis)
	}

	e := &loadByIDEntity{Name: "a", ReferenceOne: &loadByIDReference{Name: "r1", ReferenceTwo: &loadByIDSubReference{Name: "s1"}}}
	e.ReferenceSecond = &loadByIDReference{Name: "r11", ReferenceTwo: &loadByIDSubReference{Name: "s1"},
		ReferenceThree: &loadByIDSubReference2{Name: "s11", ReferenceTwo: &loadByIDSubReference{Name: "hello"}}}
	e.ReferenceThird = &loadByIDReference2{Name: "r2A"}
	c.Flusher().Track(e,
		&loadByIDEntity{Name: "b", ReferenceOne: &loadByIDReference{Name: "r2", ReferenceTwo: &loadByIDSubReference{Name: "s2"}}},
		&loadByIDEntity{Name: "c"}, &loadByIDNoCacheEntity{Name: "a"}, &loadByIDLocalEntity{}).Flush()
	c.Engine().GetLocalCache().Clear(c)

	id := e.GetID()
	c.EnableQueryDebug()
	entity = GetByID[*loadByIDEntity](c, id, "ReferenceOne/ReferenceTwo",
		"ReferenceSecond/ReferenceTwo", "ReferenceSecond/ReferenceThree/ReferenceTwo")
	assert.NotNil(t, entity)
	assert.True(t, entity.IsLoaded())
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())
	assert.True(t, entity.ReferenceSecond.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceTwo.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceThree.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceThree.ReferenceTwo.IsLoaded())

	schema := GetEntitySchema[*loadByIDEntity](c)
	assert.NotNil(t, schema)
	assert.Equal(t, "loadByIDEntity", schema.GetTableName())

	c.EnableQueryDebug()
	entity = GetByID[*loadByIDEntity](c, id, "ReferenceThird", "ReferenceOne")
	assert.NotNil(t, entity)
	assert.Equal(t, "a", entity.Name)
	assert.Equal(t, "r2A", entity.ReferenceThird.Name)
	assert.Equal(t, "r1", entity.ReferenceOne.Name)

	entity = &loadByIDEntity{}
	entity = GetByID[*loadByIDEntity](c, id, "ReferenceOne/ReferenceTwo")
	assert.NotNil(t, entity)
	assert.Equal(t, id, entity.GetID())
	assert.Equal(t, "a", entity.Name)
	assert.Equal(t, "r1", entity.ReferenceOne.Name)
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.Equal(t, "s1", entity.ReferenceOne.ReferenceTwo.Name)
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())

	entity = &loadByIDEntity{ID: id}
	Load(c, entity, "ReferenceOne/ReferenceTwo")
	assert.Equal(t, "a", entity.Name)
	assert.Equal(t, "r1", entity.ReferenceOne.Name)
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.Equal(t, "s1", entity.ReferenceOne.ReferenceTwo.Name)
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())
	Load(c, entity, "ReferenceOne/ReferenceTwo")
	assert.Equal(t, "a", entity.Name)
	assert.Equal(t, "r1", entity.ReferenceOne.Name)
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.Equal(t, "s1", entity.ReferenceOne.ReferenceTwo.Name)
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())

	entityNoCache = GetByID[*loadByIDNoCacheEntity](c, 1, "*")
	assert.NotNil(t, entityNoCache)
	assert.Equal(t, uint64(1), entityNoCache.GetID())
	assert.Equal(t, "a", entityNoCache.Name)

	entity = GetByID[*loadByIDEntity](c, 100, "*")
	assert.Nil(t, entity)
	entityRedis = GetByID[*loadByIDRedisEntity](c, 100, "*")
	assert.Nil(t, entityRedis)

	entityLocalCache := GetByID[*loadByIDLocalEntity](c, 1)
	assert.NotNil(t, entityLocalCache)

	if local && redis {
		GetByID[*loadByIDLocalEntity](c, 999)
		c.Engine().GetLocalCache().Clear(c)
		entityLocalCache = GetByID[*loadByIDLocalEntity](c, 1)
		assert.NotNil(t, entityLocalCache)
		entityLocalCache = GetByID[*loadByIDLocalEntity](c, 999)
		assert.Nil(t, entityLocalCache)
	}

	c = PrepareTables(t, &Registry{}, 5, 6, "")
	entity = &loadByIDEntity{}
	assert.PanicsWithError(t, "entity 'beeorm.loadByIDEntity' is not registered", func() {
		GetByID[*loadByIDEntity](c, 1)
	})
}
