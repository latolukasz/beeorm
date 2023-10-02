package beeorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type getByIDEntity struct {
	ID              uint64 `orm:"localCache;redisCache"`
	Name            string `orm:"max=100"`
	ReferenceOne    *Reference[*getByIDReference]
	ReferenceSecond *Reference[*getByIDReference]
	ReferenceThird  *Reference[*getByIDReference2]
}

func (e *getByIDEntity) GetID() uint64 {
	return e.ID
}

type getByIDRedisEntity struct {
	ID uint64 `orm:"redisCache"`
}

func (e *getByIDRedisEntity) GetID() uint64 {
	return e.ID
}

type getByIDLocalEntity struct {
	ID uint64 `orm:"localCache"`
}

func (e *getByIDLocalEntity) GetID() uint64 {
	return e.ID
}

type getByIDNoCacheEntity struct {
	ID   uint64
	Name string
}

func (e *getByIDNoCacheEntity) GetID() uint64 {
	return e.ID
}

type getByIDReference struct {
	ID             uint64 `orm:"localCache;redisCache"`
	Name           string
	ReferenceTwo   *getByIDSubReference
	ReferenceThree *getByIDSubReference2
}

func (e *getByIDReference) GetID() uint64 {
	return e.ID
}

type getByIDReference2 struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string
}

func (e *getByIDReference2) GetID() uint64 {
	return e.ID
}

type getByIDSubReference struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string
}

func (e *getByIDSubReference) GetID() uint64 {
	return e.ID
}

type getByIDSubReference2 struct {
	ID           uint64 `orm:"localCache"`
	Name         string
	ReferenceTwo *getByIDSubReference
}

func (e *getByIDSubReference2) GetID() uint64 {
	return e.ID
}

func TestGetByIdNoCache(t *testing.T) {
	testLoadByID(t, false, false)
}

func TestGetByIdLocalCache(t *testing.T) {
	testLoadByID(t, true, false)
}

func TestGetByIdRedisCache(t *testing.T) {
	testLoadByID(t, false, true)
}

func TestGetByIdLocalRedisCache(t *testing.T) {
	testLoadByID(t, true, true)
}

func testLoadByID(t *testing.T, local, redis bool) {
	var entity *getByIDEntity
	var entityRedis *getByIDRedisEntity
	var entityLocal *getByIDLocalEntity
	var entityNoCache *getByIDNoCacheEntity
	var reference *getByIDReference
	var reference2 *getByIDReference2
	var subReference2 *getByIDSubReference2
	var subReference *getByIDSubReference
	c := PrepareTables(t, &Registry{}, entity, entityRedis, entityLocal, entityNoCache, reference, subReference,
		subReference2, reference2)

	schemas := make([]EntitySchema, 0)
	registry := c.Engine().Registry()
	schemas = append(schemas, registry.EntitySchema(entity))
	schemas = append(schemas, registry.EntitySchema(entityRedis))
	schemas = append(schemas, registry.EntitySchema(entityLocal))
	schemas = append(schemas, registry.EntitySchema(entityNoCache))
	schemas = append(schemas, registry.EntitySchema(reference))
	schemas = append(schemas, registry.EntitySchema(reference2))
	schemas = append(schemas, registry.EntitySchema(subReference2))
	schemas = append(schemas, registry.EntitySchema(subReference))

	for _, schema := range schemas {
		schema.DisableCache(!local, !redis)
	}

	e := &getByIDEntity{Name: "a", ReferenceOne: &getByIDReference{Name: "r1", ReferenceTwo: &getByIDSubReference{Name: "s1"}}}
	e.ReferenceSecond = &getByIDReference{Name: "r11", ReferenceTwo: &getByIDSubReference{Name: "s1"},
		ReferenceThree: &getByIDSubReference2{Name: "s11", ReferenceTwo: &getByIDSubReference{Name: "hello"}}}
	e.ReferenceThird = &getByIDReference2{Name: "r2A"}
	c.Flusher().Track(e,
		&getByIDEntity{Name: "b", ReferenceOne: &getByIDReference{Name: "r2", ReferenceTwo: &getByIDSubReference{Name: "s2"}}},
		&getByIDEntity{Name: "c"}, &getByIDNoCacheEntity{Name: "a"}, &getByIDLocalEntity{}).Flush()
	c.Engine().LocalCache(DefaultPoolCode).Clear(c)

	id := e.GetID()
	c.EnableQueryDebug()
	entity = GetByID[*getByIDEntity](c, id)
	assert.NotNil(t, entity)
	assert.True(t, entity.IsLoaded())
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())
	assert.True(t, entity.ReferenceSecond.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceTwo.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceThree.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceThree.ReferenceTwo.IsLoaded())

	schema := GetEntitySchema[*getByIDEntity](c)
	assert.NotNil(t, schema)
	assert.Equal(t, "getByIDEntity", schema.GetTableName())

	entityLocalCache := GetByID[*getByIDLocalEntity](c, 1)
	assert.NotNil(t, entityLocalCache)

	if local && redis {
		GetByID[*getByIDLocalEntity](c, 999)
		c.Engine().LocalCache(DefaultPoolCode).Clear(c)
		entityLocalCache = GetByID[*getByIDLocalEntity](c, 1)
		assert.NotNil(t, entityLocalCache)
		entityLocalCache = GetByID[*getByIDLocalEntity](c, 999)
		assert.Nil(t, entityLocalCache)
	}

	c = PrepareTables(t, &Registry{})
	entity = &getByIDEntity{}
	assert.PanicsWithError(t, "entity 'beeorm.getByIDEntity' is not registered", func() {
		GetByID[*getByIDEntity](c, 1)
	})
}
