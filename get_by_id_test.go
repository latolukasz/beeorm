package beeorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type getByIDEntity struct {
	ID           uint64 `orm:"localCache;redisCache"`
	Name         string `orm:"max=100"`
	ReferenceOne *Reference[*getByIDReference]
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
	ID   uint64 `orm:"localCache;redisCache"`
	Name string
}

func (e *getByIDReference) GetID() uint64 {
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
	c := PrepareTables(t, &Registry{}, entity, entityRedis, entityLocal, entityNoCache, reference)

	schemas := make([]EntitySchema, 0)
	registry := c.Engine().Registry()
	schemas = append(schemas, registry.EntitySchema(entity))
	schemas = append(schemas, registry.EntitySchema(entityRedis))
	schemas = append(schemas, registry.EntitySchema(entityLocal))
	schemas = append(schemas, registry.EntitySchema(entityNoCache))
	schemas = append(schemas, registry.EntitySchema(reference))

	for _, schema := range schemas {
		schema.DisableCache(!local, !redis)
	}

	e := NewEntity[*getByIDEntity](c).TrackedEntity()
	e.Name = "a"
	referenceOne := NewEntity[*getByIDReference](c).TrackedEntity()
	referenceOne.Name = "r1"
	e.ReferenceOne = NewReference[*getByIDReference](referenceOne.ID)
	err := c.Flush(true)
	assert.Nil(t, err)
	c.Engine().LocalCache(DefaultPoolCode).Clear(c)

	id := e.GetID()
	entity = GetByID[*getByIDEntity](c, id)
	assert.NotNil(t, entity)

	schema := GetEntitySchema[*getByIDEntity](c)
	assert.NotNil(t, schema)
	assert.Equal(t, "getByIDEntity", schema.GetTableName())

	entityLocalCache := GetByID[*getByIDLocalEntity](c, id)
	assert.NotNil(t, entityLocalCache)

	if local && redis {
		GetByID[*getByIDLocalEntity](c, 999)
		c.Engine().LocalCache(DefaultPoolCode).Clear(c)
		entityLocalCache = GetByID[*getByIDLocalEntity](c, id)
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
