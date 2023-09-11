package beeorm

//
//import (
//	"testing"
//
//	"github.com/stretchr/testify/assert"
//)
//
//type getByIDEntity struct {
//	ORM             `orm:"localCache;redisCache"`
//	ID              uint64
//	Name            string `orm:"max=100"`
//	ReferenceOne    *loadByIDReference
//	ReferenceSecond *loadByIDReference
//	ReferenceThird  *loadByIDReference2
//}
//
//type getByIDRedisEntity struct {
//	ORM `orm:"redisCache"`
//	ID  uint64
//}
//
//typegetByIDLocalEntity struct {
//	ORM `orm:"localCache"`
//	ID  uint64
//}
//
//type getByIDNoCacheEntity struct {
//	ORM
//	ID   uint64
//	Name string
//}
//
//type getByIDReference struct {
//	ORM            `orm:"localCache;redisCache"`
//	ID             uint64
//	Name           string
//	ReferenceTwo   *loadByIDSubReference
//	ReferenceThree *loadByIDSubReference2
//}
//
//type getByIDReference2 struct {
//	ORM  `orm:"localCache;redisCache"`
//	ID   uint64
//	Name string
//}
//
//type getByIDSubReference struct {
//	ORM  `orm:"localCache;redisCache"`
//	ID   uint64
//	Name string
//}
//
//type getByIDSubReference2 struct {
//	ORM          `orm:"localCache"`
//	ID           uint64
//	Name         string
//	ReferenceTwo *loadByIDSubReference
//}
//
//func TestGetByIdNoCache(t *testing.T) {
//	testLoadByID(t, false, false)
//}
//
//func TestGetByIdLocalCache(t *testing.T) {
//	testLoadByID(t, true, false)
//}
//
//func TestGetByIdRedisCache(t *testing.T) {
//	testLoadByID(t, false, true)
//}
//
//func TestGetByIdLocalRedisCache(t *testing.T) {
//	testLoadByID(t, true, true)
//}
//
//func testLoadByID(t *testing.T, local, redis bool) {
//	var entity *loadByIDEntity
//	var entityRedis *loadByIDRedisEntity
//	var entityLocal *loadByIDLocalEntity
//	var entityNoCache *loadByIDNoCacheEntity
//	var reference *loadByIDReference
//	var reference2 *loadByIDReference2
//	var subReference2 *loadByIDSubReference2
//	var subReference *loadByIDSubReference
//	c := PrepareTables(t, &Registry{}, 5, 6, "", entity, entityRedis, entityLocal, entityNoCache, reference, subReference,
//		subReference2, reference2)
//
//	schemas := make([]EntitySchema, 0)
//	registry := c.Engine().Registry()
//	schemas = append(schemas, registry.EntitySchema(entity))
//	schemas = append(schemas, registry.EntitySchema(entityRedis))
//	schemas = append(schemas, registry.EntitySchema(entityLocal))
//	schemas = append(schemas, registry.EntitySchema(entityNoCache))
//	schemas = append(schemas, registry.EntitySchema(reference))
//	schemas = append(schemas, registry.EntitySchema(reference2))
//	schemas = append(schemas, registry.EntitySchema(subReference2))
//	schemas = append(schemas, registry.EntitySchema(subReference))
//
//	for _, schema := range schemas {
//		schema.DisableCache(!local, !redis)
//	}
//
//	e := &loadByIDEntity{Name: "a", ReferenceOne: &loadByIDReference{Name: "r1", ReferenceTwo: &loadByIDSubReference{Name: "s1"}}}
//	e.ReferenceSecond = &loadByIDReference{Name: "r11", ReferenceTwo: &loadByIDSubReference{Name: "s1"},
//		ReferenceThree: &loadByIDSubReference2{Name: "s11", ReferenceTwo: &loadByIDSubReference{Name: "hello"}}}
//	e.ReferenceThird = &loadByIDReference2{Name: "r2A"}
//	c.Flusher().Track(e,
//		&loadByIDEntity{Name: "b", ReferenceOne: &loadByIDReference{Name: "r2", ReferenceTwo: &loadByIDSubReference{Name: "s2"}}},
//		&loadByIDEntity{Name: "c"}, &loadByIDNoCacheEntity{Name: "a"}, &loadByIDLocalEntity{}).Flush()
//	c.Engine().LocalCache(DefaultPoolCode).Clear(c)
//
//	id := e.GetID()
//	c.EnableQueryDebug()
//	entity = GetByID[*loadByIDEntity](c, id)
//	assert.NotNil(t, entity)
//	assert.True(t, entity.IsLoaded())
//	assert.True(t, entity.ReferenceOne.IsLoaded())
//	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())
//	assert.True(t, entity.ReferenceSecond.IsLoaded())
//	assert.True(t, entity.ReferenceSecond.ReferenceTwo.IsLoaded())
//	assert.True(t, entity.ReferenceSecond.ReferenceThree.IsLoaded())
//	assert.True(t, entity.ReferenceSecond.ReferenceThree.ReferenceTwo.IsLoaded())
//
//	schema := GetEntitySchema[*loadByIDEntity](c)
//	assert.NotNil(t, schema)
//	assert.Equal(t, "loadByIDEntity", schema.GetTableName())
//
//	entityLocalCache := GetByID[*loadByIDLocalEntity](c, 1)
//	assert.NotNil(t, entityLocalCache)
//
//	if local && redis {
//		GetByID[*loadByIDLocalEntity](c, 999)
//		c.Engine().LocalCache(DefaultPoolCode).Clear(c)
//		entityLocalCache = GetByID[*loadByIDLocalEntity](c, 1)
//		assert.NotNil(t, entityLocalCache)
//		entityLocalCache = GetByID[*loadByIDLocalEntity](c, 999)
//		assert.Nil(t, entityLocalCache)
//	}
//
//	c = PrepareTables(t, &Registry{}, 5, 6, "")
//	entity = &loadByIDEntity{}
//	assert.PanicsWithError(t, "entity 'beeorm.loadByIDEntity' is not registered", func() {
//		GetByID[*loadByIDEntity](c, 1)
//	})
//}
