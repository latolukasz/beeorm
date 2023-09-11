package beeorm

//
//import (
//	"testing"
//
//	"github.com/stretchr/testify/assert"
//)
//
//type loadByIdsEntity struct {
//	ORM          `orm:"localCache;redisCache"`
//	ID           uint64
//	Name         string `orm:"max=100"`
//	ReferenceOne *loadByIdsReference
//}
//
//type loadByIdsReference struct {
//	ORM          `orm:"localCache;redisCache"`
//	ID           uint64
//	Name         string
//	ReferenceTwo *loadByIdsSubReference
//}
//
//type loadByIdsSubReference struct {
//	ORM  `orm:"localCache;redisCache"`
//	ID   uint64
//	Name string
//}
//
//func TestLoadByIdsNoCache(t *testing.T) {
//	testLoadByIds(t, false, false)
//}
//
//func TestLoadByIdsLocalCache(t *testing.T) {
//	testLoadByIds(t, true, false)
//}
//
//func TestLoadByIdsRedisCache(t *testing.T) {
//	testLoadByIds(t, false, true)
//}
//
//func TestLoadByIdsLocalRedisCache(t *testing.T) {
//	testLoadByIds(t, true, true)
//}
//
//func testLoadByIds(t *testing.T, local, redis bool) {
//	var entity *loadByIdsEntity
//	var reference *loadByIdsReference
//	var subReference *loadByIdsSubReference
//	c := PrepareTables(t, &Registry{}, 5, 6, "", entity, reference, subReference)
//	schema := GetEntitySchema[*loadByIdsEntity](c)
//	schema2 := GetEntitySchema[*loadByIdsReference](c)
//	schema3 := GetEntitySchema[*loadByIdsSubReference](c)
//	schema.DisableCache(!local, !redis)
//	schema2.DisableCache(!local, !redis)
//	schema3.DisableCache(!local, !redis)
//
//	c.Flusher().
//		Track(&loadByIdsEntity{Name: "a", ReferenceOne: &loadByIdsReference{Name: "r1", ReferenceTwo: &loadByIdsSubReference{Name: "s1"}}}).
//		Track(&loadByIdsEntity{Name: "b", ReferenceOne: &loadByIdsReference{Name: "r2", ReferenceTwo: &loadByIdsSubReference{Name: "s2"}}}).
//		Track(&loadByIdsEntity{Name: "c"}).
//		Flush()
//
//	c.EnableQueryDebug()
//	rows := GetByIDs[*loadByIdsEntity](c, 1, 2, 3, 4)
//	assert.Len(t, rows, 4)
//	assert.Equal(t, "a", rows[0].Name)
//	assert.Equal(t, "b", rows[1].Name)
//	assert.Equal(t, "c", rows[2].Name)
//	assert.Nil(t, rows[3])
//	c.Engine().LocalCache(DefaultPoolCode).Remove(c, "a25e2:3")
//	c.Engine().Redis(DefaultPoolCode).Del(c, "a25e2:3")
//	rows = GetByIDs[*loadByIdsEntity](c, 1, 2, 3, 4)
//	assert.Len(t, rows, 4)
//	assert.Equal(t, "a", rows[0].Name)
//	assert.Equal(t, "r1", rows[0].ReferenceOne.Name)
//	assert.Equal(t, "b", rows[1].Name)
//	assert.Equal(t, "r2", rows[1].ReferenceOne.Name)
//	assert.Equal(t, "c", rows[2].Name)
//	assert.Nil(t, rows[3])
//	rows = GetByIDs[*loadByIdsEntity](c, 1, 2, 3, 4)
//	assert.Len(t, rows, 4)
//	assert.NotNil(t, rows[0])
//	assert.NotNil(t, rows[1])
//	assert.NotNil(t, rows[2])
//	assert.Nil(t, rows[3])
//	rows = GetByIDs[*loadByIdsEntity](c, 1, 2, 3, 4)
//	assert.Len(t, rows, 4)
//	assert.NotNil(t, rows[0])
//	assert.NotNil(t, rows[1])
//	assert.NotNil(t, rows[2])
//	assert.Nil(t, rows[3])
//	rows = GetByIDs[*loadByIdsEntity](c, 1, 4, 4)
//	assert.Len(t, rows, 3)
//	assert.NotNil(t, rows[0])
//	assert.Nil(t, rows[1])
//	assert.Nil(t, rows[2])
//	rows = GetByIDs[*loadByIdsEntity](c)
//	assert.Len(t, rows, 0)
//	c.Engine().Redis(DefaultPoolCode).Del(c, "a25e2:1")
//	rows = GetByIDs[*loadByIdsEntity](c, 2, 4, 4, 1, 1, 4)
//	assert.Len(t, rows, 6)
//	assert.NotNil(t, rows[0])
//	assert.Nil(t, rows[1])
//	assert.Nil(t, rows[2])
//	assert.NotNil(t, rows[3])
//	assert.NotNil(t, rows[4])
//	assert.Nil(t, rows[5])
//	assert.Equal(t, uint64(2), rows[0].GetID())
//	assert.Equal(t, uint64(1), rows[3].GetID())
//	assert.Equal(t, uint64(1), rows[4].GetID())
//
//	c.Engine().Redis(DefaultPoolCode).FlushDB(c)
//	rows = GetByIDs[*loadByIdsEntity](c, 1)
//	rows = make([]*loadByIdsEntity, 0)
//	GetByIDs[*loadByIdsEntity](c, 1, 2, 3)
//	assert.Len(t, rows, 3)
//	assert.Equal(t, uint64(1), rows[0].GetID())
//	assert.Equal(t, uint64(2), rows[1].GetID())
//	assert.Equal(t, uint64(3), rows[2].GetID())
//
//	rows = make([]*loadByIdsEntity, 0)
//	rows = GetByIDs[*loadByIdsEntity](c, 1, 2, 3)
//	assert.Len(t, rows, 3)
//	assert.Equal(t, uint64(1), rows[0].GetID())
//	assert.Equal(t, uint64(2), rows[1].GetID())
//	assert.Equal(t, uint64(3), rows[2].GetID())
//
//	c.Engine().Redis(DefaultPoolCode).FlushDB(c)
//	rows = GetByIDs[*loadByIdsEntity](c, 2)
//	rows = make([]*loadByIdsEntity, 0)
//	rows = GetByIDs[*loadByIdsEntity](c, 1, 2, 3)
//	assert.Len(t, rows, 3)
//	assert.Equal(t, uint64(1), rows[0].GetID())
//	assert.Equal(t, uint64(2), rows[1].GetID())
//	assert.Equal(t, uint64(3), rows[2].GetID())
//
//	c.Engine().Redis(DefaultPoolCode).FlushDB(c)
//	rows = GetByIDs[*loadByIdsEntity](c, 3)
//	rows = make([]*loadByIdsEntity, 0)
//	rows = GetByIDs[*loadByIdsEntity](c, 1, 2, 3)
//	assert.Len(t, rows, 3)
//	assert.Equal(t, uint64(1), rows[0].GetID())
//	assert.Equal(t, uint64(2), rows[1].GetID())
//	assert.Equal(t, uint64(3), rows[2].GetID())
//
//	c.Engine().Redis(DefaultPoolCode).FlushDB(c)
//	rows = GetByIDs[*loadByIdsEntity](c, 1)
//	rows = make([]*loadByIdsEntity, 0)
//	rows = GetByIDs[*loadByIdsEntity](c, 1, 2, 3)
//	assert.Len(t, rows, 3)
//	assert.Equal(t, uint64(1), rows[0].GetID())
//	assert.Equal(t, uint64(2), rows[1].GetID())
//	assert.Equal(t, uint64(3), rows[2].GetID())
//	assert.Equal(t, "a", rows[0].Name)
//	assert.Equal(t, "b", rows[1].Name)
//	assert.Equal(t, "c", rows[2].Name)
//
//	rows = make([]*loadByIdsEntity, 0)
//	rows = GetByIDs[*loadByIdsEntity](c, 1, 2, 3)
//	assert.Len(t, rows, 3)
//	assert.Equal(t, uint64(1), rows[0].GetID())
//	assert.Equal(t, uint64(2), rows[1].GetID())
//	assert.Equal(t, uint64(3), rows[2].GetID())
//
//	c.Engine().Redis(DefaultPoolCode).FlushDB(c)
//	rows = GetByIDs[*loadByIdsEntity](c, 2)
//	rows = make([]*loadByIdsEntity, 0)
//	rows = GetByIDs[*loadByIdsEntity](c, 1, 2, 3)
//	assert.Len(t, rows, 3)
//	assert.Equal(t, uint64(1), rows[0].GetID())
//	assert.Equal(t, uint64(2), rows[1].GetID())
//	assert.Equal(t, uint64(3), rows[2].GetID())
//
//	c.Engine().Redis(DefaultPoolCode).FlushDB(c)
//	rows = GetByIDs[*loadByIdsEntity](c, 3)
//	rows = make([]*loadByIdsEntity, 0)
//	rows = GetByIDs[*loadByIdsEntity](c, 1, 2, 3)
//	assert.Len(t, rows, 3)
//	assert.Equal(t, uint64(1), rows[0].GetID())
//	assert.Equal(t, uint64(2), rows[1].GetID())
//	assert.Equal(t, uint64(3), rows[2].GetID())
//
//	rows = make([]*loadByIdsEntity, 0)
//	GetByIDs[*loadByIdsEntity](c, 1, 1, 1)
//	assert.Len(t, rows, 3)
//	assert.NotNil(t, rows[0])
//	assert.Equal(t, "a", rows[0].Name)
//	assert.NotNil(t, rows[1])
//	assert.Equal(t, "a", rows[1].Name)
//	assert.NotNil(t, rows[2])
//	assert.Equal(t, "a", rows[2].Name)
//
//	if local && redis {
//		c.Engine().LocalCache(DefaultPoolCode).Clear(c)
//		rows = make([]*loadByIdsEntity, 0)
//		GetByIDs[*loadByIdsEntity](c, 1, 2, 3)
//		c.Engine().LocalCache(DefaultPoolCode).Clear(c)
//		rows = make([]*loadByIdsEntity, 0)
//		GetByIDs[*loadByIdsEntity](c, 1, 2, 3)
//		assert.Len(t, rows, 3)
//		assert.Equal(t, uint64(1), rows[0].GetID())
//		assert.Equal(t, uint64(2), rows[1].GetID())
//		assert.Equal(t, uint64(3), rows[2].GetID())
//	}
//
//	c = PrepareTables(t, &Registry{}, 5, 6, "")
//	assert.PanicsWithError(t, "entity 'beeorm.loadByIdsEntity' is not registered", func() {
//		_ = GetByIDs[*loadByIdsEntity](c, 1)
//	})
//}
