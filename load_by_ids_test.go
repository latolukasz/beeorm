package beeorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type loadByIdsEntity struct {
	ORM          `orm:"localCache;redisCache"`
	ID           uint64
	Name         string `orm:"max=100"`
	ReferenceOne *loadByIdsReference
}

type loadByIdsReference struct {
	ORM          `orm:"localCache;redisCache"`
	ID           uint64
	Name         string
	ReferenceTwo *loadByIdsSubReference
}

type loadByIdsSubReference struct {
	ORM  `orm:"localCache;redisCache"`
	ID   uint64
	Name string
}

func TestLoadByIdsNoCache(t *testing.T) {
	testLoadByIds(t, false, false)
}

func TestLoadByIdsLocalCache(t *testing.T) {
	testLoadByIds(t, true, false)
}

func TestLoadByIdsRedisCache(t *testing.T) {
	testLoadByIds(t, false, true)
}

func TestLoadByIdsLocalRedisCache(t *testing.T) {
	testLoadByIds(t, true, true)
}

func testLoadByIds(t *testing.T, local, redis bool) {
	var entity *loadByIdsEntity
	var reference *loadByIdsReference
	var subReference *loadByIdsSubReference
	engine := PrepareTables(t, &Registry{}, 5, 6, "", entity, reference, subReference)
	schema := engine.GetRegistry().GetEntitySchemaForEntity(entity)
	schema2 := engine.GetRegistry().GetEntitySchemaForEntity(reference)
	schema3 := engine.GetRegistry().GetEntitySchemaForEntity(subReference)
	schema.DisableCache(!local, !redis)
	schema2.DisableCache(!local, !redis)
	schema3.DisableCache(!local, !redis)

	engine.Flush(&loadByIdsEntity{Name: "a", ReferenceOne: &loadByIdsReference{Name: "r1", ReferenceTwo: &loadByIdsSubReference{Name: "s1"}}})
	engine.Flush(&loadByIdsEntity{Name: "b", ReferenceOne: &loadByIdsReference{Name: "r2", ReferenceTwo: &loadByIdsSubReference{Name: "s2"}}})
	engine.Flush(&loadByIdsEntity{Name: "c"})

	var rows []*loadByIdsEntity
	found := engine.LoadByIDs([]uint64{1, 2, 3, 4}, &rows, "*")
	assert.False(t, found)
	assert.Len(t, rows, 4)
	assert.Equal(t, "a", rows[0].Name)
	assert.Equal(t, "r1", rows[0].ReferenceOne.Name)
	assert.Equal(t, "b", rows[1].Name)
	assert.Equal(t, "r2", rows[1].ReferenceOne.Name)
	assert.Equal(t, "c", rows[2].Name)
	assert.Nil(t, rows[3])
	engine.GetLocalCache().Remove("a25e2:3")
	engine.GetRedis().Del("a25e2:3")
	found = engine.LoadByIDs([]uint64{1, 2, 3, 4}, &rows, "*")
	assert.False(t, found)
	assert.Len(t, rows, 4)
	assert.Equal(t, "a", rows[0].Name)
	assert.Equal(t, "r1", rows[0].ReferenceOne.Name)
	assert.Equal(t, "b", rows[1].Name)
	assert.Equal(t, "r2", rows[1].ReferenceOne.Name)
	assert.Equal(t, "c", rows[2].Name)
	assert.Nil(t, rows[3])
	engine.LoadByIDs([]uint64{1, 2, 3, 4}, &rows)
	assert.Len(t, rows, 4)
	assert.NotNil(t, rows[0])
	assert.NotNil(t, rows[1])
	assert.NotNil(t, rows[2])
	assert.Nil(t, rows[3])
	found = engine.LoadByIDs([]uint64{1, 2, 3, 4}, &rows)
	assert.False(t, found)
	assert.Len(t, rows, 4)
	assert.NotNil(t, rows[0])
	assert.NotNil(t, rows[1])
	assert.NotNil(t, rows[2])
	assert.Nil(t, rows[3])
	found = engine.LoadByIDs([]uint64{1, 4, 4}, &rows)
	assert.False(t, found)
	assert.Len(t, rows, 3)
	assert.NotNil(t, rows[0])
	assert.Nil(t, rows[1])
	assert.Nil(t, rows[2])
	found = engine.LoadByIDs([]uint64{}, &rows)
	assert.True(t, found)
	assert.Len(t, rows, 0)
	engine.GetRedis().Del("a25e2:1")
	found = engine.LoadByIDs([]uint64{2, 4, 4, 1, 1, 4}, &rows)
	assert.False(t, found)
	assert.Len(t, rows, 6)
	assert.NotNil(t, rows[0])
	assert.Nil(t, rows[1])
	assert.Nil(t, rows[2])
	assert.NotNil(t, rows[3])
	assert.NotNil(t, rows[4])
	assert.Nil(t, rows[5])
	assert.Equal(t, uint64(2), rows[0].GetID())
	assert.Equal(t, uint64(1), rows[3].GetID())
	assert.Equal(t, uint64(1), rows[4].GetID())

	found = engine.LoadByIDs([]uint64{1, 2, 3, 4}, &rows, "ReferenceOne/ReferenceTwo")
	assert.False(t, found)
	assert.Len(t, rows, 4)
	assert.Equal(t, "a", rows[0].Name)
	assert.Equal(t, "r1", rows[0].ReferenceOne.Name)
	assert.Equal(t, "b", rows[1].Name)
	assert.Equal(t, "r2", rows[1].ReferenceOne.Name)
	assert.Equal(t, "c", rows[2].Name)
	assert.Nil(t, rows[3])

	engine.GetRedis().FlushDB()
	found = engine.LoadByIDs([]uint64{1}, &rows)
	assert.True(t, found)
	rows = make([]*loadByIdsEntity, 0)
	engine.LoadByIDs([]uint64{1, 2, 3}, &rows)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint64(1), rows[0].GetID())
	assert.Equal(t, uint64(2), rows[1].GetID())
	assert.Equal(t, uint64(3), rows[2].GetID())

	rows = make([]*loadByIdsEntity, 0)
	found = engine.LoadByIDs([]uint64{1, 2, 3}, &rows)
	assert.True(t, found)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint64(1), rows[0].GetID())
	assert.Equal(t, uint64(2), rows[1].GetID())
	assert.Equal(t, uint64(3), rows[2].GetID())

	engine.GetRedis().FlushDB()
	found = engine.LoadByIDs([]uint64{2}, &rows)
	assert.True(t, found)
	rows = make([]*loadByIdsEntity, 0)
	found = engine.LoadByIDs([]uint64{1, 2, 3}, &rows)
	assert.True(t, found)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint64(1), rows[0].GetID())
	assert.Equal(t, uint64(2), rows[1].GetID())
	assert.Equal(t, uint64(3), rows[2].GetID())

	engine.GetRedis().FlushDB()
	found = engine.LoadByIDs([]uint64{3}, &rows)
	assert.True(t, found)
	rows = make([]*loadByIdsEntity, 0)
	found = engine.LoadByIDs([]uint64{1, 2, 3}, &rows)
	assert.True(t, found)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint64(1), rows[0].GetID())
	assert.Equal(t, uint64(2), rows[1].GetID())
	assert.Equal(t, uint64(3), rows[2].GetID())

	engine.EnableRequestCache()
	engine.GetRedis().FlushDB()
	found = engine.LoadByIDs([]uint64{1}, &rows)
	assert.True(t, found)
	rows = make([]*loadByIdsEntity, 0)
	found = engine.LoadByIDs([]uint64{1, 2, 3}, &rows, "ReferenceOne")
	assert.True(t, found)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint64(1), rows[0].GetID())
	assert.Equal(t, uint64(2), rows[1].GetID())
	assert.Equal(t, uint64(3), rows[2].GetID())
	assert.Equal(t, "a", rows[0].Name)
	assert.Equal(t, "b", rows[1].Name)
	assert.Equal(t, "c", rows[2].Name)

	rows = make([]*loadByIdsEntity, 0)
	found = engine.LoadByIDs([]uint64{1, 2, 3}, &rows)
	assert.True(t, found)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint64(1), rows[0].GetID())
	assert.Equal(t, uint64(2), rows[1].GetID())
	assert.Equal(t, uint64(3), rows[2].GetID())

	engine.GetRedis().FlushDB()
	found = engine.LoadByIDs([]uint64{2}, &rows)
	assert.True(t, found)
	rows = make([]*loadByIdsEntity, 0)
	found = engine.LoadByIDs([]uint64{1, 2, 3}, &rows)
	assert.True(t, found)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint64(1), rows[0].GetID())
	assert.Equal(t, uint64(2), rows[1].GetID())
	assert.Equal(t, uint64(3), rows[2].GetID())

	engine.GetRedis().FlushDB()
	found = engine.LoadByIDs([]uint64{3}, &rows)
	assert.True(t, found)
	rows = make([]*loadByIdsEntity, 0)
	found = engine.LoadByIDs([]uint64{1, 2, 3}, &rows)
	assert.True(t, found)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint64(1), rows[0].GetID())
	assert.Equal(t, uint64(2), rows[1].GetID())
	assert.Equal(t, uint64(3), rows[2].GetID())

	found = engine.LoadByIDs([]uint64{3}, &rows, "ReferenceOne/ReferenceTwo")
	assert.True(t, found)

	assert.PanicsWithError(t, "reference invalid in loadByIdsEntity is not valid", func() {
		engine.LoadByIDs([]uint64{1}, &rows, "invalid")
	})

	assert.PanicsWithError(t, "reference tag Name is not valid", func() {
		engine.LoadByIDs([]uint64{1}, &rows, "Name")
	})

	rows = make([]*loadByIdsEntity, 0)
	engine.LoadByIDs([]uint64{1, 1, 1}, &rows)
	assert.Len(t, rows, 3)
	assert.NotNil(t, rows[0])
	assert.Equal(t, "a", rows[0].Name)
	assert.NotNil(t, rows[1])
	assert.Equal(t, "a", rows[1].Name)
	assert.NotNil(t, rows[2])
	assert.Equal(t, "a", rows[2].Name)

	if local && redis {
		engine.GetLocalCache().Clear()
		rows = make([]*loadByIdsEntity, 0)
		engine.LoadByIDs([]uint64{1, 2, 3}, &rows)
		engine.GetLocalCache().Clear()
		rows = make([]*loadByIdsEntity, 0)
		engine.LoadByIDs([]uint64{1, 2, 3}, &rows)
		assert.Len(t, rows, 3)
		assert.Equal(t, uint64(1), rows[0].GetID())
		assert.Equal(t, uint64(2), rows[1].GetID())
		assert.Equal(t, uint64(3), rows[2].GetID())
	}

	engine = PrepareTables(t, &Registry{}, 5, 6, "")
	assert.PanicsWithError(t, "entity 'beeorm.loadByIdsEntity' is not registered", func() {
		engine.LoadByIDs([]uint64{1}, &rows)
	})
}
