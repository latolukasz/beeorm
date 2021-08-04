package beeorm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type loadByIdsEntity struct {
	ORM          `orm:"localCache;redisCache"`
	ID           uint
	Name         string `orm:"max=100"`
	ReferenceOne *loadByIdsReference
}

type loadByIdsReference struct {
	ORM          `orm:"localCache;redisCache"`
	ID           uint
	Name         string
	ReferenceTwo *loadByIdsSubReference
}

type loadByIdsSubReference struct {
	ORM  `orm:"localCache;redisCache"`
	ID   uint
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
	engine, def := prepareTables(t, &Registry{}, 5, entity, reference, subReference)
	defer def()
	schema := engine.GetRegistry().GetTableSchemaForEntity(entity).(*tableSchema)
	schema2 := engine.GetRegistry().GetTableSchemaForEntity(reference).(*tableSchema)
	schema3 := engine.GetRegistry().GetTableSchemaForEntity(subReference).(*tableSchema)
	if local {
		schema.localCacheName = "default"
		schema.hasLocalCache = true
		schema2.localCacheName = "default"
		schema2.hasLocalCache = true
		schema3.localCacheName = "default"
		schema3.hasLocalCache = true
	} else {
		schema.localCacheName = ""
		schema.hasLocalCache = false
		schema2.localCacheName = ""
		schema2.hasLocalCache = false
		schema3.localCacheName = ""
		schema3.hasLocalCache = false
	}
	if redis {
		schema.redisCacheName = "default"
		schema.hasRedisCache = true
		schema2.redisCacheName = "default"
		schema2.hasRedisCache = true
		schema3.redisCacheName = "default"
		schema3.hasRedisCache = true
	} else {
		schema.redisCacheName = ""
		schema.hasRedisCache = false
		schema2.redisCacheName = ""
		schema2.hasRedisCache = false
		schema3.redisCacheName = ""
		schema3.hasRedisCache = false
	}

	engine.FlushMany(&loadByIdsEntity{Name: "a", ReferenceOne: &loadByIdsReference{Name: "r1", ReferenceTwo: &loadByIdsSubReference{Name: "s1"}}},
		&loadByIdsEntity{Name: "b", ReferenceOne: &loadByIdsReference{Name: "r2", ReferenceTwo: &loadByIdsSubReference{Name: "s2"}}},
		&loadByIdsEntity{Name: "c"})

	var rows []*loadByIdsEntity
	engine.LoadByIDs([]uint64{1, 2, 3, 4}, &rows, "*")
	assert.Len(t, rows, 4)
	assert.Equal(t, "a", rows[0].Name)
	assert.Equal(t, "r1", rows[0].ReferenceOne.Name)
	assert.Equal(t, "b", rows[1].Name)
	assert.Equal(t, "r2", rows[1].ReferenceOne.Name)
	assert.Equal(t, "c", rows[2].Name)
	assert.Nil(t, rows[3])
	engine.GetLocalCache().Remove("a25e2:3")
	engine.GetRedis().Del("a25e2:3")
	engine.LoadByIDs([]uint64{1, 2, 3, 4}, &rows, "*")
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
	engine.LoadByIDs([]uint64{1, 2, 3, 4}, &rows)
	assert.Len(t, rows, 4)
	assert.NotNil(t, rows[0])
	assert.NotNil(t, rows[1])
	assert.NotNil(t, rows[2])
	assert.Nil(t, rows[3])
	engine.LoadByIDs([]uint64{1, 4, 4}, &rows)
	assert.Len(t, rows, 3)
	assert.NotNil(t, rows[0])
	assert.Nil(t, rows[1])
	assert.Nil(t, rows[2])
	engine.LoadByIDs([]uint64{}, &rows)
	assert.Len(t, rows, 0)
	engine.GetRedis().Del("a25e2:1")
	engine.LoadByIDs([]uint64{2, 4, 4, 1, 1, 4}, &rows)
	assert.Len(t, rows, 6)
	assert.NotNil(t, rows[0])
	assert.Nil(t, rows[1])
	assert.Nil(t, rows[2])
	assert.NotNil(t, rows[3])
	assert.NotNil(t, rows[4])
	assert.Nil(t, rows[5])
	assert.Equal(t, uint(2), rows[0].ID)
	assert.Equal(t, uint(1), rows[3].ID)
	assert.Equal(t, uint(1), rows[4].ID)

	engine.LoadByIDs([]uint64{1, 2, 3, 4}, &rows, "ReferenceOne/ReferenceTwo")
	assert.Len(t, rows, 4)
	assert.Equal(t, "a", rows[0].Name)
	assert.Equal(t, "r1", rows[0].ReferenceOne.Name)
	assert.Equal(t, "b", rows[1].Name)
	assert.Equal(t, "r2", rows[1].ReferenceOne.Name)
	assert.Equal(t, "c", rows[2].Name)
	assert.Nil(t, rows[3])

	engine.GetRedis().FlushDB()
	engine.LoadByIDs([]uint64{1}, &rows)
	rows = make([]*loadByIdsEntity, 0)
	engine.LoadByIDs([]uint64{1, 2, 3}, &rows)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Equal(t, uint(2), rows[1].ID)
	assert.Equal(t, uint(3), rows[2].ID)

	rows = make([]*loadByIdsEntity, 0)
	engine.LoadByIDs([]uint64{1, 2, 3}, &rows)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Equal(t, uint(2), rows[1].ID)
	assert.Equal(t, uint(3), rows[2].ID)

	engine.GetRedis().FlushDB()
	engine.LoadByIDs([]uint64{2}, &rows)
	rows = make([]*loadByIdsEntity, 0)
	engine.LoadByIDs([]uint64{1, 2, 3}, &rows)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Equal(t, uint(2), rows[1].ID)
	assert.Equal(t, uint(3), rows[2].ID)

	engine.GetRedis().FlushDB()
	engine.LoadByIDs([]uint64{3}, &rows)
	rows = make([]*loadByIdsEntity, 0)
	engine.LoadByIDs([]uint64{1, 2, 3}, &rows)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Equal(t, uint(2), rows[1].ID)
	assert.Equal(t, uint(3), rows[2].ID)

	engine.EnableRequestCache()
	engine.GetRedis().FlushDB()
	engine.LoadByIDs([]uint64{1}, &rows)
	rows = make([]*loadByIdsEntity, 0)
	engine.LoadByIDs([]uint64{1, 2, 3}, &rows, "ReferenceOne")
	assert.Len(t, rows, 3)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Equal(t, uint(2), rows[1].ID)
	assert.Equal(t, uint(3), rows[2].ID)
	assert.Equal(t, "a", rows[0].Name)
	assert.Equal(t, "b", rows[1].Name)
	assert.Equal(t, "c", rows[2].Name)

	rows = make([]*loadByIdsEntity, 0)
	engine.LoadByIDs([]uint64{1, 2, 3}, &rows)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Equal(t, uint(2), rows[1].ID)
	assert.Equal(t, uint(3), rows[2].ID)

	engine.GetRedis().FlushDB()
	engine.LoadByIDs([]uint64{2}, &rows)
	rows = make([]*loadByIdsEntity, 0)
	engine.LoadByIDs([]uint64{1, 2, 3}, &rows)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Equal(t, uint(2), rows[1].ID)
	assert.Equal(t, uint(3), rows[2].ID)

	engine.GetRedis().FlushDB()
	engine.LoadByIDs([]uint64{3}, &rows)
	rows = make([]*loadByIdsEntity, 0)
	engine.LoadByIDs([]uint64{1, 2, 3}, &rows)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Equal(t, uint(2), rows[1].ID)
	assert.Equal(t, uint(3), rows[2].ID)

	engine.LoadByIDs([]uint64{3}, &rows, "ReferenceOne/ReferenceTwo")

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
		assert.Equal(t, uint(1), rows[0].ID)
		assert.Equal(t, uint(2), rows[1].ID)
		assert.Equal(t, uint(3), rows[2].ID)
	}

	engine, def = prepareTables(t, &Registry{}, 5)
	defer def()
	assert.PanicsWithError(t, "entity 'beeorm.loadByIdsEntity' is not registered", func() {
		engine.LoadByIDs([]uint64{1}, &rows)
	})
}

// BenchmarkLoadByIDsdLocalCache-12    	  505929	      2110 ns/op	     952 B/op	      10 allocs/op
func BenchmarkLoadByIDsdLocalCache(b *testing.B) {
	benchmarkLoadByIDsLocalCache(b)
}

func benchmarkLoadByIDsLocalCache(b *testing.B) {
	entity := &schemaEntity{}
	ref := &schemaEntityRef{}
	registry := &Registry{}
	registry.RegisterEnumStruct("beeorm.TestEnum", TestEnum)
	registry.RegisterLocalCache(10000)
	engine, def := prepareTables(nil, registry, 5, entity, ref)
	defer def()

	ids := make([]uint64, 0)
	for i := 1; i <= 1; i++ {
		e := &schemaEntity{}
		e.GetID()
		e.Name = fmt.Sprintf("Name %d", i)
		e.Uint32 = uint32(i)
		e.Int32 = int32(i)
		e.Int8 = 1
		e.Enum = TestEnum.A
		e.RefOne = &schemaEntityRef{}
		engine.Flush(e)
		_ = engine.LoadByID(uint64(i), e)
		ids = append(ids, uint64(i))
	}
	rows := make([]*schemaEntity, 0)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		engine.LoadByIDs(ids, &rows)
	}
}
