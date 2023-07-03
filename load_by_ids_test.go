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
	engine := prepareTables(t, &Registry{}, 5, 6, "", entity, reference, subReference)
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

	engine.Flush(&loadByIdsEntity{Name: "a", ReferenceOne: &loadByIdsReference{Name: "r1", ReferenceTwo: &loadByIdsSubReference{Name: "s1"}}},
		&loadByIdsEntity{Name: "b", ReferenceOne: &loadByIdsReference{Name: "r2", ReferenceTwo: &loadByIdsSubReference{Name: "s2"}}},
		&loadByIdsEntity{Name: "c"})

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
	assert.Equal(t, uint(2), rows[0].ID)
	assert.Equal(t, uint(1), rows[3].ID)
	assert.Equal(t, uint(1), rows[4].ID)

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
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Equal(t, uint(2), rows[1].ID)
	assert.Equal(t, uint(3), rows[2].ID)

	rows = make([]*loadByIdsEntity, 0)
	found = engine.LoadByIDs([]uint64{1, 2, 3}, &rows)
	assert.True(t, found)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Equal(t, uint(2), rows[1].ID)
	assert.Equal(t, uint(3), rows[2].ID)

	engine.GetRedis().FlushDB()
	found = engine.LoadByIDs([]uint64{2}, &rows)
	assert.True(t, found)
	rows = make([]*loadByIdsEntity, 0)
	found = engine.LoadByIDs([]uint64{1, 2, 3}, &rows)
	assert.True(t, found)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Equal(t, uint(2), rows[1].ID)
	assert.Equal(t, uint(3), rows[2].ID)

	engine.GetRedis().FlushDB()
	found = engine.LoadByIDs([]uint64{3}, &rows)
	assert.True(t, found)
	rows = make([]*loadByIdsEntity, 0)
	found = engine.LoadByIDs([]uint64{1, 2, 3}, &rows)
	assert.True(t, found)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Equal(t, uint(2), rows[1].ID)
	assert.Equal(t, uint(3), rows[2].ID)

	engine.EnableRequestCache()
	engine.GetRedis().FlushDB()
	found = engine.LoadByIDs([]uint64{1}, &rows)
	assert.True(t, found)
	rows = make([]*loadByIdsEntity, 0)
	found = engine.LoadByIDs([]uint64{1, 2, 3}, &rows, "ReferenceOne")
	assert.True(t, found)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Equal(t, uint(2), rows[1].ID)
	assert.Equal(t, uint(3), rows[2].ID)
	assert.Equal(t, "a", rows[0].Name)
	assert.Equal(t, "b", rows[1].Name)
	assert.Equal(t, "c", rows[2].Name)

	rows = make([]*loadByIdsEntity, 0)
	found = engine.LoadByIDs([]uint64{1, 2, 3}, &rows)
	assert.True(t, found)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Equal(t, uint(2), rows[1].ID)
	assert.Equal(t, uint(3), rows[2].ID)

	engine.GetRedis().FlushDB()
	found = engine.LoadByIDs([]uint64{2}, &rows)
	assert.True(t, found)
	rows = make([]*loadByIdsEntity, 0)
	found = engine.LoadByIDs([]uint64{1, 2, 3}, &rows)
	assert.True(t, found)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Equal(t, uint(2), rows[1].ID)
	assert.Equal(t, uint(3), rows[2].ID)

	engine.GetRedis().FlushDB()
	found = engine.LoadByIDs([]uint64{3}, &rows)
	assert.True(t, found)
	rows = make([]*loadByIdsEntity, 0)
	found = engine.LoadByIDs([]uint64{1, 2, 3}, &rows)
	assert.True(t, found)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Equal(t, uint(2), rows[1].ID)
	assert.Equal(t, uint(3), rows[2].ID)

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
		assert.Equal(t, uint(1), rows[0].ID)
		assert.Equal(t, uint(2), rows[1].ID)
		assert.Equal(t, uint(3), rows[2].ID)
	}

	engine = prepareTables(t, &Registry{}, 5, 6, "")
	assert.PanicsWithError(t, "entity 'beeorm.loadByIdsEntity' is not registered", func() {
		engine.LoadByIDs([]uint64{1}, &rows)
	})
}

// BenchmarkLoadByIDsdLocalCache-10    	  112261	     10702 ns/op	    9724 B/op	      55 allocs/op
func BenchmarkLoadByIDsdLocalCache(b *testing.B) {
	benchmarkLoadByIDsLocalCache(b)
}

func benchmarkLoadByIDsLocalCache(b *testing.B) {
	entity := &schemaEntity{}
	ref := &schemaEntityRef{}
	registry := &Registry{}
	registry.RegisterEnumStruct("beeorm.TestEnum", TestEnum)
	registry.RegisterLocalCache(10000)
	engine := prepareTables(nil, registry, 5, 6, "", entity, ref)

	ids := make([]uint64, 0)
	for i := 1; i <= 10; i++ {
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

// BenchmarkReadByIDsdLocalCache-10    	  112815	     10589 ns/op	    9724 B/op	      55 allocs/op
// BenchmarkReadByIDsdLocalCache-10    	  651699	      1776 ns/op	     923 B/op	      15 allocs/op
// BenchmarkReadByIDsdLocalCache-10    	  686292	      1707 ns/op	     819 B/op	      13 allocs/op
// BenchmarkReadByIDsdLocalCache-10    	  745702	      1545 ns/op	     499 B/op	      11 allocs/op
func BenchmarkReadByIDsdLocalCache(b *testing.B) {
	entity := &schemaEntity{}
	ref := &schemaEntityRef{}
	registry := &Registry{}
	registry.RegisterEnumStruct("beeorm.TestEnum", TestEnum)
	registry.RegisterLocalCache(10000)
	engine := prepareTables(nil, registry, 5, 6, "", entity, ref)

	ids := make([]uint64, 0)
	for i := 1; i <= 10; i++ {
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
	var rows = make([]*schemaEntity, 0)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		engine.ReadByIDs(ids, &rows)
	}
}
