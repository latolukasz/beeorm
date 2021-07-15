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
	engine := PrepareTables(t, &Registry{}, 5, entity, reference, subReference)
	schema := engine.GetRegistry().GetTableSchemaForEntity(entity).(*tableSchema)
	if local {
		schema.localCacheName = "default"
		schema.hasLocalCache = true
	} else {
		schema.localCacheName = ""
		schema.hasLocalCache = false
	}
	if redis {
		schema.redisCacheName = "default"
		schema.hasRedisCache = true
	} else {
		schema.redisCacheName = ""
		schema.hasRedisCache = false
	}

	engine.FlushMany(&loadByIdsEntity{Name: "a", ReferenceOne: &loadByIdsReference{Name: "r1", ReferenceTwo: &loadByIdsSubReference{Name: "s1"}}},
		&loadByIdsEntity{Name: "b", ReferenceOne: &loadByIdsReference{Name: "r2", ReferenceTwo: &loadByIdsSubReference{Name: "s2"}}},
		&loadByIdsEntity{Name: "c"})

	var rows []*loadByIdsEntity
	missing := engine.LoadByIDs([]uint64{1, 2, 3, 4}, &rows, "*")
	assert.True(t, missing)
	assert.Len(t, rows, 4)
	assert.Equal(t, "a", rows[0].Name)
	assert.Equal(t, "r1", rows[0].ReferenceOne.Name)
	assert.Equal(t, "b", rows[1].Name)
	assert.Equal(t, "r2", rows[1].ReferenceOne.Name)
	assert.Equal(t, "c", rows[2].Name)
	assert.Nil(t, rows[3])
	engine.GetLocalCache().Remove("a25e2:3")
	engine.GetRedis().Del("a25e2:3")
	missing = engine.LoadByIDs([]uint64{1, 2, 3, 4}, &rows, "*")
	assert.True(t, missing)
	assert.Len(t, rows, 4)
	assert.Equal(t, "a", rows[0].Name)
	assert.Equal(t, "r1", rows[0].ReferenceOne.Name)
	assert.Equal(t, "b", rows[1].Name)
	assert.Equal(t, "r2", rows[1].ReferenceOne.Name)
	assert.Equal(t, "c", rows[2].Name)
	assert.Nil(t, rows[3])
	missing = engine.LoadByIDs([]uint64{1, 2, 3, 4}, &rows)
	assert.Len(t, rows, 4)
	assert.True(t, missing)
	assert.NotNil(t, rows[0])
	assert.NotNil(t, rows[1])
	assert.NotNil(t, rows[2])
	assert.Nil(t, rows[3])
	missing = engine.LoadByIDs([]uint64{1, 2, 3, 4}, &rows)
	assert.Len(t, rows, 4)
	assert.True(t, missing)
	assert.NotNil(t, rows[0])
	assert.NotNil(t, rows[1])
	assert.NotNil(t, rows[2])
	assert.Nil(t, rows[3])
	missing = engine.LoadByIDs([]uint64{1, 4, 4}, &rows)
	assert.Len(t, rows, 3)
	assert.True(t, missing)
	assert.NotNil(t, rows[0])
	assert.Nil(t, rows[1])
	assert.Nil(t, rows[2])
	engine.LoadByIDs([]uint64{}, &rows)
	assert.Len(t, rows, 0)
	engine.GetRedis().Del("a25e2:1")
	missing = engine.LoadByIDs([]uint64{2, 4, 4, 1, 1, 4}, &rows)
	assert.True(t, missing)
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

	missing = engine.LoadByIDsLazy([]uint64{1, 2, 3, 4}, &rows, "*")
	assert.True(t, missing)
	assert.Len(t, rows, 4)
	assert.Equal(t, "", rows[0].Name)
	assert.True(t, rows[0].IsLazy())
	assert.True(t, rows[1].IsLazy())
	assert.True(t, rows[2].IsLazy())
	assert.Nil(t, rows[3])
	assert.Equal(t, "a", rows[0].GetFieldLazy(engine, "Name"))
	assert.Equal(t, "", rows[0].ReferenceOne.Name)
	assert.Equal(t, "r1", rows[0].ReferenceOne.GetFieldLazy(engine, "Name"))
	assert.Equal(t, "", rows[1].Name)
	assert.Equal(t, "b", rows[1].GetFieldLazy(engine, "Name"))
	assert.Equal(t, "", rows[1].ReferenceOne.Name)
	assert.Equal(t, "r2", rows[1].ReferenceOne.GetFieldLazy(engine, "Name"))
	assert.Equal(t, "", rows[2].Name)
	assert.Equal(t, "c", rows[2].GetFieldLazy(engine, "Name"))
	missing = engine.LoadByIDsLazy([]uint64{1, 2, 4, 3}, &rows, "*")
	assert.True(t, missing)
	assert.NotNil(t, rows[0])
	assert.NotNil(t, rows[1])
	assert.Nil(t, rows[2])
	assert.NotNil(t, rows[3])

	missing = engine.LoadByIDs([]uint64{1, 2, 3, 4}, &rows, "ReferenceOne/ReferenceTwo")
	assert.True(t, missing)
	assert.Len(t, rows, 4)
	assert.Equal(t, "a", rows[0].Name)
	assert.Equal(t, "r1", rows[0].ReferenceOne.Name)
	assert.Equal(t, "b", rows[1].Name)
	assert.Equal(t, "r2", rows[1].ReferenceOne.Name)
	assert.Equal(t, "c", rows[2].Name)
	assert.Nil(t, rows[3])

	missing = engine.LoadByIDs([]uint64{3}, &rows, "ReferenceOne/ReferenceTwo")
	assert.False(t, missing)

	assert.PanicsWithError(t, "reference invalid in loadByIdsEntity is not valid", func() {
		engine.LoadByIDs([]uint64{1}, &rows, "invalid")
	})

	assert.PanicsWithError(t, "reference tag Name is not valid", func() {
		engine.LoadByIDs([]uint64{1}, &rows, "Name")
	})

	rows = make([]*loadByIdsEntity, 0)
	missing = engine.LoadByIDs([]uint64{1, 1, 1}, &rows)
	assert.False(t, missing)
	assert.Len(t, rows, 3)
	assert.NotNil(t, rows[0])
	assert.Equal(t, "a", rows[0].Name)
	assert.NotNil(t, rows[1])
	assert.Equal(t, "a", rows[1].Name)
	assert.NotNil(t, rows[2])
	assert.Equal(t, "a", rows[2].Name)

	engine = PrepareTables(t, &Registry{}, 5)
	assert.PanicsWithError(t, "entity 'beeorm.loadByIdsEntity' is not registered", func() {
		engine.LoadByIDs([]uint64{1}, &rows)
	})
}

// BenchmarkLoadByIDsdLocalCache-12    	  505929	      2110 ns/op	     952 B/op	      10 allocs/op
func BenchmarkLoadByIDsdLocalCache(b *testing.B) {
	benchmarkLoadByIDsLocalCache(b, false)
}

// BenchmarkLoadByIDsLocalCacheLazy-12    	 1360686	       856.5 ns/op	     712 B/op	       6 allocs/op
func BenchmarkLoadByIDsLocalCacheLazy(b *testing.B) {
	benchmarkLoadByIDsLocalCache(b, true)
}

func benchmarkLoadByIDsLocalCache(b *testing.B, lazy bool) {
	entity := &schemaEntity{}
	ref := &schemaEntityRef{}
	registry := &Registry{}
	registry.RegisterEnumStruct("beeorm.TestEnum", TestEnum)
	registry.RegisterLocalCache(10000)
	engine := PrepareTables(nil, registry, 5, entity, ref)

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
		if lazy {
			_ = engine.LoadByIDsLazy(ids, &rows)
		} else {
			_ = engine.LoadByIDs(ids, &rows)
		}
	}
}
