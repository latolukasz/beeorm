package beeorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type loadByIDEntity struct {
	ORM             `orm:"localCache;redisCache"`
	ID              uint
	Name            string `orm:"max=100"`
	ReferenceOne    *loadByIDReference
	ReferenceSecond *loadByIDReference
	ReferenceThird  *loadByIDReference2
	ReferenceMany   []*loadByIDReferenceMany
}

type loadByIDRedisEntity struct {
	ORM `orm:"redisCache"`
	ID  uint
}

type loadByIDLocalEntity struct {
	ORM `orm:"localCache"`
	ID  uint
}

type loadByIDNoCacheEntity struct {
	ORM
	ID   uint
	Name string
}

type loadByIDReference struct {
	ORM            `orm:"localCache;redisCache"`
	ID             uint
	Name           string
	ReferenceTwo   *loadByIDSubReference
	ReferenceThree *loadByIDSubReference2
}

type loadByIDReferenceMany struct {
	ORM            `orm:"localCache;redisCache"`
	ID             uint
	Name           string
	ReferenceTwo   *loadByIDSubReference
	ReferenceThree *loadByIDSubReference2
}

type loadByIDReference2 struct {
	ORM  `orm:"localCache;redisCache"`
	ID   uint
	Name string
}

type loadByIDSubReference struct {
	ORM  `orm:"localCache;redisCache"`
	ID   uint
	Name string
}

type loadByIDSubReference2 struct {
	ORM          `orm:"localCache"`
	ID           uint
	Name         string
	ReferenceTwo *loadByIDSubReference
}

type loadByIDBenchmarkEntity struct {
	ORM
	ID      uint
	Name    string
	Int     int
	Bool    bool
	Float   float64
	Decimal float32 `orm:"decimal=10,2"`
}

func TestLoadById(t *testing.T) {
	var entity *loadByIDEntity
	var entityRedis *loadByIDRedisEntity
	var entityLocal *loadByIDLocalEntity
	var entityNoCache *loadByIDNoCacheEntity
	var reference *loadByIDReference
	var reference2 *loadByIDReference2
	var subReference2 *loadByIDSubReference2
	var subReference *loadByIDSubReference
	var refMany *loadByIDReferenceMany
	engine := prepareTables(t, &Registry{}, 5, entity, entityRedis, entityLocal, entityNoCache, reference, subReference,
		subReference2, reference2, refMany)
	e := &loadByIDEntity{Name: "a", ReferenceOne: &loadByIDReference{Name: "r1", ReferenceTwo: &loadByIDSubReference{Name: "s1"}}}
	e.ReferenceSecond = &loadByIDReference{Name: "r11", ReferenceTwo: &loadByIDSubReference{Name: "s1"},
		ReferenceThree: &loadByIDSubReference2{Name: "s11", ReferenceTwo: &loadByIDSubReference{Name: "hello"}}}
	e.ReferenceThird = &loadByIDReference2{Name: "r2A"}
	e.ReferenceMany = []*loadByIDReferenceMany{{Name: "John"}}
	engine.FlushMany(e,
		&loadByIDEntity{Name: "b", ReferenceOne: &loadByIDReference{Name: "r2", ReferenceTwo: &loadByIDSubReference{Name: "s2"}}},
		&loadByIDEntity{Name: "c"}, &loadByIDNoCacheEntity{Name: "a"}, &loadByIDLocalEntity{})

	engine.FlushMany(&loadByIDReferenceMany{Name: "rm1", ID: 100}, &loadByIDReferenceMany{Name: "rm2", ID: 101}, &loadByIDReferenceMany{Name: "rm3", ID: 102})
	engine.FlushMany(&loadByIDEntity{Name: "eMany", ID: 200, ReferenceMany: []*loadByIDReferenceMany{{ID: 100}, {ID: 101}, {ID: 102}}})

	entity = &loadByIDEntity{}
	localLogger := &testLogHandler{}
	engine.RegisterQueryLogger(localLogger, false, false, true)
	found := engine.LoadByID(1, entity, "ReferenceOne/ReferenceTwo",
		"ReferenceSecond/ReferenceTwo", "ReferenceSecond/ReferenceThree/ReferenceTwo")
	assert.True(t, found)
	assert.Len(t, localLogger.Logs, 5)
	assert.True(t, entity.IsLoaded())
	assert.False(t, entity.IsLazy())
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.False(t, entity.ReferenceOne.IsLazy())
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())
	assert.False(t, entity.ReferenceOne.ReferenceTwo.IsLazy())
	assert.True(t, entity.ReferenceSecond.IsLoaded())
	assert.False(t, entity.ReferenceSecond.IsLazy())
	assert.True(t, entity.ReferenceSecond.ReferenceTwo.IsLoaded())
	assert.False(t, entity.ReferenceSecond.ReferenceTwo.IsLazy())
	assert.True(t, entity.ReferenceSecond.ReferenceThree.IsLoaded())
	assert.False(t, entity.ReferenceSecond.ReferenceThree.IsLazy())
	assert.True(t, entity.ReferenceSecond.ReferenceThree.ReferenceTwo.IsLoaded())
	assert.False(t, entity.ReferenceSecond.ReferenceThree.ReferenceTwo.IsLazy())

	entity = &loadByIDEntity{}
	found = engine.LoadByID(1, entity, "ReferenceThird", "ReferenceOne", "ReferenceMany")
	assert.True(t, found)
	assert.Equal(t, "a", entity.Name)
	assert.Equal(t, "r2A", entity.ReferenceThird.Name)
	assert.Equal(t, "r1", entity.ReferenceOne.Name)
	assert.Len(t, entity.ReferenceMany, 1)
	assert.Equal(t, "John", entity.ReferenceMany[0].Name)

	entity = &loadByIDEntity{}
	found = engine.LoadByIDLazy(1, entity, "ReferenceOne/ReferenceTwo",
		"ReferenceSecond/ReferenceTwo", "ReferenceSecond/ReferenceThree/ReferenceTwo")
	assert.True(t, found)
	assert.True(t, entity.IsLoaded())
	assert.True(t, entity.IsLazy())
	assert.Equal(t, "", entity.Name)
	assert.Equal(t, "a", entity.GetFieldLazy("Name"))
	entity.Fill()
	assert.Equal(t, "a", entity.Name)
	assert.IsType(t, reference, entity.ReferenceOne)
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.True(t, entity.ReferenceOne.IsLazy())
	assert.Equal(t, "r1", entity.ReferenceOne.GetFieldLazy("Name"))
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())
	assert.True(t, entity.ReferenceSecond.IsLoaded())
	assert.True(t, entity.ReferenceSecond.IsLoaded())
	assert.Equal(t, "r11", entity.ReferenceSecond.GetFieldLazy("Name"))
	assert.True(t, entity.ReferenceSecond.ReferenceTwo.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceTwo.IsLazy())
	assert.Equal(t, "s1", entity.ReferenceSecond.ReferenceTwo.GetFieldLazy("Name"))
	assert.True(t, entity.ReferenceSecond.ReferenceThree.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceThree.IsLazy())
	assert.Equal(t, "s11", entity.ReferenceSecond.ReferenceThree.GetFieldLazy("Name"))
	assert.True(t, entity.ReferenceSecond.ReferenceThree.ReferenceTwo.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceThree.ReferenceTwo.IsLazy())
	assert.Equal(t, "hello", entity.ReferenceSecond.ReferenceThree.ReferenceTwo.GetFieldLazy("Name"))

	entity = &loadByIDEntity{}
	found = engine.LoadByID(1, entity, "ReferenceOne/ReferenceTwo")
	assert.True(t, found)
	assert.Equal(t, uint(1), entity.ID)
	assert.Equal(t, "a", entity.Name)
	assert.Equal(t, "r1", entity.ReferenceOne.Name)
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.Equal(t, "s1", entity.ReferenceOne.ReferenceTwo.Name)
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())

	entity = &loadByIDEntity{ID: 1}
	engine.Load(entity, "ReferenceOne/ReferenceTwo")
	assert.Equal(t, "a", entity.Name)
	assert.False(t, entity.IsLazy())
	assert.Equal(t, "r1", entity.ReferenceOne.Name)
	assert.False(t, entity.ReferenceOne.IsLazy())
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.Equal(t, "s1", entity.ReferenceOne.ReferenceTwo.Name)
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())

	entity = &loadByIDEntity{ID: 1}
	engine.LoadLazy(entity, "ReferenceOne/ReferenceTwo")
	assert.Equal(t, "", entity.Name)
	assert.Equal(t, "a", entity.GetFieldLazy("Name"))
	assert.True(t, entity.IsLazy())
	assert.Equal(t, "r1", entity.ReferenceOne.GetFieldLazy("Name"))
	assert.True(t, entity.ReferenceOne.IsLazy())
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.Equal(t, "s1", entity.ReferenceOne.ReferenceTwo.GetFieldLazy("Name"))
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLazy())

	entityNoCache = &loadByIDNoCacheEntity{}
	found = engine.LoadByID(1, entityNoCache, "*")
	assert.True(t, found)
	assert.Equal(t, uint(1), entityNoCache.ID)
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

	entity = &loadByIDEntity{}
	found = engine.LoadByID(200, entity, "ReferenceMany")
	assert.True(t, found)
	assert.Len(t, entity.ReferenceMany, 3)
	assert.Equal(t, uint(100), entity.ReferenceMany[0].ID)
	assert.Equal(t, uint(101), entity.ReferenceMany[1].ID)
	assert.Equal(t, uint(102), entity.ReferenceMany[2].ID)
	assert.Equal(t, "rm1", entity.ReferenceMany[0].Name)
	assert.Equal(t, "rm2", entity.ReferenceMany[1].Name)
	assert.Equal(t, "rm3", entity.ReferenceMany[2].Name)
	assert.True(t, entity.ReferenceMany[0].IsLoaded())
	assert.True(t, entity.ReferenceMany[1].IsLoaded())
	assert.True(t, entity.ReferenceMany[2].IsLoaded())

	entity = &loadByIDEntity{}
	found = engine.LoadByIDLazy(200, entity, "ReferenceMany")
	assert.True(t, found)
	assert.Len(t, entity.ReferenceMany, 3)
	assert.Equal(t, uint(100), entity.ReferenceMany[0].ID)
	assert.Equal(t, uint(101), entity.ReferenceMany[1].ID)
	assert.Equal(t, uint(102), entity.ReferenceMany[2].ID)
	assert.Equal(t, "", entity.ReferenceMany[0].Name)
	assert.Equal(t, "", entity.ReferenceMany[1].Name)
	assert.Equal(t, "", entity.ReferenceMany[2].Name)
	assert.True(t, entity.ReferenceMany[0].IsLazy())
	assert.True(t, entity.ReferenceMany[1].IsLazy())
	assert.True(t, entity.ReferenceMany[2].IsLazy())
	assert.True(t, entity.ReferenceMany[0].IsLoaded())
	assert.True(t, entity.ReferenceMany[1].IsLoaded())
	assert.True(t, entity.ReferenceMany[2].IsLoaded())
	assert.Equal(t, "rm1", entity.ReferenceMany[0].GetFieldLazy("Name"))
	assert.Equal(t, "rm2", entity.ReferenceMany[1].GetFieldLazy("Name"))
	assert.Equal(t, "rm3", entity.ReferenceMany[2].GetFieldLazy("Name"))
	entity.ReferenceMany[0].Fill()
	assert.Equal(t, "rm1", entity.ReferenceMany[0].Name)
	entity.ReferenceMany[0].Fill()
	assert.False(t, entity.ReferenceMany[0].IsLazy())

	entityLocalCache := &loadByIDLocalEntity{}
	found = engine.LoadByID(1, entityLocalCache)
	assert.True(t, found)

	engine = prepareTables(t, &Registry{}, 5)
	entity = &loadByIDEntity{}
	assert.PanicsWithError(t, "entity 'beeorm.loadByIDEntity' is not registered", func() {
		engine.LoadByID(1, entity)
	})
}

// BenchmarkLoadByIDdLocalCache-12    	 5869000	       203.3 ns/op	       8 B/op	       1 allocs/op
func BenchmarkLoadByIDdLocalCache(b *testing.B) {
	benchmarkLoadByIDLocalCache(b, false, true, false)
}

// BenchmarkLoadByIDLocalCacheLazy-12    	 9291440	       132.9 ns/op	       8 B/op	       1 allocs/op
func BenchmarkLoadByIDLocalCacheLazy(b *testing.B) {
	benchmarkLoadByIDLocalCache(b, true, true, false)
}

func BenchmarkLoadByIDRedisCacheLazy(b *testing.B) {
	benchmarkLoadByIDLocalCache(b, true, false, true)
}

func benchmarkLoadByIDLocalCache(b *testing.B, lazy, local, redis bool) {
	entity := &loadByIDBenchmarkEntity{}
	registry := &Registry{}
	registry.RegisterEnumStruct("beeorm.TestEnum", TestEnum)
	registry.RegisterLocalCache(10000)
	engine := prepareTables(nil, registry, 5, entity)
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

	entity.Name = "Name"
	entity.Int = 1
	entity.Float = 1.3
	entity.Decimal = 12.23
	engine.Flush(entity)
	_ = engine.LoadByID(1, entity)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		if lazy {
			_ = engine.LoadByIDLazy(1, entity)
		} else {
			_ = engine.LoadByID(1, entity)
		}
	}
}
