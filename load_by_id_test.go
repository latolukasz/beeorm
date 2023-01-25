package beeorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type loadByIDEntity struct {
	ORM             `orm:"localCache;redisCache"`
	Name            string `orm:"max=100"`
	ReferenceOne    *loadByIDReference
	ReferenceSecond *loadByIDReference
	ReferenceThird  *loadByIDReference2
}

type loadByIDRedisEntity struct {
	ORM `orm:"redisCache"`
}

type loadByIDLocalEntity struct {
	ORM `orm:"localCache"`
}

type loadByIDNoCacheEntity struct {
	ORM
	Name string
}

type loadByIDReference struct {
	ORM            `orm:"localCache;redisCache"`
	Name           string
	ReferenceTwo   *loadByIDSubReference
	ReferenceThree *loadByIDSubReference2
}

type loadByIDReference2 struct {
	ORM  `orm:"localCache;redisCache"`
	Name string
}

type loadByIDSubReference struct {
	ORM  `orm:"localCache;redisCache"`
	Name string
}

type loadByIDSubReference2 struct {
	ORM          `orm:"localCache"`
	Name         string
	ReferenceTwo *loadByIDSubReference
}

type loadByIDBenchmarkEntity struct {
	ORM
	Name    string
	Int     int
	Bool    bool
	Float   float64
	Decimal float32 `orm:"decimal=10,2"`
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
	engine := prepareTables(t, &Registry{}, 5, 6, "", entity, entityRedis, entityLocal, entityNoCache, reference, subReference,
		subReference2, reference2)

	schemas := make([]TableSchema, 0)
	registry := engine.GetRegistry()
	schemas = append(schemas, registry.GetTableSchemaForEntity(entity))

	schemas = append(schemas, registry.GetTableSchemaForEntity(entityRedis))
	schemas = append(schemas, registry.GetTableSchemaForEntity(entityLocal))
	schemas = append(schemas, registry.GetTableSchemaForEntity(entityNoCache))
	schemas = append(schemas, registry.GetTableSchemaForEntity(reference))
	schemas = append(schemas, registry.GetTableSchemaForEntity(reference2))
	schemas = append(schemas, registry.GetTableSchemaForEntity(subReference2))
	schemas = append(schemas, registry.GetTableSchemaForEntity(subReference))

	if local {
		for _, schema := range schemas {
			schema.(*tableSchema).localCacheName = "default"
			schema.(*tableSchema).hasLocalCache = true
		}
	} else {
		for _, schema := range schemas {
			schema.(*tableSchema).localCacheName = ""
			schema.(*tableSchema).hasLocalCache = false
		}
	}
	if redis {
		for _, schema := range schemas {
			schema.(*tableSchema).redisCacheName = "default"
			schema.(*tableSchema).hasRedisCache = true
		}
	} else {
		for _, schema := range schemas {
			schema.(*tableSchema).redisCacheName = ""
			schema.(*tableSchema).hasRedisCache = false
		}
	}

	e := &loadByIDEntity{Name: "a", ReferenceOne: &loadByIDReference{Name: "r1", ReferenceTwo: &loadByIDSubReference{Name: "s1"}}}
	e.ReferenceSecond = &loadByIDReference{Name: "r11", ReferenceTwo: &loadByIDSubReference{Name: "s1"},
		ReferenceThree: &loadByIDSubReference2{Name: "s11", ReferenceTwo: &loadByIDSubReference{Name: "hello"}}}
	e.ReferenceThird = &loadByIDReference2{Name: "r2A"}
	engine.EnableQueryDebugCustom(true, false, false)
	engine.Flush(e,
		&loadByIDEntity{Name: "b", ReferenceOne: &loadByIDReference{Name: "r2", ReferenceTwo: &loadByIDSubReference{Name: "s2"}}},
		&loadByIDEntity{Name: "c"}, &loadByIDNoCacheEntity{Name: "a"}, &loadByIDLocalEntity{})
	return
	engine.GetLocalCache().Clear()

	entity = &loadByIDEntity{}
	found := engine.LoadByID(1, entity, "ReferenceOne/ReferenceTwo",
		"ReferenceSecond/ReferenceTwo", "ReferenceSecond/ReferenceThree/ReferenceTwo")
	assert.True(t, found)
	assert.True(t, entity.IsLoaded())
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())
	assert.True(t, entity.ReferenceSecond.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceTwo.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceThree.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceThree.ReferenceTwo.IsLoaded())

	schema := engine.GetRegistry().GetTableSchemaForCachePrefix("f3b2d")
	assert.NotNil(t, schema)
	assert.Equal(t, "loadByIDEntity", schema.GetTableName())
	schema = engine.GetRegistry().GetTableSchemaForCachePrefix("invalid")
	assert.Nil(t, schema)

	entity = &loadByIDEntity{}
	found = engine.LoadByID(1, entity, "ReferenceThird", "ReferenceOne", "ReferenceMany")
	assert.True(t, found)
	assert.Equal(t, "a", entity.Name)
	assert.Equal(t, "r2A", entity.ReferenceThird.Name)
	assert.Equal(t, "r1", entity.ReferenceOne.Name)

	entity = &loadByIDEntity{}
	found = engine.LoadByID(1, entity, "ReferenceOne/ReferenceTwo")
	assert.True(t, found)
	assert.Equal(t, uint64(1), entity.GetID())
	assert.Equal(t, "a", entity.Name)
	assert.Equal(t, "r1", entity.ReferenceOne.Name)
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.Equal(t, "s1", entity.ReferenceOne.ReferenceTwo.Name)
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())

	entity = &loadByIDEntity{}
	entity.SetID(1)
	engine.Load(entity, "ReferenceOne/ReferenceTwo")
	assert.Equal(t, "a", entity.Name)
	assert.Equal(t, "r1", entity.ReferenceOne.Name)
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.Equal(t, "s1", entity.ReferenceOne.ReferenceTwo.Name)
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())
	engine.Load(entity, "ReferenceOne/ReferenceTwo")
	assert.Equal(t, "a", entity.Name)
	assert.Equal(t, "r1", entity.ReferenceOne.Name)
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.Equal(t, "s1", entity.ReferenceOne.ReferenceTwo.Name)
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())

	entityNoCache = &loadByIDNoCacheEntity{}
	found = engine.LoadByID(1, entityNoCache, "*")
	assert.True(t, found)
	assert.Equal(t, uint64(1), entityNoCache.GetID())
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

	entityLocalCache := &loadByIDLocalEntity{}
	found = engine.LoadByID(1, entityLocalCache)
	assert.True(t, found)

	if local && redis {
		engine.LoadByID(999, entityLocalCache)
		engine.GetLocalCache().Clear()
		assert.True(t, engine.LoadByID(1, entityLocalCache))
		assert.False(t, engine.LoadByID(999, entityLocalCache))
	}

	engine = prepareTables(t, &Registry{}, 5, 6, "")
	entity = &loadByIDEntity{}
	assert.PanicsWithError(t, "entity 'beeorm.loadByIDEntity' is not registered", func() {
		engine.LoadByID(1, entity)
	})
}

// BenchmarkLoadByIDdLocalCache-10    	 3674497	       323.8 ns/op	     152 B/op	       6 allocs/op
func BenchmarkLoadByIDdLocalCache(b *testing.B) {
	benchmarkLoadByIDLocalCache(b, true, false)
}

func benchmarkLoadByIDLocalCache(b *testing.B, local, redis bool) {
	entity := &loadByIDBenchmarkEntity{}
	registry := &Registry{}
	registry.RegisterEnumStruct("beeorm.TestEnum", TestEnum)
	registry.RegisterLocalCache(10000)
	engine := prepareTables(nil, registry, 5, 6, "", entity)
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
		_ = engine.LoadByID(1, entity)
	}
}
