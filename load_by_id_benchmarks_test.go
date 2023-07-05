package beeorm

import (
	"testing"
)

type loadByIDBenchmarkEntity struct {
	ORM     `orm:"localCache;redisCache"`
	ID      uint
	Name    string
	Int     int
	Bool    bool
	Float   float64
	Decimal float32 `orm:"decimal=10,2"`
}

// BenchmarkLoadByIDLocalCache-10    	 3572815	       325.6 ns/op	     152 B/op	       6 allocs/op
func BenchmarkLoadByIDLocalCache(b *testing.B) {
	benchmarkLoadByIDCache(b, true, false)
}

// BenchmarkLoadByIDRedisCache-10    	    1695	    603416 ns/op	     421 B/op	      15 allocs/op
func BenchmarkLoadByIDRedisCache(b *testing.B) {
	benchmarkLoadByIDCache(b, false, true)
}

func benchmarkLoadByIDCache(b *testing.B, local, redis bool) {
	entity := &loadByIDBenchmarkEntity{}
	registry := &Registry{}
	registry.RegisterLocalCache(10000)
	engine := PrepareTables(nil, registry, 5, 6, "", entity)
	schema := engine.GetRegistry().GetEntitySchemaForEntity(entity).(*entitySchema)
	schema.DisableCache(!local, !redis)

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
