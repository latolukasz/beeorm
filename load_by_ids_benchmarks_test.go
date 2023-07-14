package beeorm

import (
	"testing"
)

// BenchmarkLoadByIDsLocalCache-10    	 3970012	       300.0 ns/op	     196 B/op	       2 allocs/op
func BenchmarkLoadByIDsLocalCache(b *testing.B) {
	benchmarkLoadByIDsCache(b, true, false)
}

// BenchmarkLoadByIDsRedisCache-10    	    1947	    611389 ns/op	     512 B/op	      13 allocs/op
func BenchmarkLoadByIDsRedisCache(b *testing.B) {
	benchmarkLoadByIDsCache(b, false, true)
}

func benchmarkLoadByIDsCache(b *testing.B, local, redis bool) {
	entity := &loadByIDBenchmarkEntity{}
	registry := &Registry{}
	registry.RegisterLocalCache(10000)
	engine := PrepareTables(nil, registry, 5, 6, "", entity)
	schema := engine.GetRegistry().GetEntitySchemaForEntity(entity).(*entitySchema)
	schema.DisableCache(!local, !redis)

	const size = 1
	f := engine.NewFlusher()
	ids := make([]uint64, size)
	for i := 0; i < size; i++ {
		entity = &loadByIDBenchmarkEntity{}
		entity.Name = "Name"
		entity.Int = 1
		entity.Float = 1.3
		entity.Decimal = 12.23
		f.Track(entity)
		ids[i] = uint64(i + 1)
	}
	f.Flush()
	var entities []*loadByIDBenchmarkEntity
	_ = engine.LoadByIDs(ids, &entities)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		_ = engine.LoadByIDs(ids, &entities)
	}
}