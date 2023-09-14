package beeorm

//
//import (
//	"testing"
//)
//
//// BenchmarkReadByIDsdLocalCache-10    	   50936	     21584 ns/op	    8618 B/op	     109 allocs/op // OLD
//// BenchmarkLoadByIDsLocalCache-10    	 1068458	      1096 ns/op	       0 B/op	       0 allocs/op
//func BenchmarkLoadByIDsLocalCache(b *testing.B) {
//	benchmarkLoadByIDsCache(b, true, false)
//}
//
//// BenchmarkLoadByIDsRedisCache-10    	    1947	    611389 ns/op	     512 B/op	      13 allocs/op
//func BenchmarkLoadByIDsRedisCache(b *testing.B) {
//	benchmarkLoadByIDsCache(b, false, true)
//}
//
//func benchmarkLoadByIDsCache(b *testing.B, local, redis bool) {
//	entity := &loadByIDBenchmarkEntity{}
//	registry := &Registry{}
//	registry.RegisterLocalCache(10000)
//	c := PrepareTables(nil, registry, 5, 6, "", entity)
//	schema := GetEntitySchema[*loadByIDBenchmarkEntity](c)
//	schema.DisableCache(!local, !redis)
//
//	const size = 100
//	f := c.Flusher()
//	ids := make([]uint64, size)
//	for i := 0; i < size; i++ {
//		entity = &loadByIDBenchmarkEntity{}
//		entity.Name = "Name"
//		entity.Int = 1
//		entity.Float = 1.3
//		entity.Decimal = 12.23
//		f.Track(entity)
//		ids[i] = uint64(i + 1)
//	}
//	f.Flush()
//	_ = GetByIDs[*loadByIDBenchmarkEntity](c, ids...)
//	b.ResetTimer()
//	b.ReportAllocs()
//	for n := 0; n < b.N; n++ {
//		_ = GetByIDs[*loadByIDBenchmarkEntity](c, ids...)
//	}
//}