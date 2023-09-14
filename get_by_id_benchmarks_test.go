package beeorm

//
//import (
//	"testing"
//)
//
//type getByIDBenchmarkEntity struct {
//	ORM     `orm:"localCache;redisCache"`
//	ID      uint
//	Name    string
//	Int     int
//	Bool    bool
//	Float   float64
//	Decimal float32 `orm:"decimal=10,2"`
//}
//
//// BenchmarkGetByIDLocalCache-10    	63629875	        18.75 ns/op	       0 B/op	       0 allocs/op
//func BenchmarkGetByIDLocalCache(b *testing.B) {
//	benchmarkGetByIDCache(b, true, false)
//}
//
//// BenchmarkGetByIDRedisCache-10    	    1966	    590329 ns/op	     272 B/op	      10 allocs/op
//func BenchmarkGetByIDRedisCache(b *testing.B) {
//	benchmarkGetByIDCache(b, false, true)
//}
//
//func benchmarkGetByIDCache(b *testing.B, local, redis bool) {
//	var entity *getByIDBenchmarkEntity
//	registry := &Registry{}
//	registry.RegisterLocalCache(10000)
//	c := PrepareTables(nil, registry, 5, 6, "", entity)
//	schema := GetEntitySchema[*loadByIDBenchmarkEntity](c)
//	schema.DisableCache(!local, !redis)
//	entity = &loadByIDBenchmarkEntity{}
//	entity.Name = "Name"
//	entity.Int = 1
//	entity.Float = 1.3
//	entity.Decimal = 12.23
//	c.Flusher().Track(entity).Flush()
//	GetByID[*loadByIDBenchmarkEntity](c, 1)
//	b.ResetTimer()
//	b.ReportAllocs()
//	for n := 0; n < b.N; n++ {
//		GetByID[*loadByIDBenchmarkEntity](c, 1)
//	}
//}