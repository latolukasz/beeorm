package beeorm

//
//import (
//	"context"
//	"testing"
//
//	"github.com/stretchr/testify/assert"
//)
//
//// BenchmarkLocalCacheGet-10    	76928248	        15.49 ns/op	       0 B/op	       0 allocs/op
//func BenchmarkLocalCacheGet(b *testing.B) {
//	registry := &Registry{}
//	registry.RegisterLocalCache(100)
//	engine, err := registry.Validate()
//	assert.Nil(b, err)
//	c := engine.NewContext(context.Background())
//	lc := c.Engine().LocalCache(DefaultPoolCode)
//	lc.Set(c, "1", "test")
//
//	b.ResetTimer()
//	b.ReportAllocs()
//	for n := 0; n < b.N; n++ {
//		lc.Get(c, "1")
//	}
//}
//
//// BenchmarkLocalCacheGet3-10    	292129987	         3.949 ns/op	       0 B/op	       0 allocs/op
//func BenchmarkLocalCacheGetEntity(b *testing.B) {
//	var entity *loadByIDBenchmarkEntity
//	registry := &Registry{}
//	registry.RegisterLocalCache(10000)
//	c := PrepareTables(nil, registry, 5, 6, "", entity)
//	schema := GetEntitySchema[*loadByIDBenchmarkEntity](c)
//	lc, _ := schema.GetLocalCache()
//	lc.setEntity(c, 1, emptyReflect)
//
//	b.ResetTimer()
//	b.ReportAllocs()
//	for n := 0; n < b.N; n++ {
//		lc.getEntity(c, 1)
//	}
//}
