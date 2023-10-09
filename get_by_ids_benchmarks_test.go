package beeorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// BenchmarkLoadByIDsLocalCache-10    	 1565151	       763.9 ns/op	     896 B/op	       1 allocs/op
func BenchmarkLoadByIDsLocalCache(b *testing.B) {
	benchmarkLoadByIDsCache(b, true, false)
}

// BenchmarkLoadByIDsRedisCache-10    	     301	   5092551 ns/op	   42032 B/op	    1118 allocs/op
func BenchmarkLoadByIDsRedisCache(b *testing.B) {
	benchmarkLoadByIDsCache(b, false, true)
}

func benchmarkLoadByIDsCache(b *testing.B, local, redis bool) {
	entity := &getByIdsEntity{}
	registry := &Registry{}
	registry.RegisterLocalCache(10000)
	c := PrepareTables(nil, registry, entity)
	schema := GetEntitySchema[getByIdsEntity](c)
	schema.DisableCache(!local, !redis)

	const size = 100
	ids := make([]uint64, size)
	for i := 0; i < size; i++ {
		entity = NewEntity[getByIdsEntity](c).TrackedEntity()
		entity.Name = "Name"
		ids[i] = entity.ID
	}
	err := c.Flush(false)
	assert.NoError(b, err)
	_ = GetByIDs[getByIdsEntity](c, ids...)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		_ = GetByIDs[getByIdsEntity](c, ids...)
	}
}
