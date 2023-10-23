package beeorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// BenchmarkGetByIDsLocalCache-10    	 1565151	       763.9 ns/op	     896 B/op	       1 allocs/op
// BenchmarkGetByIDsLocalCache-10    	 1420578	       835.7 ns/op	      48 B/op	       1 allocs/op
func BenchmarkGetByIDsLocalCache(b *testing.B) {
	benchmarkGetByIDsCache(b, true, false)
}

// BenchmarkGetByIDsRedisCache-10    	     301	   5092551 ns/op	   42032 B/op	    1118 allocs/op
func BenchmarkGetByIDsRedisCache(b *testing.B) {
	benchmarkGetByIDsCache(b, false, true)
}

func benchmarkGetByIDsCache(b *testing.B, local, redis bool) {
	entity := &getByIdsEntity{}
	registry := &Registry{}
	registry.RegisterLocalCache(DefaultPoolCode)
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
		rows := GetByIDs[getByIdsEntity](c, ids...)
		for rows.Next() {
			rows.Entity()
		}
	}
}
