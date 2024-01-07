package beeorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// BenchmarkGetByIDsLocalCache-10    	 1000000	      1135 ns/op	      80 B/op	       1 allocs/op
func BenchmarkGetByIDsLocalCache(b *testing.B) {
	benchmarkGetByIDsCache(b, true, false)
}

// BenchmarkGetByIDsRedisCache-10    	     301	   5092551 ns/op	   42032 B/op	    1118 allocs/op
func BenchmarkGetByIDsRedisCache(b *testing.B) {
	benchmarkGetByIDsCache(b, false, true)
}

func benchmarkGetByIDsCache(b *testing.B, local, redis bool) {
	entity := &getByIdsEntity{}
	registry := NewRegistry()
	registry.RegisterLocalCache(DefaultPoolCode, 0)
	orm := PrepareTables(nil, registry, entity)
	schema := GetEntitySchema[getByIdsEntity](orm)
	schema.DisableCache(!local, !redis)

	const size = 100
	ids := make([]uint64, size)
	for i := 0; i < size; i++ {
		entity = NewEntity[getByIdsEntity](orm)
		entity.Name = "Name"
		ids[i] = entity.ID
	}
	err := orm.Flush()
	assert.NoError(b, err)
	_ = GetByIDs[getByIdsEntity](orm, ids...)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		rows := GetByIDs[getByIdsEntity](orm, ids...)
		for rows.Next() {
			rows.Entity()
		}
	}
}
