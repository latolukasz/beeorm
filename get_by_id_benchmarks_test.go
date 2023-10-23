package beeorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type getByIDBenchmarkEntity struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string
}

// BenchmarkGetByIDLocalCache-10    	66659722	        17.62 ns/op	       0 B/op	       0 allocs/op
func BenchmarkGetByIDLocalCache(b *testing.B) {
	benchmarkGetByIDCache(b, true, false)
}

// BenchmarkGetByIDRedisCache-10    	    1966	    590329 ns/op	     272 B/op	      10 allocs/op
func BenchmarkGetByIDRedisCache(b *testing.B) {
	benchmarkGetByIDCache(b, false, true)
}

func benchmarkGetByIDCache(b *testing.B, local, redis bool) {
	var entity *getByIDBenchmarkEntity
	registry := &Registry{}
	registry.RegisterLocalCache(DefaultPoolCode)
	c := PrepareTables(nil, registry, entity)
	schema := GetEntitySchema[getByIDBenchmarkEntity](c)
	schema.DisableCache(!local, !redis)

	entity = NewEntity[getByIDBenchmarkEntity](c).TrackedEntity()
	entity.Name = "Name"
	err := c.Flush(false)
	assert.NoError(b, err)

	GetByID[getByIDBenchmarkEntity](c, entity.ID)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		GetByID[getByIDBenchmarkEntity](c, entity.ID)
	}
}
