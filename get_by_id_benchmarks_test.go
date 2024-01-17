package beeorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type getByIDBenchmarkEntity struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string
}

type getByIDBenchmarkEntityLimit struct {
	ID   uint64 `orm:"localCache=10"`
	Name string
}

// BenchmarkGetByIDLocalCache-10    	62160598	        19.32 ns/op	       0 B/op	       0 allocs/op
func BenchmarkGetByIDLocalCacheNoLimit(b *testing.B) {
	benchmarkGetByIDCache(b, true, false)
}

// BenchmarkGetByIDLocalCacheLimit-10    	42071001	        28.35 ns/op	       0 B/op	       0 allocs/op
func BenchmarkGetByIDLocalCacheLimit(b *testing.B) {
	benchmarkGetByIDLocalCacheLimit(b)
}

// BenchmarkGetByIDRedisCache-10    	    1966	    590329 ns/op	     272 B/op	      10 allocs/op
func BenchmarkGetByIDRedisCacheNoLimit(b *testing.B) {
	benchmarkGetByIDCache(b, false, true)
}

func benchmarkGetByIDCache(b *testing.B, local, redis bool) {
	var entity *getByIDBenchmarkEntity
	registry := NewRegistry()
	orm := PrepareTables(nil, registry, entity)
	schema := GetEntitySchema[getByIDBenchmarkEntity](orm)
	schema.DisableCache(!local, !redis)

	entity = NewEntity[getByIDBenchmarkEntity](orm)
	entity.Name = "Name"
	err := orm.Flush()
	assert.NoError(b, err)

	GetByID[getByIDBenchmarkEntity](orm, entity.ID)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		GetByID[getByIDBenchmarkEntity](orm, entity.ID)
	}
}

func benchmarkGetByIDLocalCacheLimit(b *testing.B) {
	var entity *getByIDBenchmarkEntityLimit
	registry := NewRegistry()
	orm := PrepareTables(nil, registry, entity)

	entity = NewEntity[getByIDBenchmarkEntityLimit](orm)
	entity.Name = "Name"
	err := orm.Flush()
	assert.NoError(b, err)

	GetByID[getByIDBenchmarkEntityLimit](orm, entity.ID)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		GetByID[getByIDBenchmarkEntityLimit](orm, entity.ID)
	}
}
