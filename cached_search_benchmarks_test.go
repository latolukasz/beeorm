package beeorm

import (
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

// BenchmarkCachedSearchLocal-10    	 2172336	       557.8 ns/op	     176 B/op	       7 allocs/op
func BenchmarkCachedSearchLocal(b *testing.B) {
	benchmarkCachedSearch(b, true, false)
}

func BenchmarkCachedSearchRedis(b *testing.B) {
	benchmarkCachedSearch(b, false, true)
}

func benchmarkCachedSearch(b *testing.B, localCache bool, redisCache bool) {
	var entity *cachedSearchEntity
	var entityRef *cachedSearchRefEntity
	c := PrepareTables(nil, &Registry{}, 5, 6, "", entityRef, entity)
	schema := c.Engine().Registry().EntitySchema(entity)
	schema.DisableCache(!localCache, !redisCache)

	for i := 1; i <= 10; i++ {
		e := &cachedSearchEntity{Name: "Name " + strconv.Itoa(i), Age: uint16(10)}
		c.Flusher().Track(e)
	}
	c.Flusher().Flush()

	var rows []*cachedSearchEntity
	totalRows := CachedSearch(c, &rows, "IndexAge", nil, 10)
	assert.Equal(b, totalRows, totalRows)

	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		CachedSearch(c, &rows, "IndexAge", nil, 10)
	}
}
