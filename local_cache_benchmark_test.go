package beeorm

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// BenchmarkLocalCacheGet-10    	66464670	        17.87 ns/op	       0 B/op	       0 allocs/op
func BenchmarkLocalCacheGet(b *testing.B) {
	registry := &Registry{}
	registry.RegisterLocalCache(100)
	engine, err := registry.Validate()
	assert.Nil(b, err)
	c := engine.NewContext(context.Background())
	lc := c.Engine().LocalCache(DefaultPoolCode)
	lc.Set(c, 1, "test")

	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		lc.Get(c, 1)
	}
}
