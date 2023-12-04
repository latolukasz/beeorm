package beeorm

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type editByFieldAsyncBenchmarkEntity struct {
	ID  uint64 `orm:"localCache"`
	Age int
}

// BenchmarkEditByFieldAsync-10    	    6416	    170330 ns/op	     663 B/op	      23 allocs/op
func BenchmarkEditByFieldAsync(b *testing.B) {
	var entity *editByFieldAsyncBenchmarkEntity
	c := PrepareTables(nil, NewRegistry(), entity)
	entity = NewEntity[editByFieldAsyncBenchmarkEntity](c)
	entity.Age = 0
	assert.NoError(b, c.Flush())

	GetByID[editByFieldAsyncBenchmarkEntity](c, entity.ID)
	field := "Age"
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		_ = EditEntityFieldAsync(c, entity, field, n, true)
	}
}
