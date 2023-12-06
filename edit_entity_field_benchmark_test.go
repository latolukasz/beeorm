package beeorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type editByFieldAsyncBenchmarkEntity struct {
	ID  uint64 `orm:"localCache"`
	Age int
}

// BenchmarkEditByFieldAsync-10    	 2966167	       406.7 ns/op	     536 B/op	       9 allocs/op
func BenchmarkEditByFieldAsync(b *testing.B) {
	var entity *editByFieldAsyncBenchmarkEntity
	c := PrepareTables(nil, NewRegistry(), entity)
	entity = NewEntity[editByFieldAsyncBenchmarkEntity](c)
	entity.Age = 0
	assert.NoError(b, c.Flush())

	GetByID[editByFieldAsyncBenchmarkEntity](c, entity.ID)
	field := "Age"

	schema := getEntitySchema[editByFieldAsyncBenchmarkEntity](c)

	go func() {
		for {
			e := schema.asyncTemporaryQueue.Dequeue()
			if e == nil {
				return
			}
		}
	}()

	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		_ = EditEntityFieldAsync(c, entity, field, n, true)
	}
	publishAsyncEvent(schema, nil)
}

// BenchmarkEditByFieldAsyncWithRedis-10    	  908324	      1304 ns/op	     688 B/op	      13 allocs/op
func BenchmarkEditByFieldAsyncWithRedis(b *testing.B) {
	var entity *editByFieldAsyncBenchmarkEntity
	c := PrepareTables(nil, NewRegistry(), entity)
	entity = NewEntity[editByFieldAsyncBenchmarkEntity](c)
	entity.Age = 0
	assert.NoError(b, c.Flush())

	GetByID[editByFieldAsyncBenchmarkEntity](c, entity.ID)
	field := "Age"

	stop := ConsumeAsyncFlushTemporaryEvents(c, func(err error) {
		panic(err)
	})

	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		_ = EditEntityFieldAsync(c, entity, field, n, true)
	}
	stop()
}
