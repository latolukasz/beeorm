package beeorm

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

// BenchmarkGetByReferenceLocalCache-10    	35836004	        29.10 ns/op	       0 B/op	       0 allocs/op
func BenchmarkGetByReferenceLocalCache(b *testing.B) {
	benchmarkGetByReference(b, true, false)
}

func benchmarkGetByReference(b *testing.B, local, redis bool) {
	var entity *getByReferenceEntity
	c := PrepareTables(nil, &Registry{}, entity, getByReferenceReference{})
	schema := GetEntitySchema[getByReferenceEntity](c)
	schema.DisableCache(!local, !redis)

	var entities []*getByReferenceEntity
	ref := NewEntity[getByReferenceReference](c).TrackedEntity()
	ref.Name = "Ref 1"
	for i := 0; i < 10; i++ {
		entity = NewEntity[getByReferenceEntity](c).TrackedEntity()
		entity.Name = fmt.Sprintf("Name %d", i)
		entity.RefCached = NewReference[getByReferenceReference](ref.ID)
		entities = append(entities, entity)
	}
	err := c.Flush(false)
	assert.NoError(b, err)

	rows := GetByReference[getByReferenceEntity](c, "RefCached", ref.ID)
	assert.Len(b, rows, 10)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		GetByReference[getByReferenceEntity](c, "RefCached", ref.ID)
	}
}
