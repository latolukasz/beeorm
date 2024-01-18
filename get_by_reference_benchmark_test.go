package beeorm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// BenchmarkGetByReferenceLocalCache-10    	35836004	        29.10 ns/op	       0 B/op	       0 allocs/op
func BenchmarkGetByReferenceLocalCache(b *testing.B) {
	benchmarkGetByReference(b, true, false)
}

func benchmarkGetByReference(b *testing.B, local, redis bool) {
	var entity *getByReferenceEntity
	orm := PrepareTables(nil, NewRegistry(), entity, getByReferenceReference{})
	schema := GetEntitySchema[getByReferenceEntity](orm)
	schema.DisableCache(!local, !redis)

	var entities []*getByReferenceEntity
	ref := NewEntity[getByReferenceReference](orm)
	ref.Name = "Ref 1"
	for i := 0; i < 10; i++ {
		entity = NewEntity[getByReferenceEntity](orm)
		entity.Name = fmt.Sprintf("Name %d", i)
		entity.RefCached = Reference[getByReferenceReference](ref.ID)
		entities = append(entities, entity)
	}
	err := orm.Flush()
	assert.NoError(b, err)

	rows := GetByReference[getByReferenceEntity](orm, "RefCached", ref.ID)
	assert.Len(b, rows, 10)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		GetByReference[getByReferenceEntity](orm, "RefCached", ref.ID)
	}
}
