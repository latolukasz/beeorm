package beeorm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type loadReferenceSub struct {
	Sub1 *Reference[loadSubReferenceEntity1]
	Sub2 *Reference[loadSubReferenceEntity2]
}

type loadReferenceEntity struct {
	ID        uint64 `orm:"localCache;redisCache"`
	Name      string `orm:"required"`
	Sub       loadReferenceSub
	Ref1a     *Reference[loadSubReferenceEntity1]
	Ref1b     *Reference[loadSubReferenceEntity1]
	Ref2      *Reference[loadSubReferenceEntity2]
	Ref1Array [2]*Reference[loadSubReferenceEntity1]
}

type loadSubReferenceEntity1 struct {
	ID      uint64 `orm:"localCache;redisCache"`
	Name    string `orm:"required"`
	SubRef2 *Reference[loadSubReferenceEntity2]
}

type loadSubReferenceEntity2 struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string `orm:"required"`
}

func TestLoadReferencesLocal(t *testing.T) {
	testLoadReferences(t, true, false)
}

func testLoadReferences(t *testing.T, local, redis bool) {
	var entity *loadReferenceEntity
	var ref1 *loadSubReferenceEntity1
	var ref2 *loadSubReferenceEntity2
	orm := PrepareTables(t, NewRegistry(), entity, ref1, ref2)
	schema := GetEntitySchema[loadReferenceEntity](orm)
	schema.DisableCache(!local, !redis)
	GetEntitySchema[loadSubReferenceEntity1](orm).DisableCache(!local, !redis)
	GetEntitySchema[loadSubReferenceEntity2](orm).DisableCache(!local, !redis)

	for i := 1; i <= 10; i++ {
		entity = NewEntity[loadReferenceEntity](orm)
		entity.Name = fmt.Sprintf("Entity %d", i)
		ref1 = NewEntity[loadSubReferenceEntity1](orm)
		ref1.Name = fmt.Sprintf("Ref1 %d", i)
		entity.Ref1a = &Reference[loadSubReferenceEntity1]{ID: ref1.ID}
		entity.Ref1Array[0] = &Reference[loadSubReferenceEntity1]{ID: ref1.ID}
		sub1 := NewEntity[loadSubReferenceEntity1](orm)
		sub1.Name = fmt.Sprintf("Sub1 %d", i)
		entity.Sub.Sub1 = &Reference[loadSubReferenceEntity1]{ID: sub1.ID}
		ref2 = NewEntity[loadSubReferenceEntity2](orm)
		ref2.Name = fmt.Sprintf("Ref2 %d", i)
		entity.Ref2 = &Reference[loadSubReferenceEntity2]{ID: ref2.ID}
		if i > 5 {
			ref1.SubRef2 = &Reference[loadSubReferenceEntity2]{ID: ref2.ID}
			ref1 = NewEntity[loadSubReferenceEntity1](orm)
			ref1.Name = fmt.Sprintf("Ref1b %d", i)
			entity.Ref1b = &Reference[loadSubReferenceEntity1]{ID: ref1.ID}
			entity.Ref1Array[1] = &Reference[loadSubReferenceEntity1]{ID: ref1.ID}
			sub2 := NewEntity[loadSubReferenceEntity2](orm)
			sub2.Name = fmt.Sprintf("Sub2 %d", i)
			entity.Sub.Sub2 = &Reference[loadSubReferenceEntity2]{ID: sub2.ID}
		} else {
			sub2 := NewEntity[loadSubReferenceEntity2](orm)
			sub2.Name = fmt.Sprintf("SubSub %d", i)
			ref1.SubRef2 = &Reference[loadSubReferenceEntity2]{ID: sub2.ID}
		}
	}
	err := orm.Flush()
	assert.NoError(t, err)

	iterator := Search[loadReferenceEntity](orm, NewWhere("1"), nil)
	assert.Equal(t, 10, iterator.Len())
	iterator.LoadReference("Ref1a")
}
