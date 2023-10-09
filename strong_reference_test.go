package beeorm

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

type strongReferenceEntity struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string
	Ref  *Reference[strongReferenceReference] `orm:"strong;index=Ref"`
}

type strongReferenceReference struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string
}

func TestStrongReference(t *testing.T) {
	var entity *strongReferenceEntity
	var reference *strongReferenceReference
	c := PrepareTables(t, &Registry{}, entity, reference)

	reference = NewEntity[strongReferenceReference](c).TrackedEntity()
	reference.Name = "Test reference"
	for i := 0; i < 10; i++ {
		entity = NewEntity[strongReferenceEntity](c).TrackedEntity()
		entity.Name = fmt.Sprintf("Name %d", i)
		entity.Ref = NewReference[strongReferenceReference](reference.ID)
	}
	err := c.Flush(false)
	assert.NoError(t, err)

	DeleteEntity(c, reference)
	err = c.Flush(false)
	assert.NoError(t, err)

	entities := GetByReference[strongReferenceEntity](c, "Ref", reference.ID)
	assert.Len(t, entities, 0)

	reference = GetByID[strongReferenceReference](c, entity.ID)
	assert.Nil(t, reference)
}
