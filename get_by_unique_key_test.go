package beeorm

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

type getByUniqueKeyEntity struct {
	ID     uint64                              `orm:"localCache;redisCache"`
	Name   string                              `orm:"unique=Name"`
	Age    uint8                               `orm:"unique=Multi"`
	Active bool                                `orm:"unique=Multi:2"`
	Ref    *Reference[getByUniqueKeyReference] `orm:"unique=Ref"`
}

type getByUniqueKeyReference struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string
}

func TestGetByUniqueKeyNoCache(t *testing.T) {
	testGetByUniqueKey(t, false, false)
}

func TestGetByUniqueKeyLocalCache(t *testing.T) {
	testGetByUniqueKey(t, true, false)
}

func TestGetByUniqueKeyRedisCache(t *testing.T) {
	testGetByUniqueKey(t, false, true)
}

func TestGetByUniqueKeyLocalRedisCache(t *testing.T) {
	testGetByUniqueKey(t, true, true)
}

func testGetByUniqueKey(t *testing.T, local, redis bool) {
	var entity *getByUniqueKeyEntity
	c := PrepareTables(t, &Registry{}, entity, getByUniqueKeyReference{})
	schema := GetEntitySchema[getByUniqueKeyEntity](c)
	schema.DisableCache(!local, !redis)

	var ids []uint64
	var refs []uint64
	for i := 0; i < 10; i++ {
		ref := NewEntity[getByUniqueKeyReference](c).TrackedEntity()
		ref.Name = fmt.Sprintf("Ref %d", i)
		entity = NewEntity[getByUniqueKeyEntity](c).TrackedEntity()
		entity.Name = fmt.Sprintf("Name %d", i)
		entity.Age = uint8(i)
		entity.Ref = NewReference[getByUniqueKeyReference](ref.ID)
		ids = append(ids, entity.ID)
		refs = append(refs, ref.ID)
	}
	err := c.Flush(false)
	assert.NoError(t, err)

	entity = GetByUniqueKey[getByUniqueKeyEntity](c, "Name", "Name 3")
	assert.NotNil(t, entity)
	assert.Equal(t, ids[3], entity.ID)
	assert.Equal(t, "Name 3", entity.Name)

	entity = GetByUniqueKey[getByUniqueKeyEntity](c, "Multi", 4, false)
	assert.NotNil(t, entity)
	assert.Equal(t, ids[4], entity.ID)
	assert.Equal(t, "Name 4", entity.Name)

	entity = GetByUniqueKey[getByUniqueKeyEntity](c, "Multi", 4, 0)
	assert.NotNil(t, entity)
	assert.Equal(t, ids[4], entity.ID)
	assert.Equal(t, "Name 4", entity.Name)

	entity = GetByUniqueKey[getByUniqueKeyEntity](c, "Ref", refs[4])
	assert.NotNil(t, entity)
	assert.Equal(t, ids[4], entity.ID)
	assert.Equal(t, "Name 4", entity.Name)
}
