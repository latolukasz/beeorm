package beeorm

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

type getByUniqueKeyEntity struct {
	ID     uint64 `orm:"localCache;redisCache"`
	Name   string `orm:"unique=Name"`
	Age    uint8  `orm:"unique=Multi"`
	Active uint8  `orm:"unique=Multi:2"`
	Ref    *Reference[getByUniqueKeyReference]
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

	ref := NewEntity[getByUniqueKeyReference](c).TrackedEntity()
	ref.Name = "Test Reference"

	var ids []uint64
	for i := 0; i < 10; i++ {
		entity = NewEntity[getByUniqueKeyEntity](c).TrackedEntity()
		entity.Name = fmt.Sprintf("Name %d", i)
		entity.Age = uint8(i)
		entity.Ref = NewReference[getByUniqueKeyReference](ref.ID)
		ids = append(ids, entity.ID)
	}
	err := c.Flush(false)
	assert.NoError(t, err)

	entity = GetByUniqueKey[getByUniqueKeyEntity](c, "Name", "Name 3")
	assert.NotNil(t, entity)
	assert.Equal(t, ids[3], entity.ID)
	assert.Equal(t, "Name 3", entity.Name)
}
