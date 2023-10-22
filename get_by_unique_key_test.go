package beeorm

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type getByUniqueKeyEntity struct {
	ID        uint64                              `orm:"localCache;redisCache"`
	Name      string                              `orm:"unique=Name"`
	Age       uint8                               `orm:"unique=Multi"`
	Active    bool                                `orm:"unique=Multi:2"`
	Ref       *Reference[getByUniqueKeyReference] `orm:"unique=Ref"`
	BirthDate time.Time                           `orm:"time;unique=Time"`
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

	var entities []*getByUniqueKeyEntity
	var refs []*getByUniqueKeyReference
	date := time.Now().UTC()
	for i := 0; i < 10; i++ {
		ref := NewEntity[getByUniqueKeyReference](c).TrackedEntity()
		ref.Name = fmt.Sprintf("Ref %d", i)
		entity = NewEntity[getByUniqueKeyEntity](c).TrackedEntity()
		entity.Name = fmt.Sprintf("Name %d", i)
		entity.Age = uint8(i)
		entity.Ref = NewReference[getByUniqueKeyReference](ref.ID)
		date = date.Add(time.Hour)
		entity.BirthDate = date
		entities = append(entities, entity)
		refs = append(refs, ref)
	}
	err := c.Flush(false)
	assert.NoError(t, err)

	entity = GetByUniqueKey[getByUniqueKeyEntity](c, "Name", "Name 3")
	assert.NotNil(t, entity)
	assert.Equal(t, entities[3].ID, entity.ID)
	assert.Equal(t, "Name 3", entity.Name)

	entity = GetByUniqueKey[getByUniqueKeyEntity](c, "Name", "Missing")
	assert.Nil(t, entity)

	entity = GetByUniqueKey[getByUniqueKeyEntity](c, "Multi", 4, false)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[4].ID, entity.ID)
	assert.Equal(t, "Name 4", entity.Name)

	entity = GetByUniqueKey[getByUniqueKeyEntity](c, "Multi", 4, 0)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[4].ID, entity.ID)
	assert.Equal(t, "Name 4", entity.Name)

	entity = GetByUniqueKey[getByUniqueKeyEntity](c, "Ref", refs[4].ID)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[4].ID, entity.ID)
	assert.Equal(t, "Name 4", entities[4].Name)

	date = date.Add(time.Hour * -3)
	entity = GetByUniqueKey[getByUniqueKeyEntity](c, "Time", date)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[6].ID, entity.ID)
	assert.Equal(t, "Name 6", entities[6].Name)

	assert.PanicsWithError(t, "invalid number of index `Name` attributes, got 2, 1 expected", func() {
		GetByUniqueKey[getByUniqueKeyEntity](c, "Name", "a", "b")
	})

	assert.PanicsWithError(t, "unknown index name `Invalid`", func() {
		GetByUniqueKey[getByUniqueKeyEntity](c, "Invalid")
	})

	assert.PanicsWithError(t, "nil attribute for index name `Name` is not allowed", func() {
		GetByUniqueKey[getByUniqueKeyEntity](c, "Name", nil)
	})

	assert.PanicsWithError(t, "entity 'time.Time' is not registered", func() {
		GetByUniqueKey[time.Time](c, "Name", nil)
	})
}
