package beeorm

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type getByUniqueIndexEntity struct {
	ID        uint64                                `orm:"localCache;redisCache"`
	Name      string                                `orm:"unique=Name"`
	Age       uint8                                 `orm:"unique=Multi"`
	Active    bool                                  `orm:"unique=Multi:2"`
	Ref       *Reference[getByUniqueIndexReference] `orm:"unique=Ref"`
	BirthDate time.Time                             `orm:"time;unique=Time"`
	Died      bool                                  `orm:"time;unique=Died"`
	DeathDate time.Time                             `orm:"unique=Died:2"`
	Price     float32                               `orm:"unique=Price"`
}

type getByUniqueIndexReference struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string
}

func TestGetByUniqueIndexNoCache(t *testing.T) {
	testGetByUniqueIndex(t, false, false)
}

func TestGetByUniqueIndexLocalCache(t *testing.T) {
	testGetByUniqueIndex(t, true, false)
}

func TestGetByUniqueIndexRedisCache(t *testing.T) {
	testGetByUniqueIndex(t, false, true)
}

func TestGetByUniqueIndexLocalRedisCache(t *testing.T) {
	testGetByUniqueIndex(t, true, true)
}

func testGetByUniqueIndex(t *testing.T, local, redis bool) {
	var entity *getByUniqueIndexEntity
	c := PrepareTables(t, NewRegistry(), entity, getByUniqueIndexReference{})
	schema := GetEntitySchema[getByUniqueIndexEntity](c)
	schema.DisableCache(!local, !redis)

	var entities []*getByUniqueIndexEntity
	var refs []*getByUniqueIndexReference
	date := time.Now().UTC()
	died := time.Now().UTC()
	for i := 0; i < 10; i++ {
		ref := NewEntity[getByUniqueIndexReference](c).TrackedEntity()
		ref.Name = fmt.Sprintf("Ref %d", i)
		entity = NewEntity[getByUniqueIndexEntity](c).TrackedEntity()
		entity.Name = fmt.Sprintf("Name %d", i)
		entity.Age = uint8(i)
		entity.Ref = NewReference[getByUniqueIndexReference](ref.ID)
		date = date.Add(time.Hour)
		died = died.Add(time.Hour * 24)
		entity.BirthDate = date
		entity.DeathDate = died
		if i > 5 {
			entity.Died = true
		}
		entity.Price = float32(i)
		entities = append(entities, entity)
		refs = append(refs, ref)
	}
	err := c.Flush(false)
	assert.NoError(t, err)

	entity = GetByUniqueIndex[getByUniqueIndexEntity](c, "Name", "Name 3")
	assert.NotNil(t, entity)
	assert.Equal(t, entities[3].ID, entity.ID)
	assert.Equal(t, "Name 3", entity.Name)

	entity = GetByUniqueIndex[getByUniqueIndexEntity](c, "Name", "Missing")
	assert.Nil(t, entity)

	assert.PanicsWithError(t, "invalid value `time.Time` for column `Name`", func() {
		entity = GetByUniqueIndex[getByUniqueIndexEntity](c, "Name", time.Now())
	})

	entity = GetByUniqueIndex[getByUniqueIndexEntity](c, "Multi", 4, false)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[4].ID, entity.ID)
	assert.Equal(t, "Name 4", entity.Name)

	entity = GetByUniqueIndex[getByUniqueIndexEntity](c, "Multi", 4, 0)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[4].ID, entity.ID)
	assert.Equal(t, "Name 4", entity.Name)

	numbers := []interface{}{uint8(4), uint16(4), uint32(4), uint(4), "4", int8(4), int16(4), int32(4), int64(4)}
	for _, number := range numbers {
		entity = GetByUniqueIndex[getByUniqueIndexEntity](c, "Multi", number, 0)
		assert.Equal(t, "Name 4", entity.Name)
	}

	negativeNumbers := []interface{}{int8(-4), int16(-4), int32(-4), int(-4), int8(-4), int16(-4), int32(-4), int64(-4)}
	for _, number := range negativeNumbers {
		assert.PanicsWithError(t, "unsigned number for column `Age` not allowed", func() {
			entity = GetByUniqueIndex[getByUniqueIndexEntity](c, "Multi", number, 0)
		})
	}

	assert.PanicsWithError(t, "invalid number for column `Age`", func() {
		entity = GetByUniqueIndex[getByUniqueIndexEntity](c, "Multi", "invalid", 0)
	})

	assert.PanicsWithError(t, "invalid value `time.Time` for column `Age`", func() {
		entity = GetByUniqueIndex[getByUniqueIndexEntity](c, "Multi", time.Now(), 0)
	})

	assert.PanicsWithError(t, "type float32 is not supported, column `Price`", func() {
		entity = GetByUniqueIndex[getByUniqueIndexEntity](c, "Price", float32(123))
	})

	entity = GetByUniqueIndex[getByUniqueIndexEntity](c, "Ref", refs[4].ID)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[4].ID, entity.ID)
	assert.Equal(t, "Name 4", entities[4].Name)

	date = date.Add(time.Hour * -3)
	entity = GetByUniqueIndex[getByUniqueIndexEntity](c, "Time", date)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[6].ID, entity.ID)
	assert.Equal(t, "Name 6", entities[6].Name)

	died = died.Add(time.Hour * -72)
	entity = GetByUniqueIndex[getByUniqueIndexEntity](c, "Died", true, died)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[6].ID, entity.ID)
	assert.Equal(t, "Name 6", entities[6].Name)

	entity = GetByUniqueIndex[getByUniqueIndexEntity](c, "Died", "true", died)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[6].ID, entity.ID)
	assert.Equal(t, "Name 6", entities[6].Name)

	entity = GetByUniqueIndex[getByUniqueIndexEntity](c, "Died", 1, died)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[6].ID, entity.ID)
	assert.Equal(t, "Name 6", entities[6].Name)

	died = died.Add(time.Hour * -72)
	entity = GetByUniqueIndex[getByUniqueIndexEntity](c, "Died", "false", died)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[3].ID, entity.ID)
	assert.Equal(t, "Name 3", entities[3].Name)

	assert.PanicsWithError(t, "invalid number of index `Name` attributes, got 2, 1 expected", func() {
		GetByUniqueIndex[getByUniqueIndexEntity](c, "Name", "a", "b")
	})

	assert.PanicsWithError(t, "unknown index name `Invalid`", func() {
		GetByUniqueIndex[getByUniqueIndexEntity](c, "Invalid")
	})

	assert.PanicsWithError(t, "nil attribute for index name `Name` is not allowed", func() {
		GetByUniqueIndex[getByUniqueIndexEntity](c, "Name", nil)
	})

	assert.PanicsWithError(t, "entity 'time.Time' is not registered", func() {
		GetByUniqueIndex[time.Time](c, "Name", nil)
	})

	assert.PanicsWithError(t, "type int not supported, column `BirthDate`", func() {
		GetByUniqueIndex[getByUniqueIndexEntity](c, "Time", 23)
	})

	assert.PanicsWithError(t, "invalid value `time.Time` for column `Died`", func() {
		GetByUniqueIndex[getByUniqueIndexEntity](c, "Died", time.Now(), died)
	})
}
