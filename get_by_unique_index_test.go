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
	orm := PrepareTables(t, NewRegistry(), entity, getByUniqueIndexReference{})
	schema := GetEntitySchema[getByUniqueIndexEntity](orm)
	schema.DisableCache(!local, !redis)

	var entities []*getByUniqueIndexEntity
	var refs []*getByUniqueIndexReference
	date := time.Now().UTC()
	died := time.Now().UTC()
	for i := 0; i < 10; i++ {
		ref := NewEntity[getByUniqueIndexReference](orm)
		ref.Name = fmt.Sprintf("Ref %d", i)
		entity = NewEntity[getByUniqueIndexEntity](orm)
		entity.Name = fmt.Sprintf("Name %d", i)
		entity.Age = uint8(i)
		entity.Ref = &Reference[getByUniqueIndexReference]{ID: ref.ID}
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
	err := orm.Flush()
	assert.NoError(t, err)

	entity = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Name", "Name 3")
	assert.NotNil(t, entity)
	assert.Equal(t, entities[3].ID, entity.ID)
	assert.Equal(t, "Name 3", entity.Name)

	entity = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Name", "Missing")
	assert.Nil(t, entity)

	assert.PanicsWithError(t, "[Name] invalid value", func() {
		entity = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Name", time.Now())
	})

	entity = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Multi", 4, false)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[4].ID, entity.ID)
	assert.Equal(t, "Name 4", entity.Name)

	entity = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Multi", 4, 0)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[4].ID, entity.ID)
	assert.Equal(t, "Name 4", entity.Name)

	numbers := []any{uint8(4), uint16(4), uint32(4), uint(4), "4", int8(4), int16(4), int32(4), int64(4)}
	for _, number := range numbers {
		entity = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Multi", number, 0)
		assert.Equal(t, "Name 4", entity.Name)
	}

	negativeNumbers := []any{int8(-4), int16(-4), int32(-4), -4, int8(-4), int16(-4), int32(-4), int64(-4)}
	for _, number := range negativeNumbers {
		assert.PanicsWithError(t, "[Age] negative number -4 not allowed", func() {
			entity = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Multi", number, 0)
		})
	}

	assert.PanicsWithError(t, "[Age] invalid number invalid", func() {
		entity = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Multi", "invalid", 0)
	})

	assert.PanicsWithError(t, "[Age] invalid value", func() {
		entity = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Multi", time.Now(), 0)
	})

	entity = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Ref", refs[4].ID)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[4].ID, entity.ID)
	assert.Equal(t, "Name 4", entities[4].Name)

	date = date.Add(time.Hour * -3)
	entity = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Time", date)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[6].ID, entity.ID)
	assert.Equal(t, "Name 6", entities[6].Name)

	died = died.Add(time.Hour * -72)
	entity = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Died", true, died)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[6].ID, entity.ID)
	assert.Equal(t, "Name 6", entities[6].Name)

	entity = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Died", "true", died)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[6].ID, entity.ID)
	assert.Equal(t, "Name 6", entities[6].Name)

	entity = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Died", 1, died)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[6].ID, entity.ID)
	assert.Equal(t, "Name 6", entities[6].Name)

	died = died.Add(time.Hour * -72)
	entity = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Died", "false", died)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[3].ID, entity.ID)
	assert.Equal(t, "Name 3", entities[3].Name)

	assert.PanicsWithError(t, "invalid number of index `Name` attributes, got 2, 1 expected", func() {
		GetByUniqueIndex[getByUniqueIndexEntity](orm, "Name", "a", "b")
	})

	assert.PanicsWithError(t, "unknown index name `Invalid`", func() {
		GetByUniqueIndex[getByUniqueIndexEntity](orm, "Invalid")
	})

	assert.PanicsWithError(t, "nil attribute for index name `Name` is not allowed", func() {
		GetByUniqueIndex[getByUniqueIndexEntity](orm, "Name", nil)
	})

	assert.PanicsWithError(t, "entity 'time.Time' is not registered", func() {
		GetByUniqueIndex[time.Time](orm, "Name", nil)
	})

	assert.PanicsWithError(t, "[BirthDate] invalid value", func() {
		GetByUniqueIndex[getByUniqueIndexEntity](orm, "Time", 23)
	})

	assert.PanicsWithError(t, "[Died] invalid value", func() {
		GetByUniqueIndex[getByUniqueIndexEntity](orm, "Died", time.Now(), died)
	})
}
