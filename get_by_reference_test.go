package beeorm

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

type getByReferenceEntity struct {
	ID        uint64 `orm:"localCache;redisCache"`
	Name      string
	Ref       *Reference[getByReferenceReference] `orm:"index=Ref"`
	RefCached *Reference[getByReferenceReference] `orm:"index=RefCached;cached"`
}

type getByReferenceReference struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string
}

func TestGetByReferenceNoCache(t *testing.T) {
	testGetByReference(t, false, false)
}

func TestGetByReferenceLocalCache(t *testing.T) {
	testGetByReference(t, true, false)
}

func TestGetByReferenceRedisCache(t *testing.T) {
	testGetByReference(t, false, true)
}

func TestGetByReferenceLocalRedisCache(t *testing.T) {
	testGetByReference(t, true, true)
}

func testGetByReference(t *testing.T, local, redis bool) {
	var entity *getByReferenceEntity
	c := PrepareTables(t, &Registry{}, entity, getByReferenceReference{})
	schema := GetEntitySchema[getByReferenceEntity](c)
	schema.DisableCache(!local, !redis)

	loggerDB := &MockLogHandler{}
	c.RegisterQueryLogger(loggerDB, true, false, false)

	var entities []*getByReferenceEntity
	ref := NewEntity[getByReferenceReference](c).TrackedEntity()
	ref.Name = "Ref 1"
	for i := 0; i < 10; i++ {
		entity = NewEntity[getByReferenceEntity](c).TrackedEntity()
		entity.Name = fmt.Sprintf("Name %d", i)
		entity.Ref = NewReference[getByReferenceReference](ref.ID)
		entity.RefCached = NewReference[getByReferenceReference](ref.ID)
		entities = append(entities, entity)
	}
	err := c.Flush(false)
	assert.NoError(t, err)

	c.EnableQueryDebug()
	loggerDB.Clear()
	rows := GetByReference[getByReferenceEntity](c, "Ref", ref.ID)
	assert.Len(t, rows, 10)
	assert.Equal(t, entities[0].ID, rows[0].ID)
	assert.Equal(t, entities[0].Name, rows[0].Name)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
}
