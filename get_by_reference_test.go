package beeorm

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

type getByReferenceEntity struct {
	ID               uint64 `orm:"localCache;redisCache"`
	Name             string
	Ref              *Reference[getByReferenceReference]        `orm:"index=Ref"`
	RefCached        *Reference[getByReferenceReference]        `orm:"index=RefCached;cached"`
	RefCachedNoCache *Reference[getByReferenceReferenceNoCache] `orm:"index=RefCachedNoCache;cached"`
}

type getByReferenceReference struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string
}

type getByReferenceReferenceNoCache struct {
	ID   uint64
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
	c := PrepareTables(t, &Registry{}, entity, getByReferenceReference{}, getByReferenceReferenceNoCache{})
	schema := GetEntitySchema[getByReferenceEntity](c)
	schema.DisableCache(!local, !redis)

	loggerDB := &MockLogHandler{}
	c.RegisterQueryLogger(loggerDB, true, false, false)

	var entities []*getByReferenceEntity
	ref := NewEntity[getByReferenceReference](c).TrackedEntity()
	ref.Name = "Ref 1"
	refNoCache := NewEntity[getByReferenceReferenceNoCache](c).TrackedEntity()
	refNoCache.Name = "Ref 1"
	for i := 0; i < 10; i++ {
		entity = NewEntity[getByReferenceEntity](c).TrackedEntity()
		entity.Name = fmt.Sprintf("Name %d", i)
		entity.Ref = NewReference[getByReferenceReference](ref.ID)
		entity.RefCached = NewReference[getByReferenceReference](ref.ID)
		entity.RefCachedNoCache = NewReference[getByReferenceReferenceNoCache](refNoCache.ID)
		entities = append(entities, entity)
	}
	err := c.Flush(false)
	assert.NoError(t, err)

	loggerDB.Clear()
	rows := GetByReference[getByReferenceEntity](c, "Ref", ref.ID)
	assert.Len(t, rows, 10)
	assert.Equal(t, entities[0].ID, rows[0].ID)
	assert.Equal(t, entities[0].Name, rows[0].Name)
	assert.Len(t, loggerDB.Logs, 1)

	loggerDB.Clear()
	rows = GetByReference[getByReferenceEntity](c, "RefCached", ref.ID)
	assert.Len(t, rows, 10)
	assert.Equal(t, entities[0].ID, rows[0].ID)
	assert.Equal(t, entities[0].Name, rows[0].Name)
	assert.Len(t, loggerDB.Logs, 1)
	loggerDB.Clear()
	rows = GetByReference[getByReferenceEntity](c, "RefCached", ref.ID)
	assert.Len(t, rows, 10)
	assert.Equal(t, entities[0].ID, rows[0].ID)
	assert.Equal(t, entities[0].Name, rows[0].Name)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
	loggerDB.Clear()

	rows2 := GetByReference[getByReferenceEntity](c, "RefCachedNoCache", ref.ID)
	assert.Len(t, rows2, 10)
	assert.Equal(t, entities[0].ID, rows2[0].ID)
	assert.Equal(t, entities[0].Name, rows2[0].Name)
	assert.Len(t, loggerDB.Logs, 1)
	loggerDB.Clear()
	rows2 = GetByReference[getByReferenceEntity](c, "RefCachedNoCache", ref.ID)
	assert.Len(t, rows2, 10)
	assert.Equal(t, entities[0].ID, rows2[0].ID)
	assert.Equal(t, entities[0].Name, rows2[0].Name)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
}
