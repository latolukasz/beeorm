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
	ref2 := NewEntity[getByReferenceReference](c).TrackedEntity()
	ref2.Name = "Ref 2"
	refNoCache := NewEntity[getByReferenceReferenceNoCache](c).TrackedEntity()
	refNoCache.Name = "Ref 1"
	refNoCache2 := NewEntity[getByReferenceReferenceNoCache](c).TrackedEntity()
	refNoCache2.Name = "Ref 2"
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

	// Update set to nil
	entity = EditEntity[getByReferenceEntity](c, rows[0]).TrackedEntity()
	entity.Ref = nil
	entity.RefCached = nil
	entity.RefCachedNoCache = nil
	err = c.Flush(false)
	assert.NoError(t, err)
	loggerDB.Clear()

	rows = GetByReference[getByReferenceEntity](c, "RefCached", ref.ID)
	assert.Len(t, rows, 9)
	assert.Equal(t, entities[1].ID, rows[0].ID)
	assert.Equal(t, entities[1].Name, rows[0].Name)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
	loggerDB.Clear()

	rows2 = GetByReference[getByReferenceEntity](c, "RefCachedNoCache", refNoCache.ID)
	assert.Len(t, rows2, 9)
	assert.Equal(t, entities[1].ID, rows2[0].ID)
	assert.Equal(t, entities[1].Name, rows2[0].Name)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
	loggerDB.Clear()

	// update change id
	entity = EditEntity[getByReferenceEntity](c, entities[3]).TrackedEntity()
	entity.Ref = NewReference[getByReferenceReference](ref2.ID)
	entity.RefCached = NewReference[getByReferenceReference](ref2.ID)
	entity.RefCachedNoCache = NewReference[getByReferenceReferenceNoCache](refNoCache2.ID)
	err = c.Flush(false)
	assert.NoError(t, err)
	loggerDB.Clear()

	rows = GetByReference[getByReferenceEntity](c, "RefCached", ref.ID)
	assert.Len(t, rows, 8)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
	rows = GetByReference[getByReferenceEntity](c, "RefCached", ref2.ID)
	assert.Len(t, rows, 1)
	assert.Equal(t, "Name 3", rows[0].Name)

	rows2 = GetByReference[getByReferenceEntity](c, "RefCachedNoCache", refNoCache.ID)
	assert.Len(t, rows2, 8)

	rows2 = GetByReference[getByReferenceEntity](c, "RefCachedNoCache", refNoCache2.ID)
	assert.Len(t, rows2, 1)
	assert.Equal(t, "Name 3", rows2[0].Name)

	DeleteEntity(c, entities[7])
	err = c.Flush(false)
	assert.NoError(t, err)
	rows = GetByReference[getByReferenceEntity](c, "RefCached", ref.ID)
	assert.Len(t, rows, 7)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
}
