package beeorm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
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
	c := PrepareTables(t, NewRegistry(), entity, getByReferenceReference{}, getByReferenceReferenceNoCache{})
	schema := GetEntitySchema[getByReferenceEntity](c)
	schema.DisableCache(!local, !redis)

	loggerDB := &MockLogHandler{}
	c.RegisterQueryLogger(loggerDB, true, false, false)

	// getting missing rows
	rows := GetByReference[getByReferenceEntity](c, "RefCached", 1)
	assert.Equal(t, 0, rows.Len())
	loggerDB.Clear()
	rows = GetByReference[getByReferenceEntity](c, "RefCached", 1)
	assert.Equal(t, 0, rows.Len())
	assert.Len(t, loggerDB.Logs, 0)
	loggerDB.Clear()

	var entities []*getByReferenceEntity
	ref := NewEntity[getByReferenceReference](c)
	ref.Name = "Ref 1"
	ref2 := NewEntity[getByReferenceReference](c)
	ref2.Name = "Ref 2"
	refNoCache := NewEntity[getByReferenceReferenceNoCache](c)
	refNoCache.Name = "Ref 1"
	refNoCache2 := NewEntity[getByReferenceReferenceNoCache](c)
	refNoCache2.Name = "Ref 2"
	for i := 0; i < 10; i++ {
		entity = NewEntity[getByReferenceEntity](c)
		entity.Name = fmt.Sprintf("Name %d", i)
		entity.Ref = &Reference[getByReferenceReference]{ID: ref.ID}
		entity.RefCached = &Reference[getByReferenceReference]{ID: ref.ID}
		entity.RefCachedNoCache = &Reference[getByReferenceReferenceNoCache]{ID: refNoCache.ID}
		entities = append(entities, entity)
	}
	err := c.Flush()
	assert.NoError(t, err)

	loggerDB.Clear()
	rows = GetByReference[getByReferenceEntity](c, "Ref", ref.ID)
	assert.Equal(t, 10, rows.Len())
	rows.Next()
	e := rows.Entity()
	assert.Equal(t, entities[0].ID, e.ID)
	assert.Equal(t, entities[0].Name, e.Name)
	assert.Len(t, loggerDB.Logs, 1)

	loggerDB.Clear()
	rows = GetByReference[getByReferenceEntity](c, "RefCached", ref.ID)
	assert.Equal(t, 10, rows.Len())
	rows.Next()
	e = rows.Entity()
	assert.Equal(t, entities[0].ID, e.ID)
	assert.Equal(t, entities[0].Name, e.Name)
	assert.Len(t, loggerDB.Logs, 1)
	loggerDB.Clear()
	rows = GetByReference[getByReferenceEntity](c, "RefCached", ref.ID)
	assert.Equal(t, 10, rows.Len())
	rows.Next()
	e = rows.Entity()
	assert.Equal(t, entities[0].ID, e.ID)
	assert.Equal(t, entities[0].Name, e.Name)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
	loggerDB.Clear()

	rows2 := GetByReference[getByReferenceEntity](c, "RefCachedNoCache", ref.ID)
	assert.Equal(t, 10, rows2.Len())
	rows2.Next()
	e = rows2.Entity()
	assert.Equal(t, entities[0].ID, e.ID)
	assert.Equal(t, entities[0].Name, e.Name)
	assert.Len(t, loggerDB.Logs, 1)
	loggerDB.Clear()
	rows2 = GetByReference[getByReferenceEntity](c, "RefCachedNoCache", ref.ID)
	assert.Equal(t, 10, rows2.Len())
	rows2.Next()
	e = rows2.Entity()
	assert.Equal(t, entities[0].ID, e.ID)
	assert.Equal(t, entities[0].Name, e.Name)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}

	// Update set to nil
	entity = EditEntity(c, e)
	entity.Ref = nil
	entity.RefCached = nil
	entity.RefCachedNoCache = nil
	err = c.Flush()
	assert.NoError(t, err)
	loggerDB.Clear()

	rows = GetByReference[getByReferenceEntity](c, "RefCached", ref.ID)
	assert.Equal(t, 9, rows.Len())
	rows.Next()
	e = rows.Entity()
	assert.Equal(t, entities[1].ID, e.ID)
	assert.Equal(t, entities[1].Name, e.Name)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
	loggerDB.Clear()

	rows2 = GetByReference[getByReferenceEntity](c, "RefCachedNoCache", refNoCache.ID)
	assert.Equal(t, 9, rows2.Len())
	rows2.Next()
	e = rows2.Entity()
	assert.Equal(t, entities[1].ID, e.ID)
	assert.Equal(t, entities[1].Name, e.Name)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
	loggerDB.Clear()

	// update change id
	entity = EditEntity[getByReferenceEntity](c, entities[3])
	entity.Ref = &Reference[getByReferenceReference]{ID: ref2.ID}
	entity.RefCached = &Reference[getByReferenceReference]{ID: ref2.ID}
	entity.RefCachedNoCache = &Reference[getByReferenceReferenceNoCache]{ID: refNoCache2.ID}
	err = c.Flush()
	assert.NoError(t, err)
	loggerDB.Clear()

	rows = GetByReference[getByReferenceEntity](c, "RefCached", ref.ID)
	assert.Equal(t, 8, rows.Len())
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
	rows = GetByReference[getByReferenceEntity](c, "RefCached", ref2.ID)
	assert.Equal(t, 1, rows.Len())
	rows.Next()
	e = rows.Entity()
	assert.Equal(t, "Name 3", e.Name)

	rows2 = GetByReference[getByReferenceEntity](c, "RefCachedNoCache", refNoCache.ID)
	assert.Equal(t, 8, rows2.Len())

	rows2 = GetByReference[getByReferenceEntity](c, "RefCachedNoCache", refNoCache2.ID)
	assert.Equal(t, 1, rows2.Len())
	rows.Next()
	e = rows.Entity()
	assert.Equal(t, "Name 3", e.Name)

	DeleteEntity(c, entities[7])
	err = c.Flush()
	assert.NoError(t, err)
	loggerDB.Clear()
	rows = GetByReference[getByReferenceEntity](c, "RefCached", ref.ID)
	assert.Equal(t, 7, rows.Len())
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}

	loggerDB.Clear()
	rows2 = GetByReference[getByReferenceEntity](c, "RefCachedNoCache", refNoCache.ID)
	assert.Equal(t, 7, rows.Len())
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}

	err = EditEntityField(c, entities[0], "RefCached", ref2)
	assert.NoError(t, err)
	assert.NoError(t, c.Flush())
	rows = GetByReference[getByReferenceEntity](c, "RefCached", ref2.ID)
	assert.Equal(t, 2, rows.Len())
	rows = GetByReference[getByReferenceEntity](c, "RefCached", ref.ID)
	assert.Equal(t, 7, rows.Len())
	err = EditEntityField(c, entities[0], "RefCached", ref)
	assert.NoError(t, err)
	assert.NoError(t, c.Flush())
	rows = GetByReference[getByReferenceEntity](c, "RefCached", ref2.ID)
	assert.Equal(t, 1, rows.Len())
	rows = GetByReference[getByReferenceEntity](c, "RefCached", ref.ID)
	assert.Equal(t, 8, rows.Len())
}
