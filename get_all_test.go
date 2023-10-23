package beeorm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type getByAllCachedEntity struct {
	ID   uint64 `orm:"localCache;redisCache;cacheAll"`
	Name string
}

type getByAllNotCachedEntity struct {
	ID   uint64 `orm:"localCache;redisCache;"`
	Name string
}

func TestGetAllNoCache(t *testing.T) {
	testGetAll(t, false, false)
}

func TestGetAllLocalCache(t *testing.T) {
	testGetAll(t, true, false)
}

func TestGetAllRedisCache(t *testing.T) {
	testGetAll(t, false, true)
}

func TestGetAllLocalRedisCache(t *testing.T) {
	testGetAll(t, true, true)
}

func testGetAll(t *testing.T, local, redis bool) {
	var entity *getByAllCachedEntity
	var entityNotCached *getByAllNotCachedEntity
	c := PrepareTables(t, &Registry{}, entity, entityNotCached)
	schema := GetEntitySchema[getByAllCachedEntity](c)
	schema.DisableCache(!local, !redis)

	loggerDB := &MockLogHandler{}
	c.RegisterQueryLogger(loggerDB, true, false, false)

	// getting missing rows
	rows := GetAll[getByAllCachedEntity](c)
	assert.Equal(t, 0, rows.Len())
	loggerDB.Clear()
	rows = GetAll[getByAllCachedEntity](c)
	assert.Equal(t, 0, rows.Len())
	assert.Len(t, loggerDB.Logs, 0)
	loggerDB.Clear()
	rows2 := GetAll[getByAllNotCachedEntity](c)
	assert.Equal(t, 0, rows2.Len())
	loggerDB.Clear()

	var entities []*getByAllCachedEntity
	var entitiesNoCache []*getByAllNotCachedEntity
	for i := 0; i < 10; i++ {
		entity = NewEntity[getByAllCachedEntity](c).TrackedEntity()
		entity.Name = fmt.Sprintf("Name %d", i)
		entities = append(entities, entity)

		entityNotCached = NewEntity[getByAllNotCachedEntity](c).TrackedEntity()
		entityNotCached.Name = fmt.Sprintf("Name %d", i)
		entitiesNoCache = append(entitiesNoCache, entityNotCached)
	}
	err := c.Flush(false)
	assert.NoError(t, err)

	loggerDB.Clear()
	rows = GetAll[getByAllCachedEntity](c)
	assert.Equal(t, 10, rows.Len())
	rows.Next()
	e := rows.Entity()
	assert.Equal(t, entities[0].ID, e.ID)
	assert.Equal(t, entities[0].Name, e.Name)

	loggerDB.Clear()
	rows2 = GetAll[getByAllNotCachedEntity](c)
	assert.Equal(t, 10, rows2.Len())
	rows2.Next()
	e2 := rows2.Entity()
	assert.Equal(t, entitiesNoCache[0].ID, e2.ID)
	assert.Equal(t, entitiesNoCache[0].Name, e2.Name)
	assert.Len(t, loggerDB.Logs, 1)

	loggerDB.Clear()
	rows = GetAll[getByAllCachedEntity](c)
	assert.Equal(t, 10, rows.Len())
	rows.Next()
	e = rows.Entity()
	assert.Equal(t, entities[0].ID, e.ID)
	assert.Equal(t, entities[0].Name, e.Name)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}

	DeleteEntity(c, entities[7])
	DeleteEntity(c, entitiesNoCache[7])
	err = c.Flush(false)
	assert.NoError(t, err)
	loggerDB.Clear()
	rows = GetAll[getByAllCachedEntity](c)
	assert.Equal(t, 9, rows.Len())
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
	rows2 = GetAll[getByAllNotCachedEntity](c)
	assert.Equal(t, 9, rows2.Len())
}
