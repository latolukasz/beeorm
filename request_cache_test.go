package beeorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type requestCacheEntity struct {
	ORM       `orm:"redisCache"`
	ID        uint
	Name      string       `orm:"length=100;index=name"`
	Code      string       `orm:"unique=code"`
	IndexName *CachedQuery `query:":Name = ?"`
	IndexCode *CachedQuery `queryOne:":Code = ?"`
}

func TestRequestCache(t *testing.T) {
	var entity *requestCacheEntity
	engine := PrepareTables(t, &Registry{}, 5, entity)

	flusher := engine.NewFlusher()
	flusher.Track(&requestCacheEntity{Name: "a", Code: "a1"})
	flusher.Track(&requestCacheEntity{Name: "b", Code: "a2"})
	flusher.Track(&requestCacheEntity{Name: "c", Code: "a3"})
	flusher.Track(&requestCacheEntity{Name: "d", Code: "a4"})
	flusher.Track(&requestCacheEntity{Name: "d", Code: "a5"})
	flusher.Flush()

	engine.EnableRequestCache()

	dbLogger := &testLogHandler{}
	engine.RegisterQueryLogger(dbLogger, true, false, false)
	redisLogger := &testLogHandler{}
	engine.RegisterQueryLogger(redisLogger, false, true, false)

	entity = &requestCacheEntity{}
	found := engine.LoadByID(1, entity)
	assert.True(t, found)
	assert.Equal(t, uint(1), entity.ID)
	assert.Equal(t, "a", entity.Name)
	assert.Len(t, dbLogger.Logs, 1)
	assert.Len(t, redisLogger.Logs, 2)

	found = engine.LoadByID(1, entity)
	assert.True(t, found)
	assert.Equal(t, uint(1), entity.ID)
	assert.Equal(t, "a", entity.Name)
	assert.Len(t, dbLogger.Logs, 1)
	assert.Len(t, redisLogger.Logs, 2)

	entities := make([]*requestCacheEntity, 0)
	engine.LoadByIDs([]uint64{2, 3}, &entities)
	assert.Equal(t, "b", entities[0].Name)
	assert.Equal(t, "c", entities[1].Name)
	assert.Len(t, dbLogger.Logs, 2)
	assert.Len(t, redisLogger.Logs, 4)
	engine.LoadByIDs([]uint64{2, 3}, &entities)
	assert.Equal(t, "b", entities[0].Name)
	assert.Equal(t, "c", entities[1].Name)
	assert.Len(t, dbLogger.Logs, 2)
	assert.Len(t, redisLogger.Logs, 4)

	engine.Flush(&requestCacheEntity{Name: "f"})
	dbLogger.clear()
	redisLogger.clear()
	found = engine.LoadByID(6, entity)
	assert.True(t, found)
	assert.Equal(t, uint(6), entity.ID)
	assert.Equal(t, "f", entity.Name)
	assert.Len(t, dbLogger.Logs, 0)
	assert.Len(t, redisLogger.Logs, 0)
	entity.Name = "f2"
	engine.Flush(entity)
	dbLogger.clear()
	redisLogger.clear()
	entity = &requestCacheEntity{}
	found = engine.LoadByID(6, entity)
	assert.True(t, found)
	assert.Equal(t, uint(6), entity.ID)
	assert.Equal(t, "f2", entity.Name)
	assert.Len(t, dbLogger.Logs, 0)
	assert.Len(t, redisLogger.Logs, 0)
	engine.Delete(entity)
	dbLogger.clear()
	redisLogger.clear()
	found = engine.LoadByID(6, entity)
	assert.False(t, found)
	dbLogger.clear()
	redisLogger.clear()

	totalRows := engine.CachedSearch(&entities, "IndexName", nil, "d")
	assert.Equal(t, totalRows, 2)
	dbLogger.clear()
	redisLogger.clear()
	totalRows = engine.CachedSearch(&entities, "IndexName", nil, "d")
	assert.Equal(t, totalRows, 2)
	assert.Equal(t, "d", entities[0].Name)
	assert.Equal(t, "d", entities[1].Name)
	assert.Len(t, redisLogger.Logs, 0)
	entities[0].Name = "d2"
	engine.Flush(entities[0])
	dbLogger.clear()
	redisLogger.clear()
	totalRows = engine.CachedSearch(&entities, "IndexName", nil, "d")
	assert.Equal(t, totalRows, 1)

	found = engine.CachedSearchOne(entity, "IndexCode", "a2")
	assert.True(t, found)
	assert.Equal(t, "b", entity.Name)
	dbLogger.clear()
	redisLogger.clear()
	found = engine.CachedSearchOne(entity, "IndexCode", "a2")
	assert.True(t, found)
	assert.Equal(t, "b", entity.Name)
	assert.Len(t, dbLogger.Logs, 0)
	assert.Len(t, redisLogger.Logs, 0)
	entity.Code = "a22"
	engine.Flush(entity)
	found = engine.CachedSearchOne(entity, "IndexCode", "a2")
	assert.False(t, found)

	found = engine.LoadByID(1, entity)
	assert.True(t, found)
	engine.ClearCacheByIDs(entity, 1)
	dbLogger.clear()
	redisLogger.clear()
	engine.LoadByID(1, entity)
	assert.Len(t, dbLogger.Logs, 1)
	assert.Len(t, redisLogger.Logs, 2)
}
