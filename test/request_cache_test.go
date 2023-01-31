package test

import (
	"testing"

	"github.com/latolukasz/beeorm"

	"github.com/stretchr/testify/assert"
)

type requestCacheEntity struct {
	beeorm.ORM `orm:"redisCache"`
	Name       string              `orm:"length=100;index=name"`
	Code       string              `orm:"unique=code"`
	IndexName  *beeorm.CachedQuery `query:":Name = ?"`
	IndexCode  *beeorm.CachedQuery `queryOne:":Code = ?"`
}

func TestRequestCache(t *testing.T) {
	var entity *requestCacheEntity
	engine := PrepareTables(t, &beeorm.Registry{}, 5, 6, "", entity)

	flusher := engine.NewFlusher()
	e := &requestCacheEntity{Name: "a", Code: "a1"}
	e2 := &requestCacheEntity{Name: "b", Code: "a2"}
	e3 := &requestCacheEntity{Name: "c", Code: "a3"}
	e4 := &requestCacheEntity{Name: "d", Code: "a4"}
	e5 := &requestCacheEntity{Name: "d", Code: "a5"}
	flusher.Track(e)
	flusher.Track(e2)
	flusher.Track(e3)
	flusher.Track(e4)
	flusher.Track(e5)
	flusher.Flush()
	id := e.GetID()

	engine.EnableRequestCache()

	dbLogger := &MockLogHandler{}
	engine.RegisterQueryLogger(dbLogger, true, false, false)
	redisLogger := &MockLogHandler{}
	engine.RegisterQueryLogger(redisLogger, false, true, false)

	entity = &requestCacheEntity{}
	found := engine.LoadByID(id, entity)
	assert.True(t, found)
	assert.Equal(t, id, entity.GetID())
	assert.Equal(t, "a", entity.Name)
	assert.Len(t, dbLogger.Logs, 1)
	assert.Len(t, redisLogger.Logs, 2)

	found = engine.LoadByID(id, entity)
	assert.True(t, found)
	assert.Equal(t, id, entity.GetID())
	assert.Equal(t, "a", entity.Name)
	assert.Len(t, dbLogger.Logs, 1)
	assert.Len(t, redisLogger.Logs, 2)

	entities := make([]*requestCacheEntity, 0)
	engine.LoadByIDs([]uint64{e2.GetID(), e3.GetID()}, &entities)
	assert.Equal(t, "b", entities[0].Name)
	assert.Equal(t, "c", entities[1].Name)
	assert.Len(t, dbLogger.Logs, 2)
	assert.Len(t, redisLogger.Logs, 4)
	engine.LoadByIDs([]uint64{e2.GetID(), e3.GetID()}, &entities)
	assert.Equal(t, "b", entities[0].Name)
	assert.Equal(t, "c", entities[1].Name)
	assert.Len(t, dbLogger.Logs, 2)
	assert.Len(t, redisLogger.Logs, 4)

	e6 := &requestCacheEntity{Name: "f"}
	engine.Flush(e6)
	dbLogger.Clear()
	redisLogger.Clear()
	found = engine.LoadByID(e6.GetID(), entity)
	assert.True(t, found)
	assert.Equal(t, e6.GetID(), entity.GetID())
	assert.Equal(t, "f", entity.Name)
	assert.Len(t, dbLogger.Logs, 0)
	assert.Len(t, redisLogger.Logs, 0)
	entity.Name = "f2"
	engine.Flush(entity)
	id = entity.GetID()
	dbLogger.Clear()
	redisLogger.Clear()
	entity = &requestCacheEntity{}
	found = engine.LoadByID(id, entity)
	assert.True(t, found)
	assert.Equal(t, id, entity.GetID())
	assert.Equal(t, "f2", entity.Name)
	assert.Len(t, dbLogger.Logs, 0)
	assert.Len(t, redisLogger.Logs, 0)
	engine.Delete(entity)
	dbLogger.Clear()
	redisLogger.Clear()
	found = engine.LoadByID(id, entity)
	assert.False(t, found)
	dbLogger.Clear()
	redisLogger.Clear()

	totalRows := engine.CachedSearch(&entities, "IndexName", nil, "d")
	assert.Equal(t, totalRows, 2)
	dbLogger.Clear()
	redisLogger.Clear()
	totalRows = engine.CachedSearch(&entities, "IndexName", nil, "d")
	assert.Equal(t, totalRows, 2)
	assert.Equal(t, "d", entities[0].Name)
	assert.Equal(t, "d", entities[1].Name)
	assert.Len(t, redisLogger.Logs, 0)
	entities[0].Name = "d2"
	engine.Flush(entities[0])
	dbLogger.Clear()
	redisLogger.Clear()
	totalRows = engine.CachedSearch(&entities, "IndexName", nil, "d")
	assert.Equal(t, totalRows, 1)

	found = engine.CachedSearchOne(entity, "IndexCode", "a2")
	assert.True(t, found)
	assert.Equal(t, "b", entity.Name)
	dbLogger.Clear()
	redisLogger.Clear()
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
	dbLogger.Clear()
	redisLogger.Clear()
	engine.LoadByID(1, entity)
	assert.Len(t, dbLogger.Logs, 1)
	assert.Len(t, redisLogger.Logs, 2)
}
