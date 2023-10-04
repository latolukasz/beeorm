package beeorm

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

type getByIdEntity struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string `orm:"max=100"`
}

func TestLoadByIdNoCache(t *testing.T) {
	testLoadById(t, false, false)
}

func TestLoadByIdLocalCache(t *testing.T) {
	testLoadById(t, true, false)
}

func TestLoadByIdRedisCache(t *testing.T) {
	testLoadById(t, false, true)
}

func TestLoadByIdLocalRedisCache(t *testing.T) {
	testLoadById(t, true, true)
}

func testLoadById(t *testing.T, local, redis bool) {
	var entity *getByIdEntity
	c := PrepareTables(t, &Registry{}, entity)
	schema := GetEntitySchema[getByIdEntity](c)
	schema.DisableCache(!local, !redis)

	var ids []uint64
	for i := 0; i < 10; i++ {
		entity = NewEntity[getByIdEntity](c).TrackedEntity()
		entity.Name = fmt.Sprintf("Name %d", i)
		ids = append(ids, entity.ID)
	}
	err := c.Flush(false)
	assert.NoError(t, err)

	loggerDB := &MockLogHandler{}
	c.RegisterQueryLogger(loggerDB, true, false, false)
	loggerRedis := &MockLogHandler{}
	c.RegisterQueryLogger(loggerRedis, false, true, false)
	loggerLocal := &MockLogHandler{}
	c.RegisterQueryLogger(loggerLocal, false, false, false)
	entity = GetByID[getByIdEntity](c, ids[0])
	assert.NotNil(t, entity)
	assert.Equal(t, "Name 0", entity.Name)
	if !local && !redis {
		assert.Len(t, loggerDB.Logs, 1)
	}
	loggerDB.Clear()
	if local {
		lc, _ := schema.GetLocalCache()
		lc.Clear(c)
	}
	if redis {
		rc, _ := schema.GetRedisCache()
		rc.FlushDB(c)
	}
	entity = GetByID[getByIdEntity](c, ids[0])
	assert.NotNil(t, entity)
	assert.Equal(t, "Name 0", entity.Name)
	assert.Len(t, loggerDB.Logs, 1)
	loggerDB.Clear()
	if local || redis {
		entity = GetByID[getByIdEntity](c, ids[0])
		assert.NotNil(t, entity)
		assert.Equal(t, "Name 0", entity.Name)
		assert.Len(t, loggerDB.Logs, 0)
	}
	loggerDB.Clear()

	// invalid id
	entity = GetByID[getByIdEntity](c, 1)
	assert.Nil(t, entity)
	assert.Len(t, loggerDB.Logs, 1)
	loggerDB.Clear()
	if local || redis {
		entity = GetByID[getByIdEntity](c, 1)
		assert.Nil(t, entity)
		assert.Len(t, loggerDB.Logs, 0)
	}
}
