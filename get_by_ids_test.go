package beeorm

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

type getByIdsEntity struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string `orm:"max=100"`
}

func (e *getByIdsEntity) GetID() uint64 {
	return e.ID
}

func TestLoadByIdsNoCache(t *testing.T) {
	testLoadByIds(t, false, false)
}

func TestLoadByIdsLocalCache(t *testing.T) {
	testLoadByIds(t, true, false)
}

func TestLoadByIdsRedisCache(t *testing.T) {
	testLoadByIds(t, false, true)
}

func TestLoadByIdsLocalRedisCache(t *testing.T) {
	testLoadByIds(t, true, true)
}

func testLoadByIds(t *testing.T, local, redis bool) {
	var entity *getByIdsEntity
	c := PrepareTables(t, &Registry{}, entity)
	schema := GetEntitySchema[*getByIdsEntity](c)
	schema.DisableCache(!local, !redis)

	var ids []uint64
	for i := 0; i < 10; i++ {
		entity = NewEntity[*getByIdsEntity](c).TrackedEntity()
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
	rows := GetByIDs[*getByIdsEntity](c, ids...)
	assert.Len(t, rows, 10)
	for i := 0; i < 10; i++ {
		assert.NotNil(t, rows[i])
		assert.Equal(t, fmt.Sprintf("Name %d", i), rows[i].Name)
	}
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
	rows = GetByIDs[*getByIdsEntity](c, ids...)
	assert.Len(t, rows, 10)
	for i := 0; i < 10; i++ {
		assert.NotNil(t, rows[i])
		assert.Equal(t, fmt.Sprintf("Name %d", i), rows[i].Name)
	}
	assert.Len(t, loggerDB.Logs, 1)
	loggerDB.Clear()
	if local || redis {
		rows = GetByIDs[*getByIdsEntity](c, ids...)
		assert.Len(t, rows, 10)
		for i := 0; i < 10; i++ {
			assert.NotNil(t, rows[i])
			assert.Equal(t, fmt.Sprintf("Name %d", i), rows[i].Name)
		}
		assert.Len(t, loggerDB.Logs, 0)
	}
	loggerDB.Clear()

	// invalid ids
	rows = GetByIDs[*getByIdsEntity](c, 1, 2, 3)
	assert.Len(t, rows, 3)
	for i := 0; i < 3; i++ {
		assert.Nil(t, rows[i])
	}
	assert.Len(t, loggerDB.Logs, 1)
	loggerDB.Clear()
	if local || redis {
		rows = GetByIDs[*getByIdsEntity](c, 1, 2, 3)
		assert.Len(t, rows, 3)
		for i := 0; i < 3; i++ {
			assert.Nil(t, rows[i])
		}
		assert.Len(t, loggerDB.Logs, 0)
	}
	if local && redis {
		lc, _ := schema.GetLocalCache()
		lc.Clear(c)
		loggerDB.Clear()
		rows = GetByIDs[*getByIdsEntity](c, 1, 2, 3)
		assert.Len(t, rows, 3)
		for i := 0; i < 3; i++ {
			assert.Nil(t, rows[i])
		}
		assert.Len(t, loggerDB.Logs, 0)
		loggerLocal.Clear()
		loggerRedis.Clear()
		rows = GetByIDs[*getByIdsEntity](c, 1, 2, 3)
		assert.Len(t, rows, 3)
		for i := 0; i < 3; i++ {
			assert.Nil(t, rows[i])
		}
		assert.Len(t, loggerDB.Logs, 0)
		assert.Len(t, loggerRedis.Logs, 0)
	}

	// missing one
	rows = GetByIDs[*getByIdsEntity](c, ids[0], 2, ids[1])
	assert.Len(t, rows, 3)
	assert.NotNil(t, rows[0])
	assert.Nil(t, rows[1])
	assert.NotNil(t, rows[2])

	// duplicated
	rows = GetByIDs[*getByIdsEntity](c, ids[0], ids[0], ids[0])
	assert.Len(t, rows, 3)
	for i := 0; i < 3; i++ {
		assert.NotNil(t, rows[i])
		assert.Equal(t, ids[0], rows[i].ID)
		assert.Equal(t, "Name 0", rows[i].Name)
	}
}
