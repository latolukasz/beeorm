package beeorm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type getByIdsEntity struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string `orm:"max=100"`
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
	c := PrepareTables(t, NewRegistry(), entity)
	schema := GetEntitySchema[getByIdsEntity](c)
	schema.DisableCache(!local, !redis)

	var ids []uint64
	for i := 0; i < 10; i++ {
		entity = NewEntity[getByIdsEntity](c)
		entity.Name = fmt.Sprintf("Name %d", i)
		ids = append(ids, entity.ID)
	}
	err := c.Flush()
	assert.NoError(t, err)

	loggerDB := &MockLogHandler{}
	c.RegisterQueryLogger(loggerDB, true, false, false)
	loggerRedis := &MockLogHandler{}
	c.RegisterQueryLogger(loggerRedis, false, true, false)
	loggerLocal := &MockLogHandler{}
	c.RegisterQueryLogger(loggerLocal, false, false, false)
	rows := GetByIDs[getByIdsEntity](c, ids...)
	assert.Equal(t, 10, rows.Len())
	i := 0
	for rows.Next() {
		e := rows.Entity()
		assert.NotNil(t, e)
		assert.Equal(t, fmt.Sprintf("Name %d", i), e.Name)
		i++
	}
	assert.Equal(t, 10, i)
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
	rows = GetByIDs[getByIdsEntity](c, ids...)
	assert.Equal(t, 10, rows.Len())
	i = 0
	for rows.Next() {
		e := rows.Entity()
		assert.NotNil(t, e)
		assert.Equal(t, fmt.Sprintf("Name %d", i), e.Name)
		i++
	}
	if local {
		assert.Len(t, loggerDB.Logs, 10)
	} else {
		assert.Len(t, loggerDB.Logs, 1)
	}

	loggerDB.Clear()
	if local || redis {
		rows = GetByIDs[getByIdsEntity](c, ids...)
		assert.Equal(t, 10, rows.Len())
		i = 0
		for rows.Next() {
			e := rows.Entity()
			assert.NotNil(t, e)
			assert.Equal(t, fmt.Sprintf("Name %d", i), e.Name)
			i++
		}
		assert.Len(t, loggerDB.Logs, 0)
	}
	loggerDB.Clear()

	// invalid ids
	rows = GetByIDs[getByIdsEntity](c, 1, 2, 3)
	assert.Equal(t, 3, rows.Len())
	i = 0
	for rows.Next() {
		assert.Nil(t, rows.Entity())
		i++
	}
	assert.Equal(t, 3, i)
	if local {
		assert.Len(t, loggerDB.Logs, 3)
	} else {
		assert.Len(t, loggerDB.Logs, 1)
	}
	loggerDB.Clear()
	if local || redis {
		rows = GetByIDs[getByIdsEntity](c, 1, 2, 3)
		assert.Equal(t, 3, rows.Len())
		for rows.Next() {
			assert.Nil(t, rows.Entity())
		}
		assert.Len(t, loggerDB.Logs, 0)
	}
	if local && redis {
		lc, _ := schema.GetLocalCache()
		lc.Clear(c)
		loggerDB.Clear()
		rows = GetByIDs[getByIdsEntity](c, 1, 2, 3)
		assert.Equal(t, 3, rows.Len())
		for rows.Next() {
			assert.Nil(t, rows.Entity())
		}
		assert.Len(t, loggerDB.Logs, 0)
		loggerLocal.Clear()
		loggerRedis.Clear()
		rows = GetByIDs[getByIdsEntity](c, 1, 2, 3)
		assert.Equal(t, 3, rows.Len())
		for rows.Next() {
			assert.Nil(t, rows.Entity())
		}
		assert.Len(t, loggerDB.Logs, 0)
		assert.Len(t, loggerRedis.Logs, 0)
	}

	// missing one
	rows = GetByIDs[getByIdsEntity](c, ids[0], 2, ids[1])
	assert.Equal(t, 3, rows.Len())
	rows.Next()
	assert.NotNil(t, rows.Entity())
	rows.Next()
	assert.Nil(t, rows.Entity())
	rows.Next()
	assert.NotNil(t, rows.Entity())

	// duplicated
	rows = GetByIDs[getByIdsEntity](c, ids[0], ids[0], ids[0])
	assert.Equal(t, 3, rows.Len())
	for rows.Next() {
		e := rows.Entity()
		assert.NotNil(t, e)
		assert.Equal(t, ids[0], e.ID)
		assert.Equal(t, "Name 0", e.Name)
	}
}
