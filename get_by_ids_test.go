package beeorm

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

type loadByIdsEntity struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string `orm:"max=100"`
}

func (e *loadByIdsEntity) GetID() uint64 {
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
	var entity *loadByIdsEntity
	c := PrepareTables(t, &Registry{}, entity)
	schema := GetEntitySchema[*loadByIdsEntity](c)
	schema.DisableCache(!local, !redis)

	var ids []uint64
	for i := 0; i < 10; i++ {
		entity = NewEntity[*loadByIdsEntity](c).TrackedEntity()
		entity.Name = fmt.Sprintf("Name %d", i)
		ids = append(ids, entity.ID)
	}
	err := c.Flush(false)
	assert.NoError(t, err)

	loggerDB := &MockLogHandler{}
	c.RegisterQueryLogger(loggerDB, true, false, false)
	rows := GetByIDs[*loadByIdsEntity](c, ids...)
	assert.Len(t, rows, 10)
	for i := 0; i < 10; i++ {
		assert.NotNil(t, rows[i])
		assert.Equal(t, fmt.Sprintf("Name %d", i), rows[i].Name)
	}
	if !local && !redis {
		assert.Len(t, loggerDB.Logs, 1)
	}
}
