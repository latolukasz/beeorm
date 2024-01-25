package beeorm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type getByIndexEntity struct {
	ID   uint64     `orm:"localCache;redisCache"`
	Name string     `orm:"index=Name"`
	Age  uint32     `orm:"index=Age;cached"`
	Born *time.Time `orm:"index=Age:2;cached"`
}

func TestGetByIndexNoCache(t *testing.T) {
	testGetByIndex(t, false, false)
}

func TestGetByIndexLocalCache(t *testing.T) {
	testGetByIndex(t, true, false)
}

func TestGetByIndexRedisCache(t *testing.T) {
	testGetByIndex(t, false, true)
}

func TestGetByIndexLocalRedisCache(t *testing.T) {
	testGetByIndex(t, true, true)
}

func testGetByIndex(t *testing.T, local, redis bool) {
	var entity *getByIndexEntity
	orm := PrepareTables(t, NewRegistry(), entity)
	schema := GetEntitySchema[getByIndexEntity](orm)
	schema.DisableCache(!local, !redis)

	loggerDB := &MockLogHandler{}
	orm.RegisterQueryLogger(loggerDB, true, false, false)

	now := time.Now().UTC()
	nextWeek := now.Add(time.Hour * 24 * 7)
	// getting missing rows
	rows := GetByIndex[getByIndexEntity](orm, "Age", 23, now)
	assert.Equal(t, 0, rows.Len())
	loggerDB.Clear()
	rows = GetByIndex[getByIndexEntity](orm, "Age", 23, now)
	assert.Equal(t, 0, rows.Len())
	assert.Len(t, loggerDB.Logs, 0)

	var entities []*getByIndexEntity
	for i := 0; i < 10; i++ {
		entity = NewEntity[getByIndexEntity](orm)
		entity.Age = 10
		if i >= 5 {
			entity.Name = "Test Name"
			entity.Age = 18
			entity.Born = &now
		}
		if i >= 8 {
			entity.Name = "Test Name 2"
			entity.Age = 40
			entity.Born = &nextWeek
		}
		entities = append(entities, entity)
	}
	assert.NoError(t, orm.Flush())

	rows = GetByIndex[getByIndexEntity](orm, "Name", nil)
	assert.Equal(t, 5, rows.Len())
	rows.Next()
	e := rows.Entity()
	assert.Equal(t, entities[0].ID, e.ID)

	rows = GetByIndex[getByIndexEntity](orm, "Name", "Test name")
	assert.Equal(t, 3, rows.Len())
	rows.Next()
	e = rows.Entity()
	assert.Equal(t, entities[5].ID, e.ID)

	rows = GetByIndex[getByIndexEntity](orm, "Age", 10, nil)
	assert.Equal(t, 5, rows.Len())
	rows.Next()
	e = rows.Entity()
	assert.Equal(t, entities[0].ID, e.ID)
	loggerDB.Clear()
	rows = GetByIndex[getByIndexEntity](orm, "Age", 10, nil)
	assert.Equal(t, 5, rows.Len())
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
	loggerDB.Clear()

	rows = GetByIndex[getByIndexEntity](orm, "Age", 18, now)
	assert.Equal(t, 3, rows.Len())
	rows.Next()
	e = rows.Entity()
	loggerDB.Clear()
	assert.Equal(t, entities[5].ID, e.ID)
	rows = GetByIndex[getByIndexEntity](orm, "Age", 18, now)
	assert.Equal(t, 3, rows.Len())
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
	loggerDB.Clear()

	entity = NewEntity[getByIndexEntity](orm)
	entity.Age = 10
	assert.NoError(t, orm.Flush())
	entities = append(entities, entity)

	loggerDB.Clear()
	rows = GetByIndex[getByIndexEntity](orm, "Age", 10, nil)
	all := rows.All()
	assert.Equal(t, 6, rows.Len())
	assert.Equal(t, all[5].ID, entity.ID)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}

	DeleteEntity(orm, entities[0])
	assert.NoError(t, orm.Flush())

	loggerDB.Clear()
	rows = GetByIndex[getByIndexEntity](orm, "Age", 10, nil)
	all = rows.All()
	assert.Equal(t, 5, rows.Len())
	assert.Equal(t, all[0].ID, entities[1].ID)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}

	entity = EditEntity(orm, entities[6])
	entity.Name = ""
	entity.Age = 40
	assert.NoError(t, orm.Flush())

	loggerDB.Clear()
	rows = GetByIndex[getByIndexEntity](orm, "Name", nil)
	all = rows.All()
	assert.Equal(t, 6, rows.Len())
	assert.Equal(t, all[4].ID, entities[6].ID)

	loggerDB.Clear()
	rows = GetByIndex[getByIndexEntity](orm, "Name", "Test name")
	all = rows.All()
	assert.Equal(t, 2, rows.Len())
	assert.Equal(t, all[0].ID, entities[5].ID)

	rows = GetByIndex[getByIndexEntity](orm, "Age", 40, now)
	all = rows.All()
	assert.Equal(t, 1, rows.Len())
	assert.Equal(t, all[0].ID, entities[6].ID)
	loggerDB.Clear()
	rows = GetByIndex[getByIndexEntity](orm, "Age", 40, now)
	all = rows.All()
	assert.Equal(t, 1, rows.Len())
	assert.Equal(t, all[0].ID, entities[6].ID)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}

	loggerDB.Clear()
	rows = GetByIndex[getByIndexEntity](orm, "Age", 18, now)
	all = rows.All()
	assert.Equal(t, 2, rows.Len())
	assert.Equal(t, all[0].ID, entities[5].ID)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}

	assert.NoError(t, EditEntityField(orm, entity, "Age", 18))
	assert.NoError(t, orm.Flush())

	loggerDB.Clear()
	rows = GetByIndex[getByIndexEntity](orm, "Age", 18, now)
	all = rows.All()
	assert.Equal(t, 3, rows.Len())
	assert.Equal(t, all[1].ID, entities[6].ID)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}

	loggerDB.Clear()
	rows = GetByIndex[getByIndexEntity](orm, "Age", 40, now)
	all = rows.All()
	assert.Equal(t, 0, rows.Len())
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
}
