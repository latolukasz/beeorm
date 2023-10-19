package beeorm

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

type logTableEntity struct {
	ID   uint64
	Name string
	Age  uint8
}

func TestLogTable(t *testing.T) {
	var entity *logTableEntity
	c := PrepareTables(t, &Registry{}, entity, &LogEntity[logTableEntity]{})
	assert.NotNil(t, c)

	entity = NewEntity[logTableEntity](c).TrackedEntity()
	entity.Name = "Test"
	entity.Age = 18
	err := c.Flush(false)
	assert.NoError(t, err)
	err = ConsumeLazyFlushEvents(c, false)
	assert.Nil(t, err)

	logs := Search[LogEntity[logTableEntity]](c, NewWhere("1"), nil)
	assert.Len(t, logs, 1)
	assert.Equal(t, entity.ID, logs[0].EntityID)
	assert.Nil(t, logs[0].Meta)
	assert.Nil(t, logs[0].After)
	assert.NotNil(t, logs[0].Before)
	var bind Bind
	err = jsoniter.ConfigFastest.Unmarshal(logs[0].Before, &bind)
	assert.NoError(t, err)
	assert.Len(t, bind, 3)
	assert.Equal(t, strconv.FormatUint(entity.ID, 10), bind["ID"])
	assert.Equal(t, "Test", bind["Name"])
	assert.Equal(t, "18", bind["Age"])

	c.SetMetaData("source", "test case")
	entity = NewEntity[logTableEntity](c).TrackedEntity()
	entity.Name = "Test 2"
	entity.Age = 30
	err = c.Flush(false)
	assert.NoError(t, err)
	err = ConsumeLazyFlushEvents(c, false)
	assert.Nil(t, err)

	logs = Search[LogEntity[logTableEntity]](c, NewWhere("1"), nil)
	assert.Len(t, logs, 2)
	assert.Equal(t, entity.ID, logs[1].EntityID)
	assert.NotNil(t, logs[1].Meta)
	assert.Nil(t, logs[1].After)
	assert.NotNil(t, logs[1].Before)
	bind = nil
	err = jsoniter.ConfigFastest.Unmarshal(logs[1].Meta, &bind)
	assert.NoError(t, err)
	assert.Len(t, bind, 1)
	assert.Equal(t, "test case", bind["source"])
}
