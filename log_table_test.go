package beeorm

import (
	"strconv"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
)

type logTableEntity struct {
	ID   uint64
	Name string
	Age  uint8
}

func TestLogTable(t *testing.T) {
	var entity *logTableEntity
	c := PrepareTables(t, NewRegistry(), entity, LogEntity[logTableEntity]{})
	assert.NotNil(t, c)

	entity = NewEntity[logTableEntity](c)
	entity.Name = "Test"
	entity.Age = 18
	err := c.Flush()
	assert.NoError(t, err)
	err = ConsumeAsyncFlushEvents(c, false)
	assert.Nil(t, err)

	logs := Search[LogEntity[logTableEntity]](c, NewWhere("1"), nil)
	logs.Next()
	log := logs.Entity()
	assert.Equal(t, 1, logs.Len())
	assert.Equal(t, entity.ID, log.EntityID)
	assert.Nil(t, log.Meta)
	assert.Nil(t, log.Before)
	assert.NotNil(t, log.After)
	var bind Bind
	err = jsoniter.ConfigFastest.Unmarshal(log.After, &bind)
	assert.NoError(t, err)
	assert.Len(t, bind, 3)
	assert.Equal(t, strconv.FormatUint(entity.ID, 10), bind["ID"])
	assert.Equal(t, "Test", bind["Name"])
	assert.Equal(t, "18", bind["Age"])

	c.SetMetaData("source", "test case")
	entity = NewEntity[logTableEntity](c)
	entity.Name = "Test 2"
	entity.Age = 30
	err = c.Flush()
	assert.NoError(t, err)
	err = ConsumeAsyncFlushEvents(c, false)
	assert.Nil(t, err)

	logs = Search[LogEntity[logTableEntity]](c, NewWhere("1"), nil)
	assert.Equal(t, 2, logs.Len())
	logs.Next()
	logs.Next()
	log = logs.Entity()
	assert.Equal(t, entity.ID, log.EntityID)
	assert.NotNil(t, log.Meta)
	assert.Nil(t, log.Before)
	assert.NotNil(t, log.After)
	bind = nil
	err = jsoniter.ConfigFastest.Unmarshal(log.Meta, &bind)
	assert.NoError(t, err)
	assert.Len(t, bind, 1)
	assert.Equal(t, "test case", bind["source"])

	entity = EditEntity(c, entity)
	entity.Name = "Test 3"
	entity.Age = 40
	err = c.Flush()
	assert.NoError(t, err)
	err = ConsumeAsyncFlushEvents(c, false)
	assert.NoError(t, err)
	logs = Search[LogEntity[logTableEntity]](c, NewWhere("1"), nil)
	assert.Equal(t, 3, logs.Len())
	logs.Next()
	logs.Next()
	logs.Next()
	log = logs.Entity()
	assert.Equal(t, entity.ID, log.EntityID)
	assert.NotNil(t, log.Meta)
	assert.NotNil(t, log.After)
	assert.NotNil(t, log.Before)
	bind = nil
	err = jsoniter.ConfigFastest.Unmarshal(log.Meta, &bind)
	assert.NoError(t, err)
	assert.Len(t, bind, 1)
	assert.Equal(t, "test case", bind["source"])
	bind = nil
	err = jsoniter.ConfigFastest.Unmarshal(log.Before, &bind)
	assert.NoError(t, err)
	assert.Equal(t, "Test 2", bind["Name"])
	assert.Equal(t, "30", bind["Age"])
	bind = nil
	err = jsoniter.ConfigFastest.Unmarshal(log.After, &bind)
	assert.NoError(t, err)
	assert.Equal(t, "Test 3", bind["Name"])
	assert.Equal(t, "40", bind["Age"])

	DeleteEntity(c, entity)
	err = c.Flush()
	assert.NoError(t, err)
	err = ConsumeAsyncFlushEvents(c, false)
	assert.NoError(t, err)
	logs = Search[LogEntity[logTableEntity]](c, NewWhere("1"), nil)
	assert.Equal(t, 4, logs.Len())
	logs.Next()
	logs.Next()
	logs.Next()
	logs.Next()
	log = logs.Entity()
	assert.Equal(t, entity.ID, log.EntityID)
	assert.NotNil(t, log.Meta)
	assert.Nil(t, log.After)
	assert.NotNil(t, log.Before)

	bind = nil
	err = jsoniter.ConfigFastest.Unmarshal(log.Meta, &bind)
	assert.NoError(t, err)
	assert.Len(t, bind, 1)
	assert.Equal(t, "test case", bind["source"])
	bind = nil
	err = jsoniter.ConfigFastest.Unmarshal(log.Before, &bind)
	assert.NoError(t, err)
	assert.Equal(t, "Test 3", bind["Name"])
	assert.Equal(t, "40", bind["Age"])
}
