package beeorm

import (
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
	err = runAsyncConsumer(c, false)
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
	assert.Equal(t, "Test", bind["Name"])
	assert.Equal(t, float64(18), bind["Age"])

	c.SetMetaData("source", "test case")
	entity = NewEntity[logTableEntity](c)
	entity.Name = "Test 2"
	entity.Age = 30
	err = c.Flush()
	assert.NoError(t, err)
	err = runAsyncConsumer(c, false)
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
	err = runAsyncConsumer(c, false)
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
	assert.Equal(t, float64(30), bind["Age"])
	bind = nil
	err = jsoniter.ConfigFastest.Unmarshal(log.After, &bind)
	assert.NoError(t, err)
	assert.Equal(t, "Test 3", bind["Name"])
	assert.Equal(t, float64(40), bind["Age"])

	DeleteEntity(c, entity)
	err = c.Flush()
	assert.NoError(t, err)
	err = runAsyncConsumer(c, false)
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
	assert.Equal(t, float64(40), bind["Age"])

	entity = NewEntity[logTableEntity](c)
	entity.Name = "Tom"
	entity.Age = 41
	assert.NoError(t, c.Flush())
	err = EditEntityField(c, entity, "Age", 42, true)
	assert.NoError(t, c.Flush())
	assert.NoError(t, runAsyncConsumer(c, false))
	logs = Search[LogEntity[logTableEntity]](c, NewWhere("EntityID = ?", entity.ID), nil)
	assert.Equal(t, 2, logs.Len())
	logs.Next()
	logs.Next()
	log = logs.Entity()
	assert.Equal(t, entity.ID, log.EntityID)
	assert.NotNil(t, log.Meta)
	assert.NotNil(t, log.After)
	assert.NotNil(t, log.Before)
	bind = nil
	err = jsoniter.ConfigFastest.Unmarshal(log.Before, &bind)
	assert.NoError(t, err)
	assert.Len(t, bind, 1)
	assert.Equal(t, float64(41), bind["Age"])
	bind = nil
	err = jsoniter.ConfigFastest.Unmarshal(log.After, &bind)
	assert.NoError(t, err)
	assert.Len(t, bind, 1)
	assert.Equal(t, float64(42), bind["Age"])
}
