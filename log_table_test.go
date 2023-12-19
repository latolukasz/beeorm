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
	orm := PrepareTables(t, NewRegistry(), entity, LogEntity[logTableEntity]{})
	assert.NotNil(t, orm)

	entity = NewEntity[logTableEntity](orm)
	entity.Name = "Test"
	entity.Age = 18
	err := orm.Flush()
	assert.NoError(t, err)
	err = runAsyncConsumer(orm, false)
	assert.Nil(t, err)

	logs := Search[LogEntity[logTableEntity]](orm, NewWhere("1"), nil)
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

	orm.SetMetaData("source", "test case")
	entity = NewEntity[logTableEntity](orm)
	entity.Name = "Test 2"
	entity.Age = 30
	err = orm.Flush()
	assert.NoError(t, err)
	err = runAsyncConsumer(orm, false)
	assert.Nil(t, err)

	logs = Search[LogEntity[logTableEntity]](orm, NewWhere("1"), nil)
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

	entity = EditEntity(orm, entity)
	entity.Name = "Test 3"
	entity.Age = 40
	err = orm.Flush()
	assert.NoError(t, err)
	err = runAsyncConsumer(orm, false)
	assert.NoError(t, err)
	logs = Search[LogEntity[logTableEntity]](orm, NewWhere("1"), nil)
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

	DeleteEntity(orm, entity)
	err = orm.Flush()
	assert.NoError(t, err)
	err = runAsyncConsumer(orm, false)
	assert.NoError(t, err)
	logs = Search[LogEntity[logTableEntity]](orm, NewWhere("1"), nil)
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

	entity = NewEntity[logTableEntity](orm)
	entity.Name = "Tom"
	entity.Age = 41
	assert.NoError(t, orm.Flush())
	err = EditEntityField(orm, entity, "Age", 42)
	assert.NoError(t, err)
	assert.NoError(t, orm.Flush())
	assert.NoError(t, runAsyncConsumer(orm, false))
	logs = Search[LogEntity[logTableEntity]](orm, NewWhere("EntityID = ?", entity.ID), nil)
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
