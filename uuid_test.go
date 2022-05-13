package beeorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type uuidEntity struct {
	ORM  `orm:"uuid"`
	ID   uint64
	Name string `orm:"unique=name;required"`
	Age  int
}

func TestUuid(t *testing.T) {
	registry := &Registry{}
	var entity *uuidEntity
	engine, def := prepareTables(t, registry, 8, "", "2.0", entity)
	defer def()
	schema := engine.GetRegistry().GetTableSchemaForEntity(entity)
	assert.True(t, schema.(*tableSchema).hasUUID)

	id := uuid()
	assert.Greater(t, id, uint64(0))
	id++
	assert.Equal(t, id, uuid())

	entity = &uuidEntity{}
	entity.Name = "test"
	engine.EnableQueryDebug()
	engine.Flush(entity)
	id++
	assert.Equal(t, id, entity.ID)
}
