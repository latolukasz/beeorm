package beeorm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type updateSubField struct {
	SubName string
	Uint    uint16
}

type updateEntity struct {
	ID     uint64 `orm:"localCache;redisCache"`
	Name   string `orm:"length=10;required"`
	Uint   uint16
	Level1 updateSubField
}

func TestUpdateExecuteNoCache(t *testing.T) {
	testUpdateExecute(t, false, false)
}

func testUpdateExecute(t *testing.T, local, redis bool) {
	var entity *updateEntity
	c := PrepareTables(t, NewRegistry(), entity)

	schema := GetEntitySchema[updateEntity](c)
	schema.DisableCache(!local, !redis)

	var ids []uint64
	for i := 1; i <= 10; i++ {
		entity = NewEntity[updateEntity](c)
		entity.Name = fmt.Sprintf("name %d", i)
		entity.Level1.SubName = fmt.Sprintf("sub name %d", i)
		ids = append(ids, entity.ID)
	}
	err := c.Flush()
	assert.NoError(t, err)

	/* string */

	entity = GetByID[updateEntity](c, ids[0])
	err = UpdateEntityField(c, entity, "Name", "New", true)
	assert.NoError(t, err)
	assert.Equal(t, "New", entity.Name)
	entity = GetByID[updateEntity](c, ids[0])
	assert.Equal(t, "New", entity.Name)

	err = UpdateEntityField(c, entity, "Level1SubName", "Sub New", true)
	assert.NoError(t, err)
	assert.Equal(t, "Sub New", entity.Level1.SubName)
	entity = GetByID[updateEntity](c, ids[0])
	assert.Equal(t, "Sub New", entity.Level1.SubName)

	err = UpdateEntityField(c, entity, "Level1SubName", "", true)
	assert.NoError(t, err)
	assert.Equal(t, "", entity.Level1.SubName)
	entity = GetByID[updateEntity](c, ids[0])
	assert.Equal(t, "", entity.Level1.SubName)

	entity = GetByID[updateEntity](c, ids[1])
	err = UpdateEntityField(c, entity, "Level1SubName", nil, true)
	assert.NoError(t, err)
	assert.Equal(t, "", entity.Level1.SubName)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, "", entity.Level1.SubName)

	err = UpdateEntityField(c, entity, "Name", "123456789ab", true)
	assert.EqualError(t, err, "[Name] text too long, max 10 allowed")

	err = UpdateEntityField(c, entity, "Name", "", true)
	assert.EqualError(t, err, "[Name] empty string not allowed")

	/* uint */
	uintValues := []any{"14", float32(14), float64(14), uint8(14), uint16(14), uint32(14), uint(14), uint64(14), int8(14), int16(14), int32(14), int64(14), 14}
	for _, val := range uintValues {
		err = UpdateEntityField(c, entity, "Uint", val, true)
		assert.NoError(t, err)
		assert.Equal(t, uint16(14), entity.Uint)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, uint16(14), entity.Uint)
	}
	err = UpdateEntityField(c, entity, "Uint", -14, true)
	assert.EqualError(t, err, "[Uint] negative number -14 not allowed")
}
