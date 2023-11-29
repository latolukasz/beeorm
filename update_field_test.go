package beeorm

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

type updateSubField struct {
	SubName      string
	Uint         uint16
	Int          int16
	UintNullable *uint16
	IntNullable  *int16
	Reference    *Reference[updateEntityReference] `orm:"required"`
	Enum         testEnum                          `orm:"required"`
	Set          []testEnum                        `orm:"required"`
}

type updateEntityReference struct {
	ID uint64
}

type updateEntity struct {
	ID            uint64 `orm:"localCache;redisCache"`
	Name          string `orm:"length=10;required"`
	Uint          uint16
	Int           int16
	UintNullable  *uint16
	IntNullable   *int16
	Level1        updateSubField
	Reference     *Reference[updateEntityReference]
	Enum          testEnum
	Set           []testEnum
	Blob          []uint8
	Bool          bool
	BoolNullable  *bool
	Float         float64  `orm:"precision=2"`
	Decimal       float64  `orm:"decimal=5,2;unsigned"`
	FloatNullable *float32 `orm:"precision=2"`
}

func TestUpdateExecuteNoCache(t *testing.T) {
	testUpdateExecute(t, false, false)
}

func testUpdateExecute(t *testing.T, local, redis bool) {
	var entity *updateEntity
	var reference *updateEntityReference
	c := PrepareTables(t, NewRegistry(), entity, reference)

	schema := GetEntitySchema[updateEntity](c)
	schema.DisableCache(!local, !redis)

	var ids []uint64
	for i := 1; i <= 10; i++ {
		entity = NewEntity[updateEntity](c)
		entity.Name = fmt.Sprintf("name %d", i)
		entity.Level1.SubName = fmt.Sprintf("sub name %d", i)
		entity.Level1.Reference = &Reference[updateEntityReference]{ID: 1}
		entity.Level1.Enum = testEnumDefinition.A
		entity.Level1.Set = []testEnum{testEnumDefinition.A}
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
	intValues := []any{"1", float32(2), float64(3), uint8(4), uint16(5), uint32(6), uint(7), uint64(8), int8(9), int16(10), int32(11), int64(12), 13}
	for i, val := range intValues {
		err = UpdateEntityField(c, entity, "Uint", val, true)
		assert.NoError(t, err)
		assert.Equal(t, uint16(i+1), entity.Uint)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, uint16(i+1), entity.Uint)
		err = UpdateEntityField(c, entity, "Level1Uint", val, true)
		assert.NoError(t, err)
		assert.Equal(t, uint16(i+1), entity.Level1.Uint)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, uint16(i+1), entity.Level1.Uint)
	}
	err = UpdateEntityField(c, entity, "Uint", -14, true)
	assert.EqualError(t, err, "[Uint] negative number -14 not allowed")
	err = UpdateEntityField(c, entity, "Uint", math.MaxUint16+1, true)
	assert.EqualError(t, err, "[Uint] value 65536 exceeded max allowed value")
	err = UpdateEntityField(c, entity, "Uint", "invalid", true)
	assert.EqualError(t, err, "[Uint] invalid number invalid")

	/* int */
	for i, val := range intValues {
		err = UpdateEntityField(c, entity, "Int", val, true)
		assert.NoError(t, err)
		assert.Equal(t, int16(i+1), entity.Int)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, int16(i+1), entity.Int)
		err = UpdateEntityField(c, entity, "Level1Int", val, true)
		assert.NoError(t, err)
		assert.Equal(t, int16(i+1), entity.Level1.Int)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, int16(i+1), entity.Level1.Int)
	}
	err = UpdateEntityField(c, entity, "Int", math.MaxInt16+1, true)
	assert.EqualError(t, err, "[Int] value 32768 exceeded max allowed value")
	err = UpdateEntityField(c, entity, "Int", math.MinInt16-1, true)
	assert.EqualError(t, err, "[Int] value -32769 exceeded min allowed value")
	err = UpdateEntityField(c, entity, "Int", "invalid", true)
	assert.EqualError(t, err, "[Int] invalid number invalid")

	/* *uint */
	for i, val := range intValues {
		err = UpdateEntityField(c, entity, "UintNullable", val, true)
		assert.NoError(t, err)
		assert.Equal(t, uint16(i+1), *entity.UintNullable)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, uint16(i+1), *entity.UintNullable)
		err = UpdateEntityField(c, entity, "Level1UintNullable", val, true)
		assert.NoError(t, err)
		assert.Equal(t, uint16(i+1), *entity.Level1.UintNullable)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, uint16(i+1), *entity.Level1.UintNullable)
	}
	err = UpdateEntityField(c, entity, "UintNullable", nil, true)
	assert.NoError(t, err)
	assert.Nil(t, entity.UintNullable)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Nil(t, entity.UintNullable)

	/* *int */
	for i, val := range intValues {
		err = UpdateEntityField(c, entity, "IntNullable", val, true)
		assert.NoError(t, err)
		assert.Equal(t, int16(i+1), *entity.IntNullable)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, int16(i+1), *entity.IntNullable)
		err = UpdateEntityField(c, entity, "Level1IntNullable", val, true)
		assert.NoError(t, err)
		assert.Equal(t, int16(i+1), *entity.Level1.IntNullable)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, int16(i+1), *entity.Level1.IntNullable)
	}
	err = UpdateEntityField(c, entity, "IntNullable", nil, true)
	assert.NoError(t, err)
	assert.Nil(t, entity.IntNullable)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Nil(t, entity.IntNullable)

	/* reference */
	for i, val := range intValues {
		err = UpdateEntityField(c, entity, "Reference", val, true)
		assert.NoError(t, err)
		assert.Equal(t, uint64(i+1), entity.Reference.ID)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, uint64(i+1), entity.Reference.ID)
		err = UpdateEntityField(c, entity, "Level1Reference", val, true)
		assert.NoError(t, err)
		assert.Equal(t, uint64(i+1), entity.Level1.Reference.ID)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, uint64(i+1), entity.Level1.Reference.ID)
	}
	err = UpdateEntityField(c, entity, "Reference", &Reference[updateEntityReference]{ID: 20}, true)
	assert.NoError(t, err)
	assert.Equal(t, uint64(20), entity.Reference.ID)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, uint64(20), entity.Reference.ID)
	err = UpdateEntityField(c, entity, "Reference", &Reference[updateEntityReference]{ID: 0}, true)
	assert.NoError(t, err)
	assert.Nil(t, entity.Reference)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Nil(t, entity.Reference)
	_ = UpdateEntityField(c, entity, "Reference", 20, true)
	err = UpdateEntityField(c, entity, "Reference", nil, true)
	assert.NoError(t, err)
	assert.Nil(t, entity.Reference)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Nil(t, entity.Reference)
	_ = UpdateEntityField(c, entity, "Reference", 20, true)
	err = UpdateEntityField(c, entity, "Reference", 0, true)
	assert.NoError(t, err)
	assert.Nil(t, entity.Reference)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Nil(t, entity.Reference)
	err = UpdateEntityField(c, entity, "Reference", "invalid", true)
	assert.EqualError(t, err, "[Reference] invalid number invalid")

	/* enum */
	err = UpdateEntityField(c, entity, "Enum", testEnumDefinition.B, true)
	assert.NoError(t, err)
	assert.Equal(t, testEnumDefinition.B, entity.Enum)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, testEnumDefinition.B, entity.Enum)
	err = UpdateEntityField(c, entity, "Enum", "c", true)
	assert.NoError(t, err)
	assert.Equal(t, testEnumDefinition.C, entity.Enum)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, testEnumDefinition.C, entity.Enum)
	err = UpdateEntityField(c, entity, "Enum", "", true)
	assert.NoError(t, err)
	assert.Equal(t, testEnum(""), entity.Enum)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, testEnum(""), entity.Enum)
	_ = UpdateEntityField(c, entity, "Enum", testEnumDefinition.B, true)
	err = UpdateEntityField(c, entity, "Enum", nil, true)
	assert.NoError(t, err)
	assert.Equal(t, testEnum(""), entity.Enum)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, testEnum(""), entity.Enum)
	err = UpdateEntityField(c, entity, "Enum", "invalid", true)
	assert.EqualError(t, err, "[Enum] invalid value: invalid")
	err = UpdateEntityField(c, entity, "Level1Enum", nil, true)
	assert.EqualError(t, err, "[Level1Enum] nil is not allowed")
	err = UpdateEntityField(c, entity, "Level1Enum", "", true)
	assert.EqualError(t, err, "[Level1Enum] nil is not allowed")

	/* set */
	err = UpdateEntityField(c, entity, "Set", testEnumDefinition.B, true)
	assert.NoError(t, err)
	assert.Equal(t, []testEnum{testEnumDefinition.B}, entity.Set)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, []testEnum{testEnumDefinition.B}, entity.Set)
	err = UpdateEntityField(c, entity, "Set", []testEnum{testEnumDefinition.A, testEnumDefinition.C}, true)
	assert.NoError(t, err)
	assert.Equal(t, []testEnum{testEnumDefinition.A, testEnumDefinition.C}, entity.Set)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, []testEnum{testEnumDefinition.A, testEnumDefinition.C}, entity.Set)
	err = UpdateEntityField(c, entity, "Set", nil, true)
	assert.NoError(t, err)
	assert.Nil(t, entity.Set)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Nil(t, entity.Set)
	_ = UpdateEntityField(c, entity, "Set", testEnumDefinition.B, true)
	err = UpdateEntityField(c, entity, "Set", []testEnum{}, true)
	assert.NoError(t, err)
	assert.Nil(t, entity.Set)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Nil(t, entity.Set)
	err = UpdateEntityField(c, entity, "Set", "invalid", true)
	assert.EqualError(t, err, "[Set] invalid value: invalid")
	err = UpdateEntityField(c, entity, "Level1Set", "", true)
	assert.EqualError(t, err, "[Level1Set] nil is not allowed")

	/* byte */
	err = UpdateEntityField(c, entity, "Blob", "hello", true)
	assert.NoError(t, err)
	assert.Equal(t, "hello", string(entity.Blob))
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, "hello", string(entity.Blob))
	err = UpdateEntityField(c, entity, "Blob", []byte("hello 2"), true)
	assert.NoError(t, err)
	assert.Equal(t, "hello 2", string(entity.Blob))
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, "hello 2", string(entity.Blob))
	err = UpdateEntityField(c, entity, "Blob", nil, true)
	assert.NoError(t, err)
	assert.Nil(t, entity.Blob)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Nil(t, entity.Blob)

	/* boolean */
	err = UpdateEntityField(c, entity, "Bool", true, true)
	assert.NoError(t, err)
	assert.True(t, entity.Bool)
	entity = GetByID[updateEntity](c, ids[1])
	assert.True(t, entity.Bool)
	err = UpdateEntityField(c, entity, "Bool", false, true)
	assert.NoError(t, err)
	assert.False(t, entity.Bool)
	entity = GetByID[updateEntity](c, ids[1])
	assert.False(t, entity.Bool)
	err = UpdateEntityField(c, entity, "Bool", 1, true)
	assert.NoError(t, err)
	assert.True(t, entity.Bool)
	entity = GetByID[updateEntity](c, ids[1])
	assert.True(t, entity.Bool)
	_ = UpdateEntityField(c, entity, "Bool", false, true)
	err = UpdateEntityField(c, entity, "Bool", "true", true)
	assert.NoError(t, err)
	assert.True(t, entity.Bool)
	entity = GetByID[updateEntity](c, ids[1])
	assert.True(t, entity.Bool)
	err = UpdateEntityField(c, entity, "Bool", []string{}, true)
	assert.EqualError(t, err, "[Bool] invalid value")

	/* *boolean */
	err = UpdateEntityField(c, entity, "BoolNullable", true, true)
	assert.NoError(t, err)
	assert.True(t, *entity.BoolNullable)
	entity = GetByID[updateEntity](c, ids[1])
	assert.True(t, *entity.BoolNullable)
	err = UpdateEntityField(c, entity, "BoolNullable", nil, true)
	assert.NoError(t, err)
	assert.Nil(t, entity.BoolNullable)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Nil(t, entity.BoolNullable)
	err = UpdateEntityField(c, entity, "BoolNullable", []string{}, true)
	assert.EqualError(t, err, "[BoolNullable] invalid value")

	/* float */
	for i, val := range intValues {
		err = UpdateEntityField(c, entity, "Float", val, true)
		assert.NoError(t, err)
		assert.Equal(t, float64(i+1), entity.Float)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, float64(i+1), entity.Float)
	}
	err = UpdateEntityField(c, entity, "Float", 12.13, true)
	assert.NoError(t, err)
	assert.Equal(t, 12.13, entity.Float)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, 12.13, entity.Float)
	err = UpdateEntityField(c, entity, "Float", "12.14", true)
	assert.NoError(t, err)
	assert.Equal(t, 12.14, entity.Float)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, 12.14, entity.Float)
	err = UpdateEntityField(c, entity, "Float", 12.136, true)
	assert.NoError(t, err)
	assert.Equal(t, 12.14, entity.Float)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, 12.14, entity.Float)
	err = UpdateEntityField(c, entity, "Decimal", -1, true)
	assert.EqualError(t, err, "[Decimal] negative number -1 not allowed")
	err = UpdateEntityField(c, entity, "Decimal", 1234.45, true)
	assert.EqualError(t, err, "[Decimal] decimal size too big, max 3 allowed")
	err = UpdateEntityField(c, entity, "Decimal", "invalid", true)
	assert.EqualError(t, err, "[Decimal] invalid number invalid")

	/* *float */
	for i, val := range intValues {
		err = UpdateEntityField(c, entity, "FloatNullable", val, true)
		assert.NoError(t, err)
		assert.Equal(t, float32(i+1), *entity.FloatNullable)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, float32(i+1), *entity.FloatNullable)
	}
	err = UpdateEntityField(c, entity, "FloatNullable", "invalid", true)
	assert.EqualError(t, err, "[FloatNullable] invalid number invalid")
}
