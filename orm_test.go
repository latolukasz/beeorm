package beeorm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type ormEntityStruct struct {
	Name string
}

type ormEntityRef struct {
	ORM
	ID uint64
}

type ormEntity struct {
	ORM
	ID             uint64
	Name           string
	nameUnset      string
	Uint           uint
	UintNullable   *uint
	Uint8Nullable  *uint8
	Uint16Nullable *uint16
	Uint32Nullable *uint32
	Uint64Nullable *uint64
	Int            int
	IntNullable    *int
	Int8Nullable   *int8
	Int16Nullable  *int16
	Int32Nullable  *int32
	Int64Nullable  *int64
	StringSlice    []string
	Bytes          []uint8
	Bool           bool
	BoolNullable   *bool
	Float          float32
	FloatNullable  *float64
	TimeNullable   *time.Time
	Time           time.Time
	Ref            *ormEntityRef
	NotSupported   map[string]string `orm:"ignore"`
	Struct         ormEntityStruct   `orm:"ignore"`
	StructPtr      *ormEntityStruct  `orm:"ignore"`
	Slice          []ormEntityStruct
}

func TestORM(t *testing.T) {
	var entity *ormEntity
	c := PrepareTables(t, &Registry{}, 5, 6, "", entity, &ormEntityRef{})

	entity = &ormEntity{nameUnset: ""}
	id := entity.ID
	assert.Equal(t, uint64(0), id)

	err := entity.SetField(c, "Name", "hello")
	assert.EqualError(t, err, "entity is not loaded")

	Load(c, entity)
	err = entity.SetField(c, "Name", "hello")
	assert.NoError(t, err)
	assert.Equal(t, "hello", entity.Name)

	err = entity.SetField(c, "Name", "hello")
	assert.NoError(t, err)
	assert.Equal(t, "hello", entity.Name)

	err = entity.SetField(c, "Uint", 2497770)
	assert.NoError(t, err)
	assert.Equal(t, uint(2497770), entity.Uint)

	err = entity.SetField(c, "Uint", float32(2497770))
	assert.NoError(t, err)
	assert.Equal(t, uint(2497770), entity.Uint)

	err = entity.SetField(c, "Uint", float64(2497770))
	assert.NoError(t, err)
	assert.Equal(t, uint(2497770), entity.Uint)

	err = entity.SetField(c, "Invalid", "hello")
	assert.EqualError(t, err, "field Invalid not found")

	err = entity.SetField(c, "nameUnset", "hello")
	assert.EqualError(t, err, "field nameUnset is not public")

	err = entity.SetField(c, "Uint", 23)
	assert.NoError(t, err)
	assert.Equal(t, uint(23), entity.Uint)
	err = entity.SetField(c, "Uint", "hello")
	assert.EqualError(t, err, "Uint value hello not valid")

	err = entity.SetField(c, "UintNullable", 23)
	assert.NoError(t, err)
	valid := uint(23)
	assert.Equal(t, &valid, entity.UintNullable)
	err = entity.SetField(c, "UintNullable", "hello")
	assert.EqualError(t, err, "UintNullable value hello not valid")
	err = entity.SetField(c, "UintNullable", &valid)
	assert.NoError(t, err)
	assert.Equal(t, &valid, entity.UintNullable)
	err = entity.SetField(c, "UintNullable", nil)
	assert.NoError(t, err)
	assert.Nil(t, entity.UintNullable)

	err = entity.SetField(c, "Uint8Nullable", 23)
	assert.NoError(t, err)
	valid2 := uint8(23)
	assert.Equal(t, &valid2, entity.Uint8Nullable)

	err = entity.SetField(c, "Uint16Nullable", 23)
	assert.NoError(t, err)
	valid3 := uint16(23)
	assert.Equal(t, &valid3, entity.Uint16Nullable)

	err = entity.SetField(c, "Uint32Nullable", 23)
	assert.NoError(t, err)
	valid4 := uint32(23)
	assert.Equal(t, &valid4, entity.Uint32Nullable)

	err = entity.SetField(c, "Uint64Nullable", 23)
	assert.NoError(t, err)
	valid5 := uint64(23)
	assert.Equal(t, &valid5, entity.Uint64Nullable)

	err = entity.SetField(c, "Int", 23)
	assert.NoError(t, err)
	assert.Equal(t, 23, entity.Int)
	err = entity.SetField(c, "Int", "hello")
	assert.EqualError(t, err, "Int value hello not valid")

	err = entity.SetField(c, "Int", float32(2497770))
	assert.NoError(t, err)
	assert.Equal(t, 2497770, entity.Int)
	err = entity.SetField(c, "Int", float64(2497770))
	assert.NoError(t, err)
	assert.Equal(t, 2497770, entity.Int)
	err = entity.SetField(c, "Int", float32(-2497770))
	assert.EqualError(t, err, "Int value -2.49777e+06 not valid")
	err = entity.SetField(c, "Int", float64(-2497770))
	assert.EqualError(t, err, "Int value -2.49777e+06 not valid")

	err = entity.SetField(c, "IntNullable", 23)
	assert.NoError(t, err)
	valid6 := 23
	assert.Equal(t, &valid6, entity.IntNullable)
	err = entity.SetField(c, "IntNullable", "hello")
	assert.EqualError(t, err, "IntNullable value hello not valid")
	err = entity.SetField(c, "IntNullable", &valid6)
	assert.NoError(t, err)
	assert.Equal(t, &valid6, entity.IntNullable)
	err = entity.SetField(c, "IntNullable", nil)
	assert.NoError(t, err)
	assert.Nil(t, entity.IntNullable)

	err = entity.SetField(c, "Int8Nullable", 23)
	assert.NoError(t, err)
	valid7 := int8(23)
	assert.Equal(t, &valid7, entity.Int8Nullable)

	err = entity.SetField(c, "Int16Nullable", 23)
	assert.NoError(t, err)
	valid8 := int16(23)
	assert.Equal(t, &valid8, entity.Int16Nullable)

	err = entity.SetField(c, "Int32Nullable", 23)
	assert.NoError(t, err)
	valid9 := int32(23)
	assert.Equal(t, &valid9, entity.Int32Nullable)

	err = entity.SetField(c, "Int64Nullable", 23)
	assert.NoError(t, err)
	valid10 := int64(23)
	assert.Equal(t, &valid10, entity.Int64Nullable)

	err = entity.SetField(c, "StringSlice", []string{"aaa"})
	assert.NoError(t, err)
	assert.Equal(t, []string{"aaa"}, entity.StringSlice)
	err = entity.SetField(c, "StringSlice", "hello")
	assert.EqualError(t, err, "StringSlice value hello not valid")

	err = entity.SetField(c, "Bytes", []uint8{1})
	assert.NoError(t, err)
	assert.Equal(t, []uint8{1}, entity.Bytes)
	err = entity.SetField(c, "Bytes", "hello")
	assert.EqualError(t, err, "Bytes value hello not valid")

	err = entity.SetField(c, "Bool", true)
	assert.NoError(t, err)
	assert.Equal(t, true, entity.Bool)

	err = entity.SetField(c, "BoolNullable", true)
	assert.NoError(t, err)
	validBool := true
	assert.Equal(t, &validBool, entity.BoolNullable)
	err = entity.SetField(c, "BoolNullable", nil)
	assert.NoError(t, err)
	assert.Nil(t, entity.BoolNullable)

	err = entity.SetField(c, "Float", 23.12)
	assert.NoError(t, err)
	assert.Equal(t, float32(23.12), entity.Float)
	err = entity.SetField(c, "Float", "hello")
	assert.EqualError(t, err, "Float value hello is not valid")

	err = entity.SetField(c, "FloatNullable", 24.11)
	assert.NoError(t, err)
	validFloat := 24.11
	assert.Equal(t, &validFloat, entity.FloatNullable)
	err = entity.SetField(c, "FloatNullable", nil)
	assert.NoError(t, err)
	assert.Nil(t, entity.FloatNullable)
	err = entity.SetField(c, "FloatNullable", "hello")
	assert.EqualError(t, err, "FloatNullable value hello is not valid")

	timeNullable := time.Now()
	err = entity.SetField(c, "TimeNullable", &timeNullable)
	assert.NoError(t, err)
	assert.Equal(t, &timeNullable, entity.TimeNullable)
	err = entity.SetField(c, "TimeNullable", nil)
	assert.NoError(t, err)
	assert.Nil(t, entity.TimeNullable)

	err = entity.SetField(c, "TimeNullable", "2022-03-02T13:34:17Z")
	assert.NoError(t, err)
	assert.Equal(t, "2022-03-02 13:34:17", entity.TimeNullable.Format(TimeFormat))

	err = entity.SetField(c, "TimeNullable", "hello")
	assert.EqualError(t, err, "TimeNullable value hello is not valid")

	timeNotNull := time.Now()
	err = entity.SetField(c, "Time", timeNotNull)
	assert.NoError(t, err)
	assert.Equal(t, timeNotNull, entity.Time)

	timeNotNullString := timeNotNull.Format(TimeFormat)
	err = entity.SetField(c, "Time", timeNotNullString)
	assert.NoError(t, err)
	assert.Equal(t, timeNotNull.Format(TimeFormat), entity.Time.Format(TimeFormat))

	err = entity.SetField(c, "Time", "2022-03-02T13:34:17Z")
	assert.NoError(t, err)
	assert.Equal(t, "2022-03-02 13:34:17", entity.Time.Format(TimeFormat))

	err = entity.SetField(c, "Time", "hello")
	assert.EqualError(t, err, "Time value hello is not valid")

	err = entity.SetField(c, "NotSupported", "hello")
	assert.EqualError(t, err, "field NotSupported is not supported")

	err = entity.SetField(c, "Struct", ormEntityStruct{})
	assert.NoError(t, err)

	err = entity.SetField(c, "StructPtr", "hello")
	assert.EqualError(t, err, "field StructPtr is not supported")

	err = entity.SetField(c, "Slice", []ormEntityStruct{{Name: "Hello"}, {Name: "John"}})
	assert.NoError(t, err)
	assert.Len(t, entity.Slice, 2)
	c.Flusher().Track(entity).Flush()

	ref := &ormEntityRef{}
	c.Flusher().Track(ref).Flush()

	err = entity.SetField(c, "Ref", ref)
	assert.NoError(t, err)
	assert.Equal(t, ref, entity.Ref)
	err = entity.SetField(c, "Ref", "hello")
	assert.EqualError(t, err, "Ref value hello is not valid")
	err = entity.SetField(c, "Ref", nil)
	assert.NoError(t, err)
	assert.Nil(t, entity.Ref)
	err = entity.SetField(c, "Ref", 0)
	assert.NoError(t, err)
	assert.Nil(t, entity.Ref)
	err = entity.SetField(c, "Ref", 1)
	assert.NoError(t, err)
	assert.NotNil(t, entity.Ref)
	assert.Equal(t, uint64(1), entity.Ref.GetID())
}
