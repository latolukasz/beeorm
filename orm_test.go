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
	ID uint
}

type ormEntity struct {
	ORM
	ID             uint
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
	engine := prepareTables(t, &Registry{}, 5, entity, &ormEntityRef{})

	entity = &ormEntity{nameUnset: ""}
	id := entity.GetID()
	assert.Equal(t, uint64(0), id)

	err := entity.SetField("Name", "hello")
	assert.EqualError(t, err, "entity is not loaded")

	engine.Load(entity)
	err = entity.SetField("Name", "hello")
	assert.NoError(t, err)
	assert.Equal(t, "hello", entity.Name)

	err = entity.SetField("Name", "NIL")
	assert.NoError(t, err)
	assert.Equal(t, "", entity.Name)

	err = entity.SetField("Invalid", "hello")
	assert.EqualError(t, err, "field Invalid not found")

	err = entity.SetField("nameUnset", "hello")
	assert.EqualError(t, err, "field nameUnset is not public")

	err = entity.SetField("Uint", 23)
	assert.NoError(t, err)
	assert.Equal(t, uint(23), entity.Uint)
	err = entity.SetField("Uint", "hello")
	assert.EqualError(t, err, "Uint value hello not valid")

	err = entity.SetField("UintNullable", 23)
	assert.NoError(t, err)
	valid := uint(23)
	assert.Equal(t, &valid, entity.UintNullable)
	err = entity.SetField("UintNullable", "hello")
	assert.EqualError(t, err, "UintNullable value hello not valid")
	err = entity.SetField("UintNullable", &valid)
	assert.NoError(t, err)
	assert.Equal(t, &valid, entity.UintNullable)
	err = entity.SetField("UintNullable", nil)
	assert.NoError(t, err)
	assert.Nil(t, entity.UintNullable)

	err = entity.SetField("Uint8Nullable", 23)
	assert.NoError(t, err)
	valid2 := uint8(23)
	assert.Equal(t, &valid2, entity.Uint8Nullable)

	err = entity.SetField("Uint16Nullable", 23)
	assert.NoError(t, err)
	valid3 := uint16(23)
	assert.Equal(t, &valid3, entity.Uint16Nullable)

	err = entity.SetField("Uint32Nullable", 23)
	assert.NoError(t, err)
	valid4 := uint32(23)
	assert.Equal(t, &valid4, entity.Uint32Nullable)

	err = entity.SetField("Uint64Nullable", 23)
	assert.NoError(t, err)
	valid5 := uint64(23)
	assert.Equal(t, &valid5, entity.Uint64Nullable)

	err = entity.SetField("Int", 23)
	assert.NoError(t, err)
	assert.Equal(t, 23, entity.Int)
	err = entity.SetField("Int", "hello")
	assert.EqualError(t, err, "Int value hello not valid")

	err = entity.SetField("IntNullable", 23)
	assert.NoError(t, err)
	valid6 := 23
	assert.Equal(t, &valid6, entity.IntNullable)
	err = entity.SetField("IntNullable", "hello")
	assert.EqualError(t, err, "IntNullable value hello not valid")
	err = entity.SetField("IntNullable", &valid6)
	assert.NoError(t, err)
	assert.Equal(t, &valid6, entity.IntNullable)
	err = entity.SetField("IntNullable", nil)
	assert.NoError(t, err)
	assert.Nil(t, entity.IntNullable)

	err = entity.SetField("Int8Nullable", 23)
	assert.NoError(t, err)
	valid7 := int8(23)
	assert.Equal(t, &valid7, entity.Int8Nullable)

	err = entity.SetField("Int16Nullable", 23)
	assert.NoError(t, err)
	valid8 := int16(23)
	assert.Equal(t, &valid8, entity.Int16Nullable)

	err = entity.SetField("Int32Nullable", 23)
	assert.NoError(t, err)
	valid9 := int32(23)
	assert.Equal(t, &valid9, entity.Int32Nullable)

	err = entity.SetField("Int64Nullable", 23)
	assert.NoError(t, err)
	valid10 := int64(23)
	assert.Equal(t, &valid10, entity.Int64Nullable)

	err = entity.SetField("StringSlice", []string{"aaa"})
	assert.NoError(t, err)
	assert.Equal(t, []string{"aaa"}, entity.StringSlice)
	err = entity.SetField("StringSlice", "hello")
	assert.EqualError(t, err, "StringSlice value hello not valid")

	err = entity.SetField("Bytes", []uint8{1})
	assert.NoError(t, err)
	assert.Equal(t, []uint8{1}, entity.Bytes)
	err = entity.SetField("Bytes", "hello")
	assert.EqualError(t, err, "Bytes value hello not valid")

	err = entity.SetField("Bool", true)
	assert.NoError(t, err)
	assert.Equal(t, true, entity.Bool)

	err = entity.SetField("BoolNullable", true)
	assert.NoError(t, err)
	validBool := true
	assert.Equal(t, &validBool, entity.BoolNullable)
	err = entity.SetField("BoolNullable", nil)
	assert.NoError(t, err)
	assert.Nil(t, entity.BoolNullable)

	err = entity.SetField("Float", 23.12)
	assert.NoError(t, err)
	assert.Equal(t, float32(23.12), entity.Float)
	err = entity.SetField("Float", "hello")
	assert.EqualError(t, err, "Float value hello is not valid")

	err = entity.SetField("FloatNullable", 24.11)
	assert.NoError(t, err)
	validFloat := 24.11
	assert.Equal(t, &validFloat, entity.FloatNullable)
	err = entity.SetField("FloatNullable", nil)
	assert.NoError(t, err)
	assert.Nil(t, entity.FloatNullable)
	err = entity.SetField("FloatNullable", "hello")
	assert.EqualError(t, err, "FloatNullable value hello is not valid")

	timeNullable := time.Now()
	err = entity.SetField("TimeNullable", &timeNullable)
	assert.NoError(t, err)
	assert.Equal(t, &timeNullable, entity.TimeNullable)
	err = entity.SetField("TimeNullable", nil)
	assert.NoError(t, err)
	assert.Nil(t, entity.TimeNullable)
	err = entity.SetField("TimeNullable", "hello")
	assert.EqualError(t, err, "TimeNullable value hello is not valid")

	timeNotNull := time.Now()
	err = entity.SetField("Time", timeNotNull)
	assert.NoError(t, err)
	assert.Equal(t, timeNotNull, entity.Time)
	err = entity.SetField("Time", "hello")
	assert.EqualError(t, err, "Time value hello is not valid")

	err = entity.SetField("NotSupported", "hello")
	assert.EqualError(t, err, "field NotSupported is not supported")

	err = entity.SetField("Struct", ormEntityStruct{})
	assert.NoError(t, err)

	err = entity.SetField("StructPtr", "hello")
	assert.EqualError(t, err, "field StructPtr is not supported")

	err = entity.SetField("Slice", []ormEntityStruct{{Name: "Hello"}, {Name: "John"}})
	assert.NoError(t, err)
	assert.Len(t, entity.Slice, 2)
	engine.Flush(entity)

	ref := &ormEntityRef{}
	engine.Flush(ref)

	err = entity.SetField("Ref", ref)
	assert.NoError(t, err)
	assert.Equal(t, ref, entity.Ref)
	err = entity.SetField("Ref", "hello")
	assert.EqualError(t, err, "Ref value hello is not valid")
	err = entity.SetField("Ref", nil)
	assert.NoError(t, err)
	assert.Nil(t, entity.Ref)
	err = entity.SetField("Ref", 0)
	assert.NoError(t, err)
	assert.Nil(t, entity.Ref)
	err = entity.SetField("Ref", 1)
	assert.NoError(t, err)
	assert.NotNil(t, entity.Ref)
	assert.Equal(t, uint(1), entity.Ref.ID)
}
