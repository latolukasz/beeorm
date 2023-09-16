package beeorm

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

type flushStruct struct {
	Name2    string
	Age      int
	Sub      flushSubStruct
	TestTime *time.Time `orm:"time=true"`
}

type flushSubStruct struct {
	Name3 string
	Age3  int
}

type flushStructAnonymous struct {
	SubName string
	SubAge  float32 `orm:"decimal=9,5;unsigned=false"`
}

type testEnum Enum
type testSet []testEnum

func (s testEnum) EnumValues() interface{} {
	return testEnumDefinition
}
func (s testSet) EnumValues() interface{} {
	return testEnumDefinition
}

var testEnumDefinition = struct {
	A testEnum
	B testEnum
	C testEnum
}{
	A: "a",
	B: "b",
	C: "c",
}

type flushEntity struct {
	ID                   uint64 `orm:"localCache;redisCache"`
	City                 string `orm:"unique=city;length=40"`
	Name                 string `orm:"unique=name;required"`
	Age                  int
	Uint                 uint
	UintNullable         *uint
	IntNullable          *int
	BoolNullable         *bool
	FloatNullable        *float64 `orm:"precision=3"`
	Float32Nullable      *float32 `orm:"precision=4"`
	SetNullable          testSet
	SetNotNull           testSet `orm:"required"`
	EnumNullable         testEnum
	EnumNotNull          testEnum `orm:"required"`
	Ignored              []string `orm:"ignore"`
	Blob                 []uint8
	Bool                 bool
	Float64              float64  `orm:"precision=5"`
	Decimal              float64  `orm:"decimal=5,2"`
	DecimalNullable      *float64 `orm:"decimal=5,2"`
	Float64Unsigned      float64  `orm:"unsigned"`
	Float64Signed        float64
	Time                 time.Time
	TimeWithTime         time.Time `orm:"time"`
	TimeNullable         *time.Time
	TimeWithTimeNullable *time.Time `orm:"time"`
	FlushStruct          flushStruct
	Int8Nullable         *int8
	Int16Nullable        *int16
	Int32Nullable        *int32
	Int64Nullable        *int64
	Uint8Nullable        *uint8
	Uint16Nullable       *uint16
	Uint32Nullable       *uint32
	Uint64Nullable       *uint64
	flushStructAnonymous
}

func (e *flushEntity) GetID() uint64 {
	return e.ID
}

func TestFlushLocalRedis(t *testing.T) {
	testFlush(t, true, true)
}

func TestFlushLocal(t *testing.T) {
	testFlush(t, true, false)
}

func TestFlushNoCache(t *testing.T) {
	testFlush(t, false, false)
}

func TestFlushRedis(t *testing.T) {
	testFlush(t, false, true)
}

func testFlush(t *testing.T, local bool, redis bool) {
	registry := &Registry{}
	c := PrepareTables(t, registry, "", &flushEntity{})

	schema := GetEntitySchema[*flushEntity](c)
	schema.DisableCache(!local, !redis)

	// Adding empty entity
	newEntity := NewEntity[*flushEntity](c).TrackedEntity()
	assert.NotEmpty(t, newEntity.ID)
	assert.NoError(t, c.Flush())

	entity := GetByID[*flushEntity](c, newEntity.ID)
	assert.NotNil(t, entity)
	assert.Equal(t, newEntity.ID, entity.ID)
	assert.Equal(t, "", entity.City)
	assert.Equal(t, "", entity.Name)
	assert.Equal(t, 0, entity.Age)
	assert.Equal(t, uint(0), entity.Uint)
	assert.Nil(t, entity.UintNullable)
	assert.Nil(t, entity.IntNullable)
	assert.Nil(t, entity.BoolNullable)
	assert.Nil(t, entity.FloatNullable)
	assert.Nil(t, entity.Float32Nullable)
	assert.Nil(t, entity.SetNullable)
	assert.Equal(t, testSet{testEnumDefinition.A}, entity.SetNotNull)
	assert.Equal(t, testEnum(""), entity.EnumNullable)
	assert.Equal(t, testEnumDefinition.A, entity.EnumNotNull)
	assert.Nil(t, entity.Blob)
	assert.False(t, entity.Bool)
	assert.Equal(t, 0.0, entity.Float64)
	assert.Equal(t, 0.0, entity.Decimal)
	assert.Nil(t, entity.DecimalNullable)
	assert.Equal(t, 0.0, entity.Float64Unsigned)
	assert.Equal(t, 0.0, entity.Float64Signed)
	assert.Equal(t, new(time.Time).UTC(), entity.Time)
	assert.Equal(t, new(time.Time).UTC(), entity.TimeWithTime)
	assert.Nil(t, entity.TimeNullable)
	assert.Nil(t, entity.TimeWithTimeNullable)
	assert.Equal(t, "", entity.FlushStruct.Name2)
	assert.Equal(t, 0, entity.FlushStruct.Age)
	assert.Equal(t, "", entity.FlushStruct.Sub.Name3)
	assert.Equal(t, 0, entity.FlushStruct.Sub.Age3)
	assert.Nil(t, entity.FlushStruct.TestTime)
	assert.Nil(t, entity.Int8Nullable)
	assert.Nil(t, entity.Int16Nullable)
	assert.Nil(t, entity.Int32Nullable)
	assert.Nil(t, entity.Int64Nullable)
	assert.Nil(t, entity.Uint8Nullable)
	assert.Nil(t, entity.Uint16Nullable)
	assert.Nil(t, entity.Uint32Nullable)
	assert.Nil(t, entity.Uint64Nullable)
	assert.Equal(t, "", entity.SubName)
	assert.Equal(t, float32(0), entity.SubAge)

	// Adding full entity
	newEntity = NewEntity[*flushEntity](c).TrackedEntity()
	newEntity.City = "New York"
	newEntity.Name = "Test name"
	newEntity.Age = -19
	newEntity.Uint = 134
	uintNullable := uint(23)
	newEntity.UintNullable = &uintNullable
	intNullable := -45
	newEntity.IntNullable = &intNullable
	boolNullable := true
	newEntity.BoolNullable = &boolNullable
	floatNullable := 12.23
	newEntity.FloatNullable = &floatNullable
	float32Nullable := float32(12.24)
	newEntity.Float32Nullable = &float32Nullable
	newEntity.SetNullable = testSet{testEnumDefinition.B, testEnumDefinition.C}
	newEntity.SetNotNull = testSet{testEnumDefinition.A, testEnumDefinition.C}
	newEntity.EnumNullable = testEnumDefinition.C
	newEntity.EnumNotNull = testEnumDefinition.A
	newEntity.Blob = []byte("test binary")
	newEntity.Bool = true
	newEntity.Float64 = 986.2322
	newEntity.Decimal = 78.24
	decimalNullable := 123.23
	newEntity.DecimalNullable = &decimalNullable
	newEntity.Float64Unsigned = 8932.299423
	newEntity.Float64Signed = -352.120321
	newEntity.Time = time.Date(2023, 11, 12, 22, 12, 34, 0, time.UTC)
	newEntity.TimeWithTime = time.Date(2023, 8, 16, 12, 23, 11, 0, time.UTC)
	timeNullable := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	newEntity.TimeNullable = &timeNullable
	timeWithTimeNullable := time.Date(2025, 11, 4, 21, 0, 5, 6, time.UTC)
	newEntity.TimeWithTimeNullable = &timeWithTimeNullable
	newEntity.FlushStruct.Name2 = "Tom"
	newEntity.FlushStruct.Age = 23
	newEntity.FlushStruct.Sub.Name3 = "Zoya"
	newEntity.FlushStruct.Sub.Age3 = 18
	testTime := time.Date(1982, 11, 4, 21, 0, 5, 6, time.UTC)
	newEntity.FlushStruct.TestTime = &testTime
	int8Nullable := int8(23)
	newEntity.Int8Nullable = &int8Nullable
	int16Nullable := int16(-29)
	newEntity.Int16Nullable = &int16Nullable
	int32Nullable := int32(-2923)
	newEntity.Int32Nullable = &int32Nullable
	int64Nullable := int64(98872)
	newEntity.Int64Nullable = &int64Nullable
	uint8Nullable := uint8(23)
	newEntity.Uint8Nullable = &uint8Nullable
	uint16Nullable := uint16(29)
	newEntity.Uint16Nullable = &uint16Nullable
	uint32Nullable := uint32(2923)
	newEntity.Uint32Nullable = &uint32Nullable
	uint64Nullable := uint64(98872)
	newEntity.Uint64Nullable = &uint64Nullable
	newEntity.SubName = "sub name"
	newEntity.SubAge = 123
	assert.NoError(t, c.Flush())
	entity = GetByID[*flushEntity](c, newEntity.ID)
	assert.NotNil(t, entity)
	assert.Equal(t, newEntity.ID, entity.ID)
	assert.Equal(t, "New York", entity.City)
	assert.Equal(t, "Test name", entity.Name)
	assert.Equal(t, -19, entity.Age)
	assert.Equal(t, uint(134), entity.Uint)
	assert.Equal(t, uint(23), *entity.UintNullable)
	assert.Equal(t, -45, *entity.IntNullable)
	assert.True(t, *entity.BoolNullable)
	assert.Equal(t, 12.23, *entity.FloatNullable)
	assert.Equal(t, float32(12.24), *entity.Float32Nullable)
	assert.Equal(t, testSet{testEnumDefinition.B, testEnumDefinition.C}, entity.SetNullable)
	assert.Equal(t, testSet{testEnumDefinition.A, testEnumDefinition.C}, entity.SetNotNull)
	assert.Equal(t, testEnumDefinition.C, entity.EnumNullable)
	assert.Equal(t, testEnumDefinition.A, entity.EnumNotNull)
	assert.Equal(t, []byte("test binary"), entity.Blob)
	assert.True(t, entity.Bool)
	assert.Equal(t, 986.2322, entity.Float64)
	assert.Equal(t, 78.24, entity.Decimal)
	assert.Equal(t, 123.23, *entity.DecimalNullable)
	assert.Equal(t, 8932.299423, entity.Float64Unsigned)
	assert.Equal(t, -352.120321, entity.Float64Signed)
	assert.Equal(t, time.Date(2023, 11, 12, 0, 0, 0, 0, time.UTC), entity.Time)
	assert.Equal(t, time.Date(2023, 8, 16, 12, 23, 11, 0, time.UTC), entity.TimeWithTime)
	assert.Equal(t, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), *entity.TimeNullable)
	assert.Equal(t, time.Date(2025, 11, 4, 21, 0, 5, 0, time.UTC), *entity.TimeWithTimeNullable)
	assert.Equal(t, "Tom", entity.FlushStruct.Name2)
	assert.Equal(t, 23, entity.FlushStruct.Age)
	assert.Equal(t, "Zoya", entity.FlushStruct.Sub.Name3)
	assert.Equal(t, 18, entity.FlushStruct.Sub.Age3)
	assert.Equal(t, time.Date(1982, 11, 4, 21, 0, 5, 0, time.UTC), *entity.FlushStruct.TestTime)
	assert.Equal(t, int8(23), *entity.Int8Nullable)
	assert.Equal(t, int16(-29), *entity.Int16Nullable)
	assert.Equal(t, int32(-2923), *entity.Int32Nullable)
	assert.Equal(t, int64(98872), *entity.Int64Nullable)
	assert.Equal(t, uint8(23), *entity.Uint8Nullable)
	assert.Equal(t, uint16(29), *entity.Uint16Nullable)
	assert.Equal(t, uint32(2923), *entity.Uint32Nullable)
	assert.Equal(t, uint64(98872), *entity.Uint64Nullable)
	assert.Equal(t, "sub name", entity.SubName)
	assert.Equal(t, float32(123), entity.SubAge)

	// rounding dates
	newEntity = NewEntity[*flushEntity](c).TrackedEntity()
	newEntity.Name = "rounding dates"
	newEntity.City = "rounding dates"
	newEntity.Time = time.Date(2023, 11, 12, 22, 12, 34, 4, time.UTC)
	newEntity.TimeWithTime = time.Date(2023, 8, 16, 12, 23, 11, 6, time.UTC)
	timeNullable = time.Date(2023, 11, 12, 22, 12, 34, 4, time.UTC)
	newEntity.TimeNullable = &timeNullable
	timeWithTimeNullable = time.Date(2023, 8, 16, 12, 23, 11, 6, time.UTC)
	newEntity.TimeWithTimeNullable = &timeWithTimeNullable
	assert.NoError(t, c.Flush())
	assert.Equal(t, time.Date(2023, 11, 12, 0, 0, 0, 0, time.UTC), newEntity.Time)
	assert.Equal(t, time.Date(2023, 8, 16, 12, 23, 11, 0, time.UTC), newEntity.TimeWithTime)
	assert.Equal(t, time.Date(2023, 11, 12, 0, 0, 0, 0, time.UTC), *newEntity.TimeNullable)
	assert.Equal(t, time.Date(2023, 8, 16, 12, 23, 11, 0, time.UTC), *newEntity.TimeWithTimeNullable)

	// rounding floats
	newEntity = NewEntity[*flushEntity](c).TrackedEntity()
	newEntity.Name = "rounding floats"
	newEntity.City = "rounding floats"
	newEntity.Float64 = 1.123456
	newEntity.Decimal = 1.123
	floatNullable = 1.1234
	newEntity.FloatNullable = &floatNullable
	decimalNullable = 1.126
	newEntity.DecimalNullable = &decimalNullable
	assert.NoError(t, c.Flush())
	assert.Equal(t, 1.12346, newEntity.Float64)
	assert.Equal(t, 1.12, newEntity.Decimal)
	assert.Equal(t, 1.123, *newEntity.FloatNullable)
	assert.Equal(t, 1.13, *newEntity.DecimalNullable)

	// invalid values

	// string too long
	newEntity = NewEntity[*flushEntity](c).TrackedEntity()
	newEntity.Name = strings.Repeat("a", 256)
	err := c.Flush()
	assert.EqualError(t, err, "[Name] text too long, max 255 allowed")
	assert.Equal(t, "Name", err.(*BindError).Field)
	err = c.Flush()
	assert.EqualError(t, err, "[Name] text too long, max 255 allowed")
	c.ClearFlush()
	assert.NoError(t, c.Flush())
	newEntity = NewEntity[*flushEntity](c).TrackedEntity()
	newEntity.City = strings.Repeat("a", 41)
	err = c.Flush()
	assert.EqualError(t, err, "[City] text too long, max 40 allowed")
	newEntity.City = strings.Repeat("a", 40)
	newEntity.Name = "String to long"
	assert.NoError(t, c.Flush())

	// invalid decimal
	newEntity = NewEntity[*flushEntity](c).TrackedEntity()
	newEntity.Name = "Invalid decimal"
	newEntity.City = "Invalid decimal"
	newEntity.Decimal = 1234
	err = c.Flush()
	assert.EqualError(t, err, "[Decimal] decimal size too big, max 3 allowed")
	assert.Equal(t, "Decimal", err.(*BindError).Field)
	c.ClearFlush()

	// TODO invalid decimal nullable

	// float signed
	newEntity = NewEntity[*flushEntity](c).TrackedEntity()
	newEntity.Name = "Float signed"
	newEntity.City = "Float signed"
	newEntity.Float64Unsigned = -1
	err = c.Flush()
	assert.EqualError(t, err, "[Decimal] decimal size too big, max 3 allowed")
	assert.Equal(t, "Decimal", err.(*BindError).Field)
	c.ClearFlush()
}
