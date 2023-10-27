package beeorm

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

type testEnum string

func (s testEnum) EnumValues() any {
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
	ID                        uint64 `orm:"localCache;redisCache"`
	City                      string `orm:"unique=city;length=40"`
	Name                      string `orm:"unique=name;required"`
	StringArray               [2]string
	Age                       int
	IntArray                  [2]int
	Uint                      uint
	UintArray                 [2]uint
	UintNullable              *uint
	UintNullableArray         [2]*uint
	IntNullable               *int
	IntNullableArray          [2]*int
	BoolNullable              *bool
	BoolNullableArray         [2]*bool
	FloatNullable             *float64    `orm:"precision=3;unsigned"`
	FloatNullableArray        [2]*float64 `orm:"precision=3;unsigned"`
	Float32Nullable           *float32    `orm:"precision=4"`
	Float32NullableArray      [2]*float32 `orm:"precision=4"`
	SetNullable               []testEnum
	SetNullableArray          [2][]testEnum
	SetNotNull                []testEnum `orm:"required"`
	EnumNullable              testEnum
	EnumNullableArray         [2]testEnum
	EnumNotNull               testEnum `orm:"required"`
	Ignored                   []string `orm:"ignore"`
	Blob                      []uint8
	BlobArray                 [2][]uint8
	Bool                      bool
	BoolArray                 [2]bool
	Float64                   float64     `orm:"precision=5"`
	Float64Array              [2]float64  `orm:"precision=5"`
	Float32                   float32     `orm:"precision=5"`
	Float32Array              [2]float32  `orm:"precision=5"`
	Decimal                   float64     `orm:"decimal=5,2"`
	DecimalArray              [2]float64  `orm:"decimal=5,2"`
	DecimalNullable           *float64    `orm:"decimal=5,2"`
	DecimalNullableArray      [2]*float64 `orm:"decimal=5,2"`
	Float64Unsigned           float64     `orm:"unsigned"`
	Float64UnsignedArray      [2]float64  `orm:"unsigned"`
	Float64Signed             float64
	Float64SignedArray        [2]float64
	Time                      time.Time
	TimeArray                 [2]time.Time
	TimeWithTime              time.Time    `orm:"time"`
	TimeWithTimeArray         [2]time.Time `orm:"time"`
	TimeNullable              *time.Time
	TimeNullableArray         [2]*time.Time
	TimeWithTimeNullable      *time.Time    `orm:"time"`
	TimeWithTimeNullableArray [2]*time.Time `orm:"time"`
	FlushStruct               flushStruct
	FlushStructArray          [2]flushStruct
	Int8Nullable              *int8
	Int16Nullable             *int16
	Int32Nullable             *int32
	Int64Nullable             *int64
	Uint8Nullable             *uint8
	Uint16Nullable            *uint16
	Uint32Nullable            *uint32
	Uint32NullableArray       [2]*uint32
	Uint64Nullable            *uint64
	Reference                 *Reference[flushEntityReference]
	ReferenceArray            [2]*Reference[flushEntityReference]
	ReferenceRequired         *Reference[flushEntityReference] `orm:"required"`
	flushStructAnonymous
}

type flushEntityReference struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string `orm:"required"`
}

func TestFlushInsertLocalRedis(t *testing.T) {
	testFlushInsert(t, false, true, true)
}

func TestFlushLazyInsertLocalRedis(t *testing.T) {
	testFlushInsert(t, true, true, true)
}

func TestFlushInsertLocal(t *testing.T) {
	testFlushInsert(t, false, true, false)
}

func TestFlushLazyInsertLocal(t *testing.T) {
	testFlushInsert(t, true, true, false)
}

func TestFlushInsertNoCache(t *testing.T) {
	testFlushInsert(t, false, false, false)
}

func TestFlushLazyInsertNoCache(t *testing.T) {
	testFlushInsert(t, true, false, false)
}

func TestFlushInsertRedis(t *testing.T) {
	testFlushInsert(t, false, false, true)
}

func TestFlushLazyInsertRedis(t *testing.T) {
	testFlushInsert(t, true, false, true)
}

func TestFlushDeleteLocalRedis(t *testing.T) {
	testFlushDelete(t, false, true, true)
}

func TestFlushDeleteLocal(t *testing.T) {
	testFlushDelete(t, false, true, false)
}

func TestFlushDeleteNoCache(t *testing.T) {
	testFlushDelete(t, false, false, false)
}

func TestFlushLazyDeleteNoCache(t *testing.T) {
	testFlushDelete(t, true, false, false)
}

func TestFlushDeleteRedis(t *testing.T) {
	testFlushDelete(t, false, false, true)
}

func TestFlushUpdateLocalRedis(t *testing.T) {
	testFlushUpdate(t, false, true, true)
}

func TestFlushUpdateLocal(t *testing.T) {
	testFlushUpdate(t, false, true, false)
}

func TestFlushUpdateNoCache(t *testing.T) {
	testFlushUpdate(t, false, false, false)
}

func TestFlushUpdateRedis(t *testing.T) {
	testFlushUpdate(t, false, false, true)
}

func TestFlushUpdateUpdateLocalRedis(t *testing.T) {
	testFlushUpdate(t, true, true, true)
}

func TestFlushUpdateUpdateLocal(t *testing.T) {
	testFlushUpdate(t, true, true, false)
}

func TestFlushUpdateUpdateNoCache(t *testing.T) {
	testFlushUpdate(t, true, false, false)
}

func TestFlushUpdateUpdateRedis(t *testing.T) {
	testFlushUpdate(t, true, false, true)
}

func testFlushInsert(t *testing.T, lazy, local, redis bool) {
	r := NewRegistry()
	c := PrepareTables(t, r, flushEntity{}, flushEntityReference{})

	schema := GetEntitySchema[flushEntity](c)
	schema.DisableCache(!local, !redis)

	reference := NewEntity[flushEntityReference](c)
	reference.Name = "test reference"
	err := c.Flush(lazy)
	assert.NoError(t, err)

	loggerDB := &MockLogHandler{}
	c.RegisterQueryLogger(loggerDB, true, false, false)
	loggerLocal := &MockLogHandler{}
	c.RegisterQueryLogger(loggerLocal, false, false, true)
	loggerRedis := &MockLogHandler{}
	c.RegisterQueryLogger(loggerRedis, false, true, false)

	// Adding empty entity
	newEntity := NewEntity[flushEntity](c)
	newEntity.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	newEntity.Name = "Name"
	assert.NotEmpty(t, newEntity.ID)
	firstEntityID := newEntity.ID
	assert.NoError(t, c.Flush(lazy))
	loggerDB.Clear()

	entity := GetByID[flushEntity](c, newEntity.ID)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
		assert.NotNil(t, entity)
	} else if lazy {
		assert.Nil(t, entity)
		err = ConsumeLazyFlushEvents(c, false)
		assert.NoError(t, err)
		entity = GetByID[flushEntity](c, newEntity.ID)
		assert.NotNil(t, entity)
	}

	assert.Equal(t, newEntity.ID, entity.ID)
	assert.Equal(t, "", entity.City)
	assert.Equal(t, "Name", entity.Name)
	assert.Equal(t, 0, entity.Age)
	assert.Equal(t, uint(0), entity.Uint)
	assert.Nil(t, entity.UintNullable)
	assert.Nil(t, entity.IntNullable)
	assert.Nil(t, entity.BoolNullable)
	assert.Nil(t, entity.FloatNullable)
	assert.Nil(t, entity.Float32Nullable)
	assert.Nil(t, entity.SetNullable)
	assert.Equal(t, []testEnum{testEnumDefinition.A}, entity.SetNotNull)
	assert.Equal(t, testEnum(""), entity.EnumNullable)
	assert.Equal(t, testEnumDefinition.A, entity.EnumNotNull)
	assert.Nil(t, entity.Blob)
	assert.False(t, entity.Bool)
	assert.Equal(t, 0.0, entity.Float64)
	assert.Equal(t, float32(0.0), entity.Float32)
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
	assert.Nil(t, entity.Reference)
	assert.NotNil(t, reference.ID, entity.ReferenceRequired)

	for i := 0; i < 2; i++ {
		assert.Equal(t, "", entity.StringArray[i])
		assert.Equal(t, 0, entity.IntArray[i])
		assert.Equal(t, uint(0), entity.UintArray[i])
		assert.Nil(t, entity.UintNullableArray[i])
		assert.Nil(t, entity.IntNullableArray[i])
		assert.Nil(t, entity.BoolNullableArray[i])
		assert.Nil(t, entity.Float32NullableArray[i])
		assert.Nil(t, entity.SetNullableArray[i])
		assert.Equal(t, testEnum(""), entity.EnumNullableArray[i])
		assert.Nil(t, entity.BlobArray[i])
		assert.Equal(t, false, entity.BoolArray[i])
		assert.Equal(t, float64(0), entity.Float64Array[i])
		assert.Equal(t, float32(0), entity.Float32Array[i])
		assert.Equal(t, float64(0), entity.DecimalArray[i])
		assert.Nil(t, entity.DecimalNullableArray[i])
		assert.Equal(t, float64(0), entity.Float64UnsignedArray[i])
		assert.Equal(t, float64(0), entity.Float64SignedArray[i])
		assert.Equal(t, new(time.Time).UTC(), entity.TimeArray[i])
		assert.Equal(t, new(time.Time).UTC(), entity.TimeWithTimeArray[i])
		assert.Nil(t, entity.TimeNullableArray[i])
		assert.Nil(t, entity.TimeWithTimeNullableArray[i])
		assert.Nil(t, entity.Uint32NullableArray[i])
		assert.Nil(t, entity.ReferenceArray[i])
		assert.Nil(t, entity.FlushStructArray[i].TestTime)
		assert.Equal(t, 0, entity.FlushStructArray[i].Age)
		assert.Equal(t, "", entity.FlushStructArray[i].Name2)
		assert.Equal(t, "", entity.FlushStructArray[i].Sub.Name3)
		assert.Equal(t, 0, entity.FlushStructArray[i].Sub.Age3)
	}

	// Adding full entity
	newEntity = NewEntity[flushEntity](c)
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
	newEntity.SetNullable = []testEnum{testEnumDefinition.B, testEnumDefinition.C}
	newEntity.SetNotNull = []testEnum{testEnumDefinition.A, testEnumDefinition.C}
	newEntity.EnumNullable = testEnumDefinition.C
	newEntity.EnumNotNull = testEnumDefinition.A
	newEntity.Blob = []byte("test binary")
	newEntity.Bool = true
	newEntity.Float64 = 986.2322
	newEntity.Float32 = 86.232
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
	newEntity.Reference = NewReference[flushEntityReference](reference.ID)
	newEntity.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	for i := 0; i < 2; i++ {
		newEntity.StringArray[i] = fmt.Sprintf("Test %d", i)
		newEntity.IntArray[i] = i + 1
		newEntity.UintArray[i] = uint(i + 1)
		newEntity.UintNullableArray[i] = &newEntity.UintArray[i]
		newEntity.IntNullableArray[i] = &newEntity.IntArray[i]
		newEntity.BoolArray[i] = true
		newEntity.BoolNullableArray[i] = &newEntity.BoolArray[i]
		newEntity.Float64Array[i] = float64(i + 1)
		newEntity.Float32Array[i] = float32(i + 1)
		newEntity.DecimalArray[i] = float64(i + 1)
		newEntity.Float64UnsignedArray[i] = float64(i + 1)
		newEntity.Float64SignedArray[i] = float64(i + 1)
		newEntity.Float32NullableArray[i] = &newEntity.Float32Array[i]
		newEntity.SetNullableArray[i] = []testEnum{testEnumDefinition.B, testEnumDefinition.C}
		newEntity.EnumNullableArray[i] = testEnumDefinition.C
		newEntity.BlobArray[i] = []byte(fmt.Sprintf("Test %d", i))
		newEntity.DecimalNullableArray[i] = &newEntity.DecimalArray[i]
		newEntity.TimeArray[i] = time.Date(1982, 11, 4, 21, 0, 5, 6, time.UTC)
		newEntity.TimeWithTimeArray[i] = time.Date(1982, 11, 4, 21, 0, 5, 6, time.UTC)
		newEntity.TimeNullableArray[i] = &newEntity.TimeWithTimeArray[i]
		newEntity.TimeWithTimeNullableArray[i] = &newEntity.TimeWithTimeArray[i]
		newEntity.Uint32NullableArray[i] = &uint32Nullable
		newEntity.ReferenceArray[i] = NewReference[flushEntityReference](reference.ID)
		newEntity.FlushStructArray[i].Age = i + 1
		newEntity.FlushStructArray[i].Name2 = fmt.Sprintf("Name %d", i)
		newEntity.FlushStructArray[i].Sub.Name3 = fmt.Sprintf("Name %d", i)
		newEntity.FlushStructArray[i].Sub.Age3 = i + 1
	}
	assert.NoError(t, c.Flush(lazy))
	if lazy {
		err = ConsumeLazyFlushEvents(c, false)
		assert.NoError(t, err)
	}
	entity = GetByID[flushEntity](c, newEntity.ID)
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
	assert.Equal(t, []testEnum{testEnumDefinition.B, testEnumDefinition.C}, entity.SetNullable)
	assert.Equal(t, []testEnum{testEnumDefinition.A, testEnumDefinition.C}, entity.SetNotNull)
	assert.Equal(t, testEnumDefinition.C, entity.EnumNullable)
	assert.Equal(t, testEnumDefinition.A, entity.EnumNotNull)
	assert.Equal(t, []byte("test binary"), entity.Blob)
	assert.True(t, entity.Bool)
	assert.Equal(t, 986.2322, entity.Float64)
	assert.Equal(t, float32(86.232), entity.Float32)
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
	assert.Equal(t, reference.ID, entity.Reference.GetID())
	assert.Equal(t, reference.ID, entity.ReferenceRequired.GetID())
	for i := 0; i < 2; i++ {
		assert.Equal(t, fmt.Sprintf("Test %d", i), entity.StringArray[i])
		assert.Equal(t, i+1, entity.IntArray[i])
		assert.Equal(t, uint(i+1), entity.UintArray[i])
		assert.Equal(t, uint(i+1), *entity.UintNullableArray[i])
		assert.Equal(t, i+1, *entity.IntNullableArray[i])
		assert.True(t, *entity.BoolNullableArray[i])
		assert.Equal(t, float32(i+1), *entity.Float32NullableArray[i])
		assert.Equal(t, []testEnum{testEnumDefinition.B, testEnumDefinition.C}, entity.SetNullableArray[i])
		assert.Equal(t, testEnumDefinition.C, entity.EnumNullableArray[i])
		assert.Equal(t, []byte(fmt.Sprintf("Test %d", i)), entity.BlobArray[i])
		assert.True(t, entity.BoolArray[i])
		assert.Equal(t, float64(i+1), entity.Float64Array[i])
		assert.Equal(t, float32(i+1), entity.Float32Array[i])
		assert.Equal(t, float64(i+1), entity.DecimalArray[i])
		assert.Equal(t, float64(i+1), *entity.DecimalNullableArray[i])
		assert.Equal(t, float64(i+1), entity.Float64UnsignedArray[i])
		assert.Equal(t, float64(i+1), entity.Float64SignedArray[i])
		assert.Equal(t, time.Date(1982, 11, 4, 0, 0, 0, 0, time.UTC), entity.TimeArray[i])
		assert.Equal(t, time.Date(1982, 11, 4, 21, 0, 5, 0, time.UTC), entity.TimeWithTimeArray[i])
		assert.Equal(t, time.Date(1982, 11, 4, 0, 0, 0, 0, time.UTC), *entity.TimeNullableArray[i])
		assert.Equal(t, time.Date(1982, 11, 4, 21, 0, 5, 0, time.UTC), *entity.TimeWithTimeNullableArray[i])
		assert.Equal(t, uint32(2923), *entity.Uint32NullableArray[i])
		assert.Equal(t, entity.Reference.GetID(), entity.ReferenceArray[i].GetID())
		assert.Equal(t, i+1, entity.FlushStructArray[i].Age)
		assert.Equal(t, fmt.Sprintf("Name %d", i), entity.FlushStructArray[i].Name2)
		assert.Equal(t, fmt.Sprintf("Name %d", i), entity.FlushStructArray[i].Sub.Name3)
		assert.Equal(t, i+1, entity.FlushStructArray[i].Sub.Age3)
	}

	// rounding dates
	newEntity = NewEntity[flushEntity](c)
	newEntity.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	newEntity.Name = "rounding dates"
	newEntity.City = "rounding dates"
	newEntity.Time = time.Date(2023, 11, 12, 22, 12, 34, 4, time.UTC)
	newEntity.TimeWithTime = time.Date(2023, 8, 16, 12, 23, 11, 6, time.UTC)
	timeNullable = time.Date(2023, 11, 12, 22, 12, 34, 4, time.UTC)
	newEntity.TimeNullable = &timeNullable
	timeWithTimeNullable = time.Date(2023, 8, 16, 12, 23, 11, 6, time.UTC)
	newEntity.TimeWithTimeNullable = &timeWithTimeNullable
	assert.NoError(t, c.Flush(lazy))
	assert.Equal(t, time.Date(2023, 11, 12, 0, 0, 0, 0, time.UTC), newEntity.Time)
	assert.Equal(t, time.Date(2023, 8, 16, 12, 23, 11, 0, time.UTC), newEntity.TimeWithTime)
	assert.Equal(t, time.Date(2023, 11, 12, 0, 0, 0, 0, time.UTC), *newEntity.TimeNullable)
	assert.Equal(t, time.Date(2023, 8, 16, 12, 23, 11, 0, time.UTC), *newEntity.TimeWithTimeNullable)

	// rounding floats
	newEntity = NewEntity[flushEntity](c)
	newEntity.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	newEntity.Name = "rounding floats"
	newEntity.City = "rounding floats"
	newEntity.Float64 = 1.123456
	newEntity.Decimal = 1.123
	floatNullable = 1.1234
	newEntity.FloatNullable = &floatNullable
	decimalNullable = 1.126
	newEntity.DecimalNullable = &decimalNullable
	assert.NoError(t, c.Flush(lazy))
	assert.Equal(t, 1.12346, newEntity.Float64)
	assert.Equal(t, 1.12, newEntity.Decimal)
	assert.Equal(t, 1.123, *newEntity.FloatNullable)
	assert.Equal(t, 1.13, *newEntity.DecimalNullable)

	// invalid values

	// empty string
	newEntity = NewEntity[flushEntity](c)
	newEntity.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[Name] empty string not allowed")
	assert.Equal(t, "Name", err.(*BindError).Field)
	c.ClearFlush()

	// string too long
	newEntity = NewEntity[flushEntity](c)
	newEntity.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	newEntity.Name = strings.Repeat("a", 256)
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[Name] text too long, max 255 allowed")
	assert.Equal(t, "Name", err.(*BindError).Field)
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[Name] text too long, max 255 allowed")
	c.ClearFlush()
	assert.NoError(t, c.Flush(lazy))
	newEntity = NewEntity[flushEntity](c)
	newEntity.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	newEntity.City = strings.Repeat("a", 41)
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[City] text too long, max 40 allowed")
	newEntity.City = strings.Repeat("a", 40)
	newEntity.Name = "String to long"
	assert.NoError(t, c.Flush(lazy))

	// invalid decimal
	newEntity = NewEntity[flushEntity](c)
	newEntity.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	newEntity.Decimal = 1234
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[Decimal] decimal size too big, max 3 allowed")
	assert.Equal(t, "Decimal", err.(*BindError).Field)
	c.ClearFlush()
	newEntity = NewEntity[flushEntity](c)
	newEntity.Name = "Name"
	newEntity.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	decimalNullable = 1234
	newEntity.DecimalNullable = &decimalNullable
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[DecimalNullable] decimal size too big, max 3 allowed")
	assert.Equal(t, "DecimalNullable", err.(*BindError).Field)
	c.ClearFlush()

	// float signed
	newEntity = NewEntity[flushEntity](c)
	newEntity.Name = "Name"
	newEntity.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	newEntity.Float64Unsigned = -1
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[Float64Unsigned] negative value not allowed")
	assert.Equal(t, "Float64Unsigned", err.(*BindError).Field)
	newEntity.Float64Unsigned = 1
	floatNullable = -1
	newEntity.FloatNullable = &floatNullable
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[FloatNullable] negative value not allowed")
	assert.Equal(t, "FloatNullable", err.(*BindError).Field)
	c.ClearFlush()

	// invalid enum, set
	newEntity = NewEntity[flushEntity](c)
	newEntity.Name = "Name 2"
	newEntity.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	newEntity.EnumNotNull = ""
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[EnumNotNull] empty value not allowed")
	assert.Equal(t, "EnumNotNull", err.(*BindError).Field)
	newEntity.EnumNotNull = "invalid"
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[EnumNotNull] invalid value: invalid")
	assert.Equal(t, "EnumNotNull", err.(*BindError).Field)
	newEntity.EnumNotNull = testEnumDefinition.C
	newEntity.SetNotNull = nil
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[SetNotNull] empty value not allowed")
	assert.Equal(t, "SetNotNull", err.(*BindError).Field)
	newEntity.SetNotNull = []testEnum{}
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[SetNotNull] empty value not allowed")
	assert.Equal(t, "SetNotNull", err.(*BindError).Field)
	newEntity.SetNotNull = []testEnum{"invalid"}
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[SetNotNull] invalid value: invalid")
	assert.Equal(t, "SetNotNull", err.(*BindError).Field)
	c.ClearFlush()

	// Time
	newEntity = NewEntity[flushEntity](c)
	newEntity.Name = "Name"
	newEntity.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	newEntity.Time = time.Now().Local()
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[Time] time must be in UTC location")
	assert.Equal(t, "Time", err.(*BindError).Field)
	newEntity.Time = newEntity.Time.UTC()
	newEntity.TimeWithTime = time.Now().Local()
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[TimeWithTime] time must be in UTC location")
	assert.Equal(t, "TimeWithTime", err.(*BindError).Field)

	// nullable times
	newEntity.TimeWithTime = newEntity.Time.UTC()
	timeNullable = time.Now().Local()
	newEntity.TimeNullable = &timeNullable
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[TimeNullable] time must be in UTC location")
	assert.Equal(t, "TimeNullable", err.(*BindError).Field)
	timeWithTimeNullable = time.Now().Local()
	timeNullable = time.Now().UTC()
	newEntity.TimeNullable = &timeNullable
	newEntity.TimeWithTimeNullable = &timeWithTimeNullable
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[TimeWithTimeNullable] time must be in UTC location")
	assert.Equal(t, "TimeWithTimeNullable", err.(*BindError).Field)
	c.ClearFlush()

	// duplicated key
	newEntity = NewEntity[flushEntity](c)
	newEntity.City = "Another city "
	newEntity.Name = "Name"
	newEntity.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	err = c.Flush(lazy)
	assert.EqualError(t, err, "duplicated value for unique index 'name'")
	assert.Equal(t, firstEntityID, err.(*DuplicatedKeyBindError).ID)
	assert.Equal(t, "name", err.(*DuplicatedKeyBindError).Index)
	assert.Equal(t, []string{"Name"}, err.(*DuplicatedKeyBindError).Columns)
	c.ClearFlush()

	c.Engine().Redis(DefaultPoolCode).FlushDB(c)
	LoadUniqueKeys(c, false)
	newEntity = NewEntity[flushEntity](c)
	newEntity.Name = "Name"
	newEntity.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	err = c.Flush(lazy)
	assert.EqualError(t, err, "duplicated value for unique index 'name'")
	assert.Equal(t, firstEntityID, err.(*DuplicatedKeyBindError).ID)
	assert.Equal(t, "name", err.(*DuplicatedKeyBindError).Index)
	assert.Equal(t, []string{"Name"}, err.(*DuplicatedKeyBindError).Columns)
	c.ClearFlush()
}

func testFlushDelete(t *testing.T, lazy, local, redis bool) {
	registry := NewRegistry()
	c := PrepareTables(t, registry, &flushEntity{}, &flushEntityReference{})

	schema := GetEntitySchema[flushEntity](c)
	schema.DisableCache(!local, !redis)

	reference := NewEntity[flushEntityReference](c)
	reference.Name = "test reference"
	err := c.Flush(false)
	assert.NoError(t, err)

	entity := NewEntity[flushEntity](c)
	entity.Name = "Test 1"
	entity.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	err = c.Flush(false)
	assert.NoError(t, err)

	id := entity.ID

	toDelete := DeleteEntity(c, entity)
	assert.NotNil(t, toDelete.SourceEntity())
	assert.Equal(t, toDelete.SourceEntity().ID, entity.ID)
	err = c.Flush(lazy)
	assert.NoError(t, err)

	loggerDB := &MockLogHandler{}
	c.RegisterQueryLogger(loggerDB, true, false, false)

	if redis || local {
		entity = GetByID[flushEntity](c, id)
		assert.Nil(t, entity)
		assert.Len(t, loggerDB.Logs, 0)
		loggerDB.Clear()
	}

	if lazy {
		err = ConsumeLazyFlushEvents(c, false)
		assert.NoError(t, err)
	}

	entity = GetByID[flushEntity](c, id)
	assert.Nil(t, entity)

	// duplicated key
	entity = NewEntity[flushEntity](c)
	entity.Name = "Test 1"
	entity.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	err = c.Flush(false)
	assert.NoError(t, err)
}

func testFlushUpdate(t *testing.T, lazy, local, redis bool) {
	registry := NewRegistry()
	c := PrepareTables(t, registry, &flushEntity{}, &flushEntityReference{})

	schema := GetEntitySchema[flushEntity](c)
	schema.DisableCache(!local, !redis)

	reference := NewEntity[flushEntityReference](c)
	reference.Name = "test reference"
	err := c.Flush(false)
	assert.NoError(t, err)

	newEntity := NewEntity[flushEntity](c)
	newEntity.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	newEntity.Name = "Name"
	assert.NoError(t, c.Flush(false))

	loggerDB := &MockLogHandler{}
	c.RegisterQueryLogger(loggerDB, true, false, false)
	loggerLocal := &MockLogHandler{}
	c.RegisterQueryLogger(loggerLocal, false, false, true)

	// empty entity
	editedEntity := EditEntity(c, newEntity).TrackedEntity()
	assert.Equal(t, "Name", editedEntity.Name)
	assert.Equal(t, newEntity.ReferenceRequired, editedEntity.ReferenceRequired)
	assert.NoError(t, c.Flush(lazy))
	assert.Len(t, loggerDB.Logs, 0)

	// editing to full entity
	editedEntityFull := EditEntity(c, editedEntity)
	editedEntity = editedEntityFull.TrackedEntity()
	editedEntity.City = "New York"
	editedEntity.Name = "Test name"
	editedEntity.Age = -19
	editedEntity.Uint = 134
	uintNullable := uint(23)
	editedEntity.UintNullable = &uintNullable
	intNullable := -45
	editedEntity.IntNullable = &intNullable
	boolNullable := true
	editedEntity.BoolNullable = &boolNullable
	floatNullable := 12.23
	editedEntity.FloatNullable = &floatNullable
	float32Nullable := float32(12.24)
	editedEntity.Float32Nullable = &float32Nullable
	editedEntity.SetNullable = []testEnum{testEnumDefinition.B, testEnumDefinition.C}
	editedEntity.SetNotNull = []testEnum{testEnumDefinition.A, testEnumDefinition.C}
	editedEntity.EnumNullable = testEnumDefinition.C
	editedEntity.EnumNotNull = testEnumDefinition.A
	editedEntity.Blob = []byte("test binary")
	editedEntity.Bool = true
	editedEntity.Float64 = 986.2322
	editedEntity.Decimal = 78.24
	decimalNullable := 123.23
	editedEntity.DecimalNullable = &decimalNullable
	editedEntity.Float64Unsigned = 8932.299423
	editedEntity.Float64Signed = -352.120321
	editedEntity.Time = time.Date(2023, 11, 12, 22, 12, 34, 0, time.UTC)
	editedEntity.TimeWithTime = time.Date(2023, 8, 16, 12, 23, 11, 0, time.UTC)
	timeNullable := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	editedEntity.TimeNullable = &timeNullable
	timeWithTimeNullable := time.Date(2025, 11, 4, 21, 0, 5, 6, time.UTC)
	editedEntity.TimeWithTimeNullable = &timeWithTimeNullable
	editedEntity.FlushStruct.Name2 = "Tom"
	editedEntity.FlushStruct.Age = 23
	editedEntity.FlushStruct.Sub.Name3 = "Zoya"
	editedEntity.FlushStruct.Sub.Age3 = 18
	testTime := time.Date(1982, 11, 4, 21, 0, 5, 6, time.UTC)
	editedEntity.FlushStruct.TestTime = &testTime
	int8Nullable := int8(23)
	editedEntity.Int8Nullable = &int8Nullable
	int16Nullable := int16(-29)
	editedEntity.Int16Nullable = &int16Nullable
	int32Nullable := int32(-2923)
	editedEntity.Int32Nullable = &int32Nullable
	int64Nullable := int64(98872)
	editedEntity.Int64Nullable = &int64Nullable
	uint8Nullable := uint8(23)
	editedEntity.Uint8Nullable = &uint8Nullable
	uint16Nullable := uint16(29)
	editedEntity.Uint16Nullable = &uint16Nullable
	uint32Nullable := uint32(2923)
	editedEntity.Uint32Nullable = &uint32Nullable
	uint64Nullable := uint64(98872)
	editedEntity.Uint64Nullable = &uint64Nullable
	editedEntity.SubName = "sub name"
	editedEntity.SubAge = 123
	editedEntity.Reference = NewReference[flushEntityReference](reference.ID)
	editedEntity.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	for i := 0; i < 2; i++ {
		editedEntity.StringArray[i] = fmt.Sprintf("Test %d", i)
		editedEntity.IntArray[i] = i + 1
		editedEntity.UintArray[i] = uint(i + 1)
		editedEntity.UintNullableArray[i] = &editedEntity.UintArray[i]
		editedEntity.IntNullableArray[i] = &editedEntity.IntArray[i]
		editedEntity.BoolArray[i] = true
		editedEntity.BoolNullableArray[i] = &editedEntity.BoolArray[i]
		editedEntity.Float64Array[i] = float64(i + 1)
		editedEntity.Float32Array[i] = float32(i + 1)
		editedEntity.DecimalArray[i] = float64(i + 1)
		editedEntity.Float64UnsignedArray[i] = float64(i + 1)
		editedEntity.Float64SignedArray[i] = float64(i + 1)
		editedEntity.Float32NullableArray[i] = &editedEntity.Float32Array[i]
		editedEntity.SetNullableArray[i] = []testEnum{testEnumDefinition.B, testEnumDefinition.C}
		editedEntity.EnumNullableArray[i] = testEnumDefinition.C
		editedEntity.BlobArray[i] = []byte(fmt.Sprintf("Test %d", i))
		editedEntity.DecimalNullableArray[i] = &editedEntity.DecimalArray[i]
		editedEntity.TimeArray[i] = time.Date(1982, 11, 4, 21, 0, 5, 6, time.UTC)
		editedEntity.TimeWithTimeArray[i] = time.Date(1982, 11, 4, 21, 0, 5, 6, time.UTC)
		editedEntity.TimeNullableArray[i] = &editedEntity.TimeWithTimeArray[i]
		editedEntity.TimeWithTimeNullableArray[i] = &editedEntity.TimeWithTimeArray[i]
		editedEntity.Uint32NullableArray[i] = &uint32Nullable
		editedEntity.ReferenceArray[i] = NewReference[flushEntityReference](reference.ID)
		editedEntity.FlushStructArray[i].Age = i + 1
		editedEntity.FlushStructArray[i].Name2 = fmt.Sprintf("Name %d", i)
		editedEntity.FlushStructArray[i].Sub.Name3 = fmt.Sprintf("Name %d", i)
		editedEntity.FlushStructArray[i].Sub.Age3 = i + 1
	}

	loggerLocal.Clear()
	assert.NoError(t, c.Flush(lazy))
	if !lazy {
		assert.Len(t, loggerDB.Logs, 1)
	} else {
		assert.Len(t, loggerDB.Logs, 0)
		err = ConsumeLazyFlushEvents(c, false)
		assert.NoError(t, err)
	}
	loggerDB.Clear()
	loggerLocal.Clear()
	entity := GetByID[flushEntity](c, editedEntity.ID)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
	assert.NotNil(t, entity)
	assert.Equal(t, editedEntity.ID, entity.ID)
	assert.Equal(t, "New York", entity.City)
	assert.Equal(t, "Test name", entity.Name)
	assert.Equal(t, -19, entity.Age)
	assert.Equal(t, uint(134), entity.Uint)
	assert.Equal(t, uint(23), *entity.UintNullable)
	assert.Equal(t, -45, *entity.IntNullable)
	assert.True(t, *entity.BoolNullable)
	assert.Equal(t, 12.23, *entity.FloatNullable)
	assert.Equal(t, float32(12.24), *entity.Float32Nullable)
	assert.Equal(t, []testEnum{testEnumDefinition.B, testEnumDefinition.C}, entity.SetNullable)
	assert.Equal(t, []testEnum{testEnumDefinition.A, testEnumDefinition.C}, entity.SetNotNull)
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
	assert.Equal(t, reference.ID, entity.Reference.GetID())
	assert.Equal(t, reference.ID, entity.ReferenceRequired.GetID())
	for i := 0; i < 2; i++ {
		assert.Equal(t, fmt.Sprintf("Test %d", i), entity.StringArray[i])
		assert.Equal(t, i+1, entity.IntArray[i])
		assert.Equal(t, uint(i+1), entity.UintArray[i])
		assert.Equal(t, uint(i+1), *entity.UintNullableArray[i])
		assert.Equal(t, i+1, *entity.IntNullableArray[i])
		assert.True(t, *entity.BoolNullableArray[i])
		assert.Equal(t, float32(i+1), *entity.Float32NullableArray[i])
		assert.Equal(t, []testEnum{testEnumDefinition.B, testEnumDefinition.C}, entity.SetNullableArray[i])
		assert.Equal(t, testEnumDefinition.C, entity.EnumNullableArray[i])
		assert.Equal(t, []byte(fmt.Sprintf("Test %d", i)), entity.BlobArray[i])
		assert.True(t, entity.BoolArray[i])
		assert.Equal(t, float64(i+1), entity.Float64Array[i])
		assert.Equal(t, float32(i+1), entity.Float32Array[i])
		assert.Equal(t, float64(i+1), entity.DecimalArray[i])
		assert.Equal(t, float64(i+1), *entity.DecimalNullableArray[i])
		assert.Equal(t, float64(i+1), entity.Float64UnsignedArray[i])
		assert.Equal(t, float64(i+1), entity.Float64SignedArray[i])
		assert.Equal(t, time.Date(1982, 11, 4, 0, 0, 0, 0, time.UTC), entity.TimeArray[i])
		assert.Equal(t, time.Date(1982, 11, 4, 21, 0, 5, 0, time.UTC), entity.TimeWithTimeArray[i])
		assert.Equal(t, time.Date(1982, 11, 4, 0, 0, 0, 0, time.UTC), *entity.TimeNullableArray[i])
		assert.Equal(t, time.Date(1982, 11, 4, 21, 0, 5, 0, time.UTC), *entity.TimeWithTimeNullableArray[i])
		assert.Equal(t, uint32(2923), *entity.Uint32NullableArray[i])
		assert.Equal(t, entity.Reference.GetID(), entity.ReferenceArray[i].GetID())
		assert.Equal(t, i+1, entity.FlushStructArray[i].Age)
		assert.Equal(t, fmt.Sprintf("Name %d", i), entity.FlushStructArray[i].Name2)
		assert.Equal(t, fmt.Sprintf("Name %d", i), entity.FlushStructArray[i].Sub.Name3)
		assert.Equal(t, i+1, entity.FlushStructArray[i].Sub.Age3)
	}
	if local {
		assert.Len(t, loggerDB.Logs, 0)
		assert.Len(t, loggerLocal.Logs, 1)
		assert.Equal(t, "New York", editedEntityFull.SourceEntity().City)
		assert.Equal(t, "Test name", editedEntityFull.SourceEntity().Name)
	}

	loggerDB.Clear()
	editedEntity = EditEntity(c, editedEntity).TrackedEntity()
	assert.NoError(t, c.Flush(lazy))
	assert.Len(t, loggerDB.Logs, 0)

	// rounding dates
	editedEntity = EditEntity(c, editedEntity).TrackedEntity()
	editedEntity.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	editedEntity.Name = "rounding dates"
	editedEntity.City = "rounding dates"
	editedEntity.Time = time.Date(2023, 11, 12, 22, 12, 34, 4, time.UTC)
	editedEntity.TimeWithTime = time.Date(2023, 8, 16, 12, 23, 11, 6, time.UTC)
	timeNullable = time.Date(2023, 11, 12, 22, 12, 34, 4, time.UTC)
	editedEntity.TimeNullable = &timeNullable
	timeWithTimeNullable = time.Date(2023, 8, 16, 12, 23, 11, 6, time.UTC)
	editedEntity.TimeWithTimeNullable = &timeWithTimeNullable
	assert.NoError(t, c.Flush(lazy))
	if !lazy {
		assert.Len(t, loggerDB.Logs, 1)
	}
	assert.Equal(t, time.Date(2023, 11, 12, 0, 0, 0, 0, time.UTC), editedEntity.Time)
	assert.Equal(t, time.Date(2023, 8, 16, 12, 23, 11, 0, time.UTC), editedEntity.TimeWithTime)
	assert.Equal(t, time.Date(2023, 11, 12, 0, 0, 0, 0, time.UTC), *editedEntity.TimeNullable)
	assert.Equal(t, time.Date(2023, 8, 16, 12, 23, 11, 0, time.UTC), *editedEntity.TimeWithTimeNullable)

	// same dates
	loggerDB.Clear()
	editedEntity = EditEntity(c, editedEntity).TrackedEntity()
	editedEntity.Time = time.Date(2023, 11, 12, 22, 12, 34, 4, time.UTC)
	timeNullable = time.Date(2023, 11, 12, 22, 12, 34, 4, time.UTC)
	editedEntity.TimeNullable = &timeNullable
	assert.NoError(t, c.Flush(lazy))
	assert.Len(t, loggerDB.Logs, 0)

	// same times
	editedEntity = EditEntity(c, editedEntity).TrackedEntity()
	editedEntity.TimeWithTime = time.Date(2023, 8, 16, 12, 23, 11, 6, time.UTC)
	timeWithTimeNullable = time.Date(2023, 8, 16, 12, 23, 11, 6, time.UTC)
	editedEntity.TimeWithTimeNullable = &timeWithTimeNullable
	assert.NoError(t, c.Flush(lazy))
	assert.Len(t, loggerDB.Logs, 0)

	// rounding floats
	editedEntity = EditEntity(c, editedEntity).TrackedEntity()
	editedEntity.Name = "rounding floats"
	editedEntity.City = "rounding floats"
	editedEntity.Float64 = 1.123456
	editedEntity.Decimal = 1.123
	floatNullable = 1.1234
	editedEntity.FloatNullable = &floatNullable
	decimalNullable = 1.126
	editedEntity.DecimalNullable = &decimalNullable
	assert.NoError(t, c.Flush(lazy))
	if !lazy {
		assert.Len(t, loggerDB.Logs, 1)
	}
	assert.Equal(t, 1.12346, editedEntity.Float64)
	assert.Equal(t, 1.12, editedEntity.Decimal)
	assert.Equal(t, 1.123, *editedEntity.FloatNullable)
	assert.Equal(t, 1.13, *editedEntity.DecimalNullable)
	loggerDB.Clear()

	// same floats
	editedEntity = EditEntity(c, editedEntity).TrackedEntity()
	editedEntity.Float64 = 1.123456
	editedEntity.Decimal = 1.123
	floatNullable = 1.1234
	editedEntity.FloatNullable = &floatNullable
	decimalNullable = 1.126
	editedEntity.DecimalNullable = &decimalNullable
	assert.NoError(t, c.Flush(lazy))
	assert.Len(t, loggerDB.Logs, 0)

	// same set
	editedEntity = EditEntity(c, editedEntity).TrackedEntity()
	editedEntity.SetNullable = []testEnum{testEnumDefinition.C, testEnumDefinition.B}
	editedEntity.SetNotNull = []testEnum{testEnumDefinition.C, testEnumDefinition.A}
	assert.NoError(t, c.Flush(lazy))
	assert.Len(t, loggerDB.Logs, 0)
	editedEntity = EditEntity(c, editedEntity).TrackedEntity()
	editedEntity.SetNullable = []testEnum{testEnumDefinition.C, testEnumDefinition.B, testEnumDefinition.B}
	editedEntity.SetNotNull = []testEnum{testEnumDefinition.C, testEnumDefinition.A, testEnumDefinition.A}
	assert.NoError(t, c.Flush(lazy))
	assert.Len(t, loggerDB.Logs, 0)

	// invalid values

	// empty string
	editedEntity = EditEntity(c, editedEntity).TrackedEntity()
	editedEntity.Name = ""
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[Name] empty string not allowed")
	assert.Equal(t, "Name", err.(*BindError).Field)
	c.ClearFlush()

	// string too long
	editedEntity = EditEntity(c, editedEntity).TrackedEntity()
	editedEntity.Name = strings.Repeat("a", 256)
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[Name] text too long, max 255 allowed")
	assert.Equal(t, "Name", err.(*BindError).Field)
	c.ClearFlush()
	editedEntity = EditEntity(c, editedEntity).TrackedEntity()
	editedEntity.City = strings.Repeat("a", 41)
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[City] text too long, max 40 allowed")
	editedEntity.City = strings.Repeat("a", 40)
	editedEntity.Name = "String to long"
	assert.NoError(t, c.Flush(lazy))

	// invalid decimal
	editedEntity = EditEntity(c, editedEntity).TrackedEntity()
	editedEntity.Decimal = 1234
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[Decimal] decimal size too big, max 3 allowed")
	assert.Equal(t, "Decimal", err.(*BindError).Field)
	c.ClearFlush()
	editedEntity = EditEntity(c, editedEntity).TrackedEntity()
	editedEntity.Decimal = 123
	decimalNullable = 1234
	editedEntity.DecimalNullable = &decimalNullable
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[DecimalNullable] decimal size too big, max 3 allowed")
	assert.Equal(t, "DecimalNullable", err.(*BindError).Field)
	c.ClearFlush()
	decimalNullable = 123
	editedEntity.DecimalNullable = &decimalNullable

	// float signed
	editedEntity = EditEntity(c, editedEntity).TrackedEntity()
	editedEntity.Float64Unsigned = -1
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[Float64Unsigned] negative value not allowed")
	assert.Equal(t, "Float64Unsigned", err.(*BindError).Field)
	editedEntity.Float64Unsigned = 1
	floatNullable = -1
	editedEntity.FloatNullable = &floatNullable
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[FloatNullable] negative value not allowed")
	assert.Equal(t, "FloatNullable", err.(*BindError).Field)
	c.ClearFlush()
	floatNullable = 1
	editedEntity.FloatNullable = &floatNullable

	// invalid enum, set
	editedEntity = EditEntity(c, editedEntity).TrackedEntity()
	editedEntity.EnumNotNull = ""
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[EnumNotNull] empty value not allowed")
	assert.Equal(t, "EnumNotNull", err.(*BindError).Field)
	editedEntity.EnumNotNull = "invalid"
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[EnumNotNull] invalid value: invalid")
	assert.Equal(t, "EnumNotNull", err.(*BindError).Field)
	editedEntity.EnumNotNull = testEnumDefinition.C
	editedEntity.SetNotNull = nil
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[SetNotNull] empty value not allowed")
	assert.Equal(t, "SetNotNull", err.(*BindError).Field)
	editedEntity.SetNotNull = []testEnum{}
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[SetNotNull] empty value not allowed")
	assert.Equal(t, "SetNotNull", err.(*BindError).Field)
	editedEntity.SetNotNull = []testEnum{"invalid"}
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[SetNotNull] invalid value: invalid")
	assert.Equal(t, "SetNotNull", err.(*BindError).Field)
	editedEntity.SetNotNull = []testEnum{testEnumDefinition.B}
	c.ClearFlush()

	// Time
	editedEntity = EditEntity(c, editedEntity).TrackedEntity()
	editedEntity.Time = time.Now().Local()
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[Time] time must be in UTC location")
	assert.Equal(t, "Time", err.(*BindError).Field)
	editedEntity.Time = newEntity.Time.UTC()
	editedEntity.TimeWithTime = time.Now().Local()
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[TimeWithTime] time must be in UTC location")
	assert.Equal(t, "TimeWithTime", err.(*BindError).Field)

	// nullable times
	editedEntity.TimeWithTime = editedEntity.Time.UTC()
	timeNullable = time.Now().Local()
	editedEntity.TimeNullable = &timeNullable
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[TimeNullable] time must be in UTC location")
	assert.Equal(t, "TimeNullable", err.(*BindError).Field)
	timeWithTimeNullable = time.Now().Local()
	timeNullable = time.Now().UTC()
	editedEntity.TimeNullable = &timeNullable
	editedEntity.TimeWithTimeNullable = &timeWithTimeNullable
	err = c.Flush(lazy)
	assert.EqualError(t, err, "[TimeWithTimeNullable] time must be in UTC location")
	assert.Equal(t, "TimeWithTimeNullable", err.(*BindError).Field)
	timeWithTimeNullable = time.Now().UTC()
	editedEntity.TimeWithTimeNullable = &timeWithTimeNullable
	c.ClearFlush()

	// duplicated key
	newEntity = NewEntity[flushEntity](c)
	newEntity.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	newEntity.Name = "Name 2"
	assert.NoError(t, c.Flush(lazy))

	editedEntity = EditEntity(c, editedEntity).TrackedEntity()
	editedEntity.Name = "Name 2"
	err = c.Flush(lazy)
	assert.EqualError(t, err, "duplicated value for unique index 'name'")
	assert.Equal(t, newEntity.ID, err.(*DuplicatedKeyBindError).ID)
	assert.Equal(t, "name", err.(*DuplicatedKeyBindError).Index)
	assert.Equal(t, []string{"Name"}, err.(*DuplicatedKeyBindError).Columns)
	c.ClearFlush()

	editedEntity = EditEntity(c, newEntity).TrackedEntity()
	editedEntity.Name = "Name 3"
	assert.NoError(t, c.Flush(lazy))

	newEntity = NewEntity[flushEntity](c)
	newEntity.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	newEntity.Name = "Name 2"
	assert.NoError(t, c.Flush(lazy))
}

func TestFlushTransaction(t *testing.T) {
	registry := NewRegistry()
	c := PrepareTables(t, registry, &flushEntity{}, &flushEntityReference{})

	schema := GetEntitySchema[flushEntity](c)
	schema.DisableCache(true, true)

	loggerDB := &MockLogHandler{}
	c.RegisterQueryLogger(loggerDB, true, false, false)

	reference := NewEntity[flushEntityReference](c)
	reference.Name = "test reference"
	err := c.Flush(false)
	assert.NoError(t, err)
	assert.Len(t, loggerDB.Logs, 1)
	loggerDB.Clear()

	reference = NewEntity[flushEntityReference](c)
	reference.Name = "test reference 2"
	reference2 := NewEntity[flushEntityReference](c)
	reference2.Name = "test reference 3"
	err = c.Flush(false)
	assert.NoError(t, err)
	assert.Len(t, loggerDB.Logs, 1)
	loggerDB.Clear()

	reference = NewEntity[flushEntityReference](c)
	reference.Name = "test reference 2"
	flushE := NewEntity[flushEntity](c)
	flushE.Name = "test"
	flushE.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	err = c.Flush(false)
	assert.NoError(t, err)
	assert.Len(t, loggerDB.Logs, 4)
	assert.Equal(t, "START TRANSACTION", loggerDB.Logs[0]["query"])
	assert.Equal(t, "COMMIT", loggerDB.Logs[3]["query"])
	loggerDB.Clear()

	// Skipping invalid event
}
