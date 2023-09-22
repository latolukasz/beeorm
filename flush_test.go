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
	FloatNullable        *float64 `orm:"precision=3;unsigned"`
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
	Reference            *Reference[*flushEntityReference]
	ReferenceRequired    *Reference[*flushEntityReference] `orm:"required"`
	flushStructAnonymous
}

type flushEntityReference struct {
	ID   uint64
	Name string `orm:"required"`
}

func (e *flushEntity) GetID() uint64 {
	return e.ID
}

func (e *flushEntityReference) GetID() uint64 {
	return e.ID
}

func TestFlushInsertLocalRedis(t *testing.T) {
	testFlushInsert(t, true, true)
}

func TestFlushInsertLocal(t *testing.T) {
	testFlushInsert(t, true, false)
}

func TestFlushInsertNoCache(t *testing.T) {
	testFlushInsert(t, false, false)
}

func TestFlushInsertRedis(t *testing.T) {
	testFlushInsert(t, false, true)
}

func TestFlushDeleteLocalRedis(t *testing.T) {
	testFlushDelete(t, true, true)
}

func TestFlushDeleteLocal(t *testing.T) {
	testFlushDelete(t, true, false)
}

func TestFlushDeleteNoCache(t *testing.T) {
	testFlushDelete(t, false, false)
}

func TestFlushDeleteRedis(t *testing.T) {
	testFlushDelete(t, false, true)
}

func TestFlushUpdateLocalRedis(t *testing.T) {
	testFlushUpdate(t, true, true)
}

func TestFlushUpdateLocal(t *testing.T) {
	testFlushUpdate(t, true, false)
}

func TestFlushUpdateNoCache(t *testing.T) {
	testFlushUpdate(t, false, false)
}

func TestFlushUpdateRedis(t *testing.T) {
	testFlushUpdate(t, false, true)
}

//

func testFlushInsert(t *testing.T, local bool, redis bool) {
	registry := &Registry{}
	c := PrepareTables(t, registry, "", &flushEntity{}, &flushEntityReference{})

	schema := GetEntitySchema[*flushEntity](c)
	schema.DisableCache(!local, !redis)

	reference := NewEntity[*flushEntityReference](c).TrackedEntity()
	reference.Name = "test reference"
	err := c.Flush()
	assert.NoError(t, err)

	loggerDB := &MockLogHandler{}
	c.RegisterQueryLogger(loggerDB, true, false, false)
	loggerLocal := &MockLogHandler{}
	c.RegisterQueryLogger(loggerLocal, false, false, true)
	loggerRedis := &MockLogHandler{}
	c.RegisterQueryLogger(loggerRedis, false, true, false)

	// Adding empty entity
	newEntity := NewEntity[*flushEntity](c).TrackedEntity()
	newEntity.ReferenceRequired = NewReference[*flushEntityReference](reference.ID)
	newEntity.Name = "Name"
	assert.NotEmpty(t, newEntity.ID)
	firstEntityID := newEntity.ID
	assert.NoError(t, c.Flush())
	if local {
		assert.Len(t, loggerLocal.Logs, 1)
	}
	loggerDB.Clear()

	entity := GetByID[*flushEntity](c, newEntity.ID)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
	assert.NotNil(t, entity)
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
	assert.Nil(t, entity.Reference)
	assert.NotNil(t, reference.ID, entity.ReferenceRequired)

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
	newEntity.Reference = NewReference[*flushEntityReference](reference.ID)
	newEntity.ReferenceRequired = NewReference[*flushEntityReference](reference.ID)
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
	assert.Equal(t, reference.ID, entity.Reference.GetID())
	assert.Equal(t, reference.ID, entity.ReferenceRequired.GetID())

	// rounding dates
	newEntity = NewEntity[*flushEntity](c).TrackedEntity()
	newEntity.ReferenceRequired = NewReference[*flushEntityReference](reference.ID)
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
	newEntity.ReferenceRequired = NewReference[*flushEntityReference](reference.ID)
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

	// empty string
	newEntity = NewEntity[*flushEntity](c).TrackedEntity()
	newEntity.ReferenceRequired = NewReference[*flushEntityReference](reference.ID)
	err = c.Flush()
	assert.EqualError(t, err, "[Name] empty string not allowed")
	assert.Equal(t, "Name", err.(*BindError).Field)
	c.ClearFlush()

	// string too long
	newEntity = NewEntity[*flushEntity](c).TrackedEntity()
	newEntity.ReferenceRequired = NewReference[*flushEntityReference](reference.ID)
	newEntity.Name = strings.Repeat("a", 256)
	err = c.Flush()
	assert.EqualError(t, err, "[Name] text too long, max 255 allowed")
	assert.Equal(t, "Name", err.(*BindError).Field)
	err = c.Flush()
	assert.EqualError(t, err, "[Name] text too long, max 255 allowed")
	c.ClearFlush()
	assert.NoError(t, c.Flush())
	newEntity = NewEntity[*flushEntity](c).TrackedEntity()
	newEntity.ReferenceRequired = NewReference[*flushEntityReference](reference.ID)
	newEntity.City = strings.Repeat("a", 41)
	err = c.Flush()
	assert.EqualError(t, err, "[City] text too long, max 40 allowed")
	newEntity.City = strings.Repeat("a", 40)
	newEntity.Name = "String to long"
	assert.NoError(t, c.Flush())

	// invalid decimal
	newEntity = NewEntity[*flushEntity](c).TrackedEntity()
	newEntity.ReferenceRequired = NewReference[*flushEntityReference](reference.ID)
	newEntity.Decimal = 1234
	err = c.Flush()
	assert.EqualError(t, err, "[Decimal] decimal size too big, max 3 allowed")
	assert.Equal(t, "Decimal", err.(*BindError).Field)
	c.ClearFlush()
	newEntity = NewEntity[*flushEntity](c).TrackedEntity()
	newEntity.Name = "Name"
	newEntity.ReferenceRequired = NewReference[*flushEntityReference](reference.ID)
	decimalNullable = 1234
	newEntity.DecimalNullable = &decimalNullable
	err = c.Flush()
	assert.EqualError(t, err, "[DecimalNullable] decimal size too big, max 3 allowed")
	assert.Equal(t, "DecimalNullable", err.(*BindError).Field)
	c.ClearFlush()

	// float signed
	newEntity = NewEntity[*flushEntity](c).TrackedEntity()
	newEntity.Name = "Name"
	newEntity.ReferenceRequired = NewReference[*flushEntityReference](reference.ID)
	newEntity.Float64Unsigned = -1
	err = c.Flush()
	assert.EqualError(t, err, "[Float64Unsigned] negative value not allowed")
	assert.Equal(t, "Float64Unsigned", err.(*BindError).Field)
	newEntity.Float64Unsigned = 1
	floatNullable = -1
	newEntity.FloatNullable = &floatNullable
	err = c.Flush()
	assert.EqualError(t, err, "[FloatNullable] negative value not allowed")
	assert.Equal(t, "FloatNullable", err.(*BindError).Field)
	c.ClearFlush()

	// invalid enum, set
	newEntity = NewEntity[*flushEntity](c).TrackedEntity()
	newEntity.Name = "Name"
	newEntity.ReferenceRequired = NewReference[*flushEntityReference](reference.ID)
	newEntity.EnumNotNull = ""
	err = c.Flush()
	assert.EqualError(t, err, "[EnumNotNull] empty value not allowed")
	assert.Equal(t, "EnumNotNull", err.(*BindError).Field)
	newEntity.EnumNotNull = "invalid"
	err = c.Flush()
	assert.EqualError(t, err, "[EnumNotNull] invalid value: invalid")
	assert.Equal(t, "EnumNotNull", err.(*BindError).Field)
	newEntity.EnumNotNull = testEnumDefinition.C
	newEntity.SetNotNull = nil
	err = c.Flush()
	assert.EqualError(t, err, "[SetNotNull] empty value not allowed")
	assert.Equal(t, "SetNotNull", err.(*BindError).Field)
	newEntity.SetNotNull = testSet{}
	err = c.Flush()
	assert.EqualError(t, err, "[SetNotNull] empty value not allowed")
	assert.Equal(t, "SetNotNull", err.(*BindError).Field)
	newEntity.SetNotNull = testSet{"invalid"}
	err = c.Flush()
	assert.EqualError(t, err, "[SetNotNull] invalid value: invalid")
	assert.Equal(t, "SetNotNull", err.(*BindError).Field)
	c.ClearFlush()

	// Time
	newEntity = NewEntity[*flushEntity](c).TrackedEntity()
	newEntity.Name = "Name"
	newEntity.ReferenceRequired = NewReference[*flushEntityReference](reference.ID)
	newEntity.Time = time.Now().Local()
	err = c.Flush()
	assert.EqualError(t, err, "[Time] time must be in UTC location")
	assert.Equal(t, "Time", err.(*BindError).Field)
	newEntity.Time = newEntity.Time.UTC()
	newEntity.TimeWithTime = time.Now().Local()
	err = c.Flush()
	assert.EqualError(t, err, "[TimeWithTime] time must be in UTC location")
	assert.Equal(t, "TimeWithTime", err.(*BindError).Field)

	// nullable times
	newEntity.TimeWithTime = newEntity.Time.UTC()
	timeNullable = time.Now().Local()
	newEntity.TimeNullable = &timeNullable
	err = c.Flush()
	assert.EqualError(t, err, "[TimeNullable] time must be in UTC location")
	assert.Equal(t, "TimeNullable", err.(*BindError).Field)
	timeWithTimeNullable = time.Now().Local()
	timeNullable = time.Now().UTC()
	newEntity.TimeNullable = &timeNullable
	newEntity.TimeWithTimeNullable = &timeWithTimeNullable
	err = c.Flush()
	assert.EqualError(t, err, "[TimeWithTimeNullable] time must be in UTC location")
	assert.Equal(t, "TimeWithTimeNullable", err.(*BindError).Field)
	c.ClearFlush()

	// duplicated key
	newEntity = NewEntity[*flushEntity](c).TrackedEntity()
	newEntity.Name = "Name"
	newEntity.ReferenceRequired = NewReference[*flushEntityReference](reference.ID)
	err = c.Flush()
	assert.EqualError(t, err, "duplicated value for unique index 'name'")
	assert.Equal(t, firstEntityID, err.(*DuplicatedKeyBindError).ID)
	assert.Equal(t, "name", err.(*DuplicatedKeyBindError).Index)
	assert.Equal(t, []string{"Name"}, err.(*DuplicatedKeyBindError).Columns)
	c.ClearFlush()

	c.Engine().Redis(DefaultPoolCode).FlushDB(c)
	LoadUniqueKeys(c, false)
	newEntity = NewEntity[*flushEntity](c).TrackedEntity()
	newEntity.Name = "Name"
	newEntity.ReferenceRequired = NewReference[*flushEntityReference](reference.ID)
	err = c.Flush()
	assert.EqualError(t, err, "duplicated value for unique index 'name'")
	assert.Equal(t, firstEntityID, err.(*DuplicatedKeyBindError).ID)
	assert.Equal(t, "name", err.(*DuplicatedKeyBindError).Index)
	assert.Equal(t, []string{"Name"}, err.(*DuplicatedKeyBindError).Columns)
	c.ClearFlush()
}

func testFlushDelete(t *testing.T, local bool, redis bool) {
	registry := &Registry{}
	c := PrepareTables(t, registry, "", &flushEntity{}, &flushEntityReference{})

	schema := GetEntitySchema[*flushEntity](c)
	schema.DisableCache(!local, !redis)

	reference := NewEntity[*flushEntityReference](c).TrackedEntity()
	reference.Name = "test reference"
	err := c.Flush()
	assert.NoError(t, err)

	entity := NewEntity[*flushEntity](c).TrackedEntity()
	entity.Name = "Test 1"
	entity.ReferenceRequired = NewReference[*flushEntityReference](reference.ID)
	err = c.Flush()
	assert.NoError(t, err)

	// duplicated key

	toDelete := DeleteEntity(c, entity)
	assert.NotNil(t, toDelete.SourceEntity())
	assert.Equal(t, toDelete.SourceEntity().ID, entity.ID)
	err = c.Flush()
	assert.NoError(t, err)
	entity = GetByID[*flushEntity](c, entity.GetID())
	assert.Nil(t, entity)

	entity = NewEntity[*flushEntity](c).TrackedEntity()
	entity.Name = "Test 1"
	entity.ReferenceRequired = NewReference[*flushEntityReference](reference.ID)
	err = c.Flush()
	assert.NoError(t, err)
}

func testFlushUpdate(t *testing.T, local bool, redis bool) {
	registry := &Registry{}
	c := PrepareTables(t, registry, "", &flushEntity{}, &flushEntityReference{})

	schema := GetEntitySchema[*flushEntity](c)
	schema.DisableCache(!local, !redis)

	reference := NewEntity[*flushEntityReference](c).TrackedEntity()
	reference.Name = "test reference"
	err := c.Flush()
	assert.NoError(t, err)

	newEntity := NewEntity[*flushEntity](c).TrackedEntity()
	newEntity.ReferenceRequired = NewReference[*flushEntityReference](reference.ID)
	newEntity.Name = "Name"
	assert.NoError(t, c.Flush())

	loggerDB := &MockLogHandler{}
	c.RegisterQueryLogger(loggerDB, true, false, false)
	loggerLocal := &MockLogHandler{}
	c.RegisterQueryLogger(loggerLocal, false, false, true)

	// empty entity
	editedEntity := EditEdit[*flushEntity](c, newEntity).TrackedEntity()
	assert.Equal(t, "Name", editedEntity.Name)
	assert.Equal(t, newEntity.ReferenceRequired, editedEntity.ReferenceRequired)
	assert.NoError(t, c.Flush())
	assert.Len(t, loggerDB.Logs, 0)

	// editing to full entity
	editedEntityFull := EditEdit[*flushEntity](c, editedEntity)
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
	editedEntity.SetNullable = testSet{testEnumDefinition.B, testEnumDefinition.C}
	editedEntity.SetNotNull = testSet{testEnumDefinition.A, testEnumDefinition.C}
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
	editedEntity.Reference = NewReference[*flushEntityReference](reference.ID)
	editedEntity.ReferenceRequired = NewReference[*flushEntityReference](reference.ID)
	loggerLocal.Clear()
	assert.NoError(t, c.Flush())
	assert.Len(t, loggerDB.Logs, 1)
	if local {
		assert.Len(t, loggerLocal.Logs, 1)
	}
	loggerDB.Clear()
	loggerLocal.Clear()
	entity := GetByID[*flushEntity](c, editedEntity.ID)
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
	assert.Equal(t, reference.ID, entity.Reference.GetID())
	assert.Equal(t, reference.ID, entity.ReferenceRequired.GetID())
	if local {
		assert.Len(t, loggerDB.Logs, 0)
		assert.Len(t, loggerLocal.Logs, 1)
		assert.Equal(t, "New York", editedEntityFull.SourceEntity().City)
		assert.Equal(t, "Test name", editedEntityFull.SourceEntity().Name)
	}

	loggerDB.Clear()
	editedEntity = EditEdit[*flushEntity](c, editedEntity).TrackedEntity()
	assert.NoError(t, c.Flush())
	assert.Len(t, loggerDB.Logs, 0)

	// rounding dates
	editedEntity = EditEdit[*flushEntity](c, editedEntity).TrackedEntity()
	editedEntity.ReferenceRequired = NewReference[*flushEntityReference](reference.ID)
	editedEntity.Name = "rounding dates"
	editedEntity.City = "rounding dates"
	editedEntity.Time = time.Date(2023, 11, 12, 22, 12, 34, 4, time.UTC)
	editedEntity.TimeWithTime = time.Date(2023, 8, 16, 12, 23, 11, 6, time.UTC)
	timeNullable = time.Date(2023, 11, 12, 22, 12, 34, 4, time.UTC)
	editedEntity.TimeNullable = &timeNullable
	timeWithTimeNullable = time.Date(2023, 8, 16, 12, 23, 11, 6, time.UTC)
	editedEntity.TimeWithTimeNullable = &timeWithTimeNullable
	assert.NoError(t, c.Flush())
	assert.Len(t, loggerDB.Logs, 1)
	assert.Equal(t, time.Date(2023, 11, 12, 0, 0, 0, 0, time.UTC), editedEntity.Time)
	assert.Equal(t, time.Date(2023, 8, 16, 12, 23, 11, 0, time.UTC), editedEntity.TimeWithTime)
	assert.Equal(t, time.Date(2023, 11, 12, 0, 0, 0, 0, time.UTC), *editedEntity.TimeNullable)
	assert.Equal(t, time.Date(2023, 8, 16, 12, 23, 11, 0, time.UTC), *editedEntity.TimeWithTimeNullable)

	// same dates
	loggerDB.Clear()
	editedEntity = EditEdit[*flushEntity](c, editedEntity).TrackedEntity()
	editedEntity.Time = time.Date(2023, 11, 12, 22, 12, 34, 4, time.UTC)
	timeNullable = time.Date(2023, 11, 12, 22, 12, 34, 4, time.UTC)
	editedEntity.TimeNullable = &timeNullable
	assert.NoError(t, c.Flush())
	assert.Len(t, loggerDB.Logs, 0)

	// same times
	editedEntity = EditEdit[*flushEntity](c, editedEntity).TrackedEntity()
	editedEntity.TimeWithTime = time.Date(2023, 8, 16, 12, 23, 11, 6, time.UTC)
	timeWithTimeNullable = time.Date(2023, 8, 16, 12, 23, 11, 6, time.UTC)
	editedEntity.TimeWithTimeNullable = &timeWithTimeNullable
	assert.NoError(t, c.Flush())
	assert.Len(t, loggerDB.Logs, 0)

	// rounding floats
	editedEntity = EditEdit[*flushEntity](c, editedEntity).TrackedEntity()
	editedEntity.Name = "rounding floats"
	editedEntity.City = "rounding floats"
	editedEntity.Float64 = 1.123456
	editedEntity.Decimal = 1.123
	floatNullable = 1.1234
	editedEntity.FloatNullable = &floatNullable
	decimalNullable = 1.126
	editedEntity.DecimalNullable = &decimalNullable
	assert.NoError(t, c.Flush())
	assert.Len(t, loggerDB.Logs, 1)
	assert.Equal(t, 1.12346, editedEntity.Float64)
	assert.Equal(t, 1.12, editedEntity.Decimal)
	assert.Equal(t, 1.123, *editedEntity.FloatNullable)
	assert.Equal(t, 1.13, *editedEntity.DecimalNullable)
	loggerDB.Clear()

	// same floats
	editedEntity = EditEdit[*flushEntity](c, editedEntity).TrackedEntity()
	editedEntity.Float64 = 1.123456
	editedEntity.Decimal = 1.123
	floatNullable = 1.1234
	editedEntity.FloatNullable = &floatNullable
	decimalNullable = 1.126
	editedEntity.DecimalNullable = &decimalNullable
	assert.NoError(t, c.Flush())
	assert.Len(t, loggerDB.Logs, 0)

	// same set
	editedEntity = EditEdit[*flushEntity](c, editedEntity).TrackedEntity()
	editedEntity.SetNullable = testSet{testEnumDefinition.C, testEnumDefinition.B}
	editedEntity.SetNotNull = testSet{testEnumDefinition.C, testEnumDefinition.A}
	assert.NoError(t, c.Flush())
	assert.Len(t, loggerDB.Logs, 0)
	editedEntity = EditEdit[*flushEntity](c, editedEntity).TrackedEntity()
	editedEntity.SetNullable = testSet{testEnumDefinition.C, testEnumDefinition.B, testEnumDefinition.B}
	editedEntity.SetNotNull = testSet{testEnumDefinition.C, testEnumDefinition.A, testEnumDefinition.A}
	assert.NoError(t, c.Flush())
	assert.Len(t, loggerDB.Logs, 0)

	// invalid values

	// empty string
	editedEntity = EditEdit[*flushEntity](c, editedEntity).TrackedEntity()
	editedEntity.Name = ""
	err = c.Flush()
	assert.EqualError(t, err, "[Name] empty string not allowed")
	assert.Equal(t, "Name", err.(*BindError).Field)
	c.ClearFlush()

	// string too long
	editedEntity = EditEdit[*flushEntity](c, editedEntity).TrackedEntity()
	editedEntity.Name = strings.Repeat("a", 256)
	err = c.Flush()
	assert.EqualError(t, err, "[Name] text too long, max 255 allowed")
	assert.Equal(t, "Name", err.(*BindError).Field)
	c.ClearFlush()
	editedEntity = EditEdit[*flushEntity](c, editedEntity).TrackedEntity()
	editedEntity.City = strings.Repeat("a", 41)
	err = c.Flush()
	assert.EqualError(t, err, "[City] text too long, max 40 allowed")
	editedEntity.City = strings.Repeat("a", 40)
	editedEntity.Name = "String to long"
	assert.NoError(t, c.Flush())

	// invalid decimal
	editedEntity = EditEdit[*flushEntity](c, editedEntity).TrackedEntity()
	editedEntity.Decimal = 1234
	err = c.Flush()
	assert.EqualError(t, err, "[Decimal] decimal size too big, max 3 allowed")
	assert.Equal(t, "Decimal", err.(*BindError).Field)
	c.ClearFlush()
	editedEntity = EditEdit[*flushEntity](c, editedEntity).TrackedEntity()
	editedEntity.Decimal = 123
	decimalNullable = 1234
	editedEntity.DecimalNullable = &decimalNullable
	err = c.Flush()
	assert.EqualError(t, err, "[DecimalNullable] decimal size too big, max 3 allowed")
	assert.Equal(t, "DecimalNullable", err.(*BindError).Field)
	c.ClearFlush()
	decimalNullable = 123
	editedEntity.DecimalNullable = &decimalNullable

	// float signed
	editedEntity = EditEdit[*flushEntity](c, editedEntity).TrackedEntity()
	editedEntity.Float64Unsigned = -1
	err = c.Flush()
	assert.EqualError(t, err, "[Float64Unsigned] negative value not allowed")
	assert.Equal(t, "Float64Unsigned", err.(*BindError).Field)
	editedEntity.Float64Unsigned = 1
	floatNullable = -1
	editedEntity.FloatNullable = &floatNullable
	err = c.Flush()
	assert.EqualError(t, err, "[FloatNullable] negative value not allowed")
	assert.Equal(t, "FloatNullable", err.(*BindError).Field)
	c.ClearFlush()
	floatNullable = 1
	editedEntity.FloatNullable = &floatNullable

	// invalid enum, set
	editedEntity = EditEdit[*flushEntity](c, editedEntity).TrackedEntity()
	editedEntity.EnumNotNull = ""
	err = c.Flush()
	assert.EqualError(t, err, "[EnumNotNull] empty value not allowed")
	assert.Equal(t, "EnumNotNull", err.(*BindError).Field)
	editedEntity.EnumNotNull = "invalid"
	err = c.Flush()
	assert.EqualError(t, err, "[EnumNotNull] invalid value: invalid")
	assert.Equal(t, "EnumNotNull", err.(*BindError).Field)
	editedEntity.EnumNotNull = testEnumDefinition.C
	editedEntity.SetNotNull = nil
	err = c.Flush()
	assert.EqualError(t, err, "[SetNotNull] empty value not allowed")
	assert.Equal(t, "SetNotNull", err.(*BindError).Field)
	editedEntity.SetNotNull = testSet{}
	err = c.Flush()
	assert.EqualError(t, err, "[SetNotNull] empty value not allowed")
	assert.Equal(t, "SetNotNull", err.(*BindError).Field)
	editedEntity.SetNotNull = testSet{"invalid"}
	err = c.Flush()
	assert.EqualError(t, err, "[SetNotNull] invalid value: invalid")
	assert.Equal(t, "SetNotNull", err.(*BindError).Field)
	editedEntity.SetNotNull = testSet{testEnumDefinition.B}
	c.ClearFlush()

	// Time
	editedEntity = EditEdit[*flushEntity](c, editedEntity).TrackedEntity()
	editedEntity.Time = time.Now().Local()
	err = c.Flush()
	assert.EqualError(t, err, "[Time] time must be in UTC location")
	assert.Equal(t, "Time", err.(*BindError).Field)
	editedEntity.Time = newEntity.Time.UTC()
	editedEntity.TimeWithTime = time.Now().Local()
	err = c.Flush()
	assert.EqualError(t, err, "[TimeWithTime] time must be in UTC location")
	assert.Equal(t, "TimeWithTime", err.(*BindError).Field)

	// nullable times
	editedEntity.TimeWithTime = editedEntity.Time.UTC()
	timeNullable = time.Now().Local()
	editedEntity.TimeNullable = &timeNullable
	err = c.Flush()
	assert.EqualError(t, err, "[TimeNullable] time must be in UTC location")
	assert.Equal(t, "TimeNullable", err.(*BindError).Field)
	timeWithTimeNullable = time.Now().Local()
	timeNullable = time.Now().UTC()
	editedEntity.TimeNullable = &timeNullable
	editedEntity.TimeWithTimeNullable = &timeWithTimeNullable
	err = c.Flush()
	assert.EqualError(t, err, "[TimeWithTimeNullable] time must be in UTC location")
	assert.Equal(t, "TimeWithTimeNullable", err.(*BindError).Field)
	timeWithTimeNullable = time.Now().UTC()
	editedEntity.TimeWithTimeNullable = &timeWithTimeNullable
	c.ClearFlush()

	// duplicated key
	newEntity = NewEntity[*flushEntity](c).TrackedEntity()
	newEntity.ReferenceRequired = NewReference[*flushEntityReference](reference.ID)
	newEntity.Name = "Name 2"
	assert.NoError(t, c.Flush())

	editedEntity = EditEdit[*flushEntity](c, editedEntity).TrackedEntity()
	editedEntity.Name = "Name 2"
	err = c.Flush()
	assert.EqualError(t, err, "duplicated value for unique index 'name'")
	assert.Equal(t, newEntity.ID, err.(*DuplicatedKeyBindError).ID)
	assert.Equal(t, "name", err.(*DuplicatedKeyBindError).Index)
	assert.Equal(t, []string{"Name"}, err.(*DuplicatedKeyBindError).Columns)
	c.ClearFlush()

	editedEntity = EditEdit[*flushEntity](c, newEntity).TrackedEntity()
	editedEntity.Name = "Name 3"
	assert.NoError(t, c.Flush())

	newEntity = NewEntity[*flushEntity](c).TrackedEntity()
	newEntity.ReferenceRequired = NewReference[*flushEntityReference](reference.ID)
	newEntity.Name = "Name 2"
	assert.NoError(t, c.Flush())
}
