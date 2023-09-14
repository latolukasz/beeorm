package beeorm

import (
	"github.com/stretchr/testify/assert"
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

var testSetDefinition = struct {
	D testEnum
	E testEnum
	F testEnum
}{
	D: "d",
	E: "e",
	F: "f",
}

type testEnum Enum
type testSet Set

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
	City                 string `orm:"unique=city"`
	Name                 string `orm:"unique=name;required"`
	Age                  int
	Uint                 uint
	UintNullable         *uint
	IntNullable          *int
	Year                 uint16  `orm:"year"`
	YearNullable         *uint16 `orm:"year"`
	BoolNullable         *bool
	FloatNullable        *float64 `orm:"precision=10"`
	Float32Nullable      *float32 `orm:"precision=4"`
	SetNullable          testSet
	SetNotNull           testSet `orm:"required"`
	EnumNullable         testEnum
	EnumNotNull          testEnum `orm:"required"`
	Ignored              []string `orm:"ignore"`
	Blob                 []uint8
	Bool                 bool
	Float64              float64  `orm:"precision=10"`
	Decimal              float64  `orm:"decimal=5,2"`
	DecimalNullable      *float64 `orm:"decimal=5,2"`
	Float64Default       float64  `orm:"unsigned"`
	Float64Signed        float64
	CachedQuery          *CachedQuery
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
	var entity *flushEntity
	registry := &Registry{}
	c := PrepareTables(t, registry, 5, 6, "", entity)

	schema := GetEntitySchema[*flushEntity](c)
	schema.DisableCache(!local, !redis)

	newEntity := NewEntity[*flushEntity](c).TrackedEntity()
	assert.NotEmpty(t, newEntity.ID)
	newEntity.Name = "Hello"
	newEntity.EnumNotNull = testEnumDefinition.B
	newEntity.SetNotNull = testSet{Enum(testEnumDefinition.A), Enum(testEnumDefinition.C)}
	c.Flush()
}
