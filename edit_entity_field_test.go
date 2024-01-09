package beeorm

import (
	"fmt"
	"math"
	"testing"
	"time"

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
	Name          string `orm:"length=10;required;unique=Name"`
	Uint          uint16 `orm:"unique=Multi"`
	Int           int16  `orm:"unique=Multi:2"`
	UintNullable  *uint16
	IntNullable   *int16
	Level1        updateSubField
	Reference     *Reference[updateEntityReference]
	Enum          testEnum
	Set           []testEnum
	Blob          []uint8
	Bool          bool
	BoolNullable  *bool
	Float         float64    `orm:"precision=2"`
	Decimal       float64    `orm:"decimal=5,2;unsigned"`
	FloatNullable *float32   `orm:"precision=2"`
	Time          time.Time  `orm:"time"`
	TimeNullable  *time.Time `orm:"time"`
	Date          time.Time
	DateNullable  *time.Time
}

func TestUpdateFieldExecuteNoCache(t *testing.T) {
	testUpdateFieldExecute(t, false, false, false)
}

func TestUpdateFieldExecuteLocalCache(t *testing.T) {
	testUpdateFieldExecute(t, false, true, false)
}

func TestUpdateFieldExecuteRedis(t *testing.T) {
	testUpdateFieldExecute(t, false, false, true)
}

func TestUpdateFieldExecuteLocalCacheRedis(t *testing.T) {
	testUpdateFieldExecute(t, false, true, true)
}

func TestUpdateFieldExecuteNoCacheAsync(t *testing.T) {
	testUpdateFieldExecute(t, true, false, false)
}

func TestUpdateFieldExecuteLocalCacheAsync(t *testing.T) {
	testUpdateFieldExecute(t, true, true, false)
}

func TestUpdateFieldExecuteRedisAsync(t *testing.T) {
	testUpdateFieldExecute(t, true, false, true)
}

func TestUpdateFieldExecuteLocalCacheRedisAsync(t *testing.T) {
	testUpdateFieldExecute(t, true, true, true)
}

func testUpdateFieldExecute(t *testing.T, async, local, redis bool) {
	var entity *updateEntity
	var reference *updateEntityReference
	orm := PrepareTables(t, NewRegistry(), entity, reference)

	schema := GetEntitySchema[updateEntity](orm)
	schema.DisableCache(!local, !redis)

	var ids []uint64
	for i := 1; i <= 10; i++ {
		entity = NewEntity[updateEntity](orm)
		entity.Uint = uint16(i)
		entity.Int = int16(i)
		entity.Name = fmt.Sprintf("name %d", i)
		entity.Level1.SubName = fmt.Sprintf("sub name %d", i)
		entity.Level1.Reference = &Reference[updateEntityReference]{ID: 1}
		entity.Level1.Enum = testEnumDefinition.A
		entity.Level1.Set = []testEnum{testEnumDefinition.A}
		ids = append(ids, entity.ID)
	}
	err := orm.Flush()
	assert.NoError(t, err)

	/* string */
	entity, _ = GetByID[updateEntity](orm, ids[0])
	err = runEditEntityField(orm, entity, "Name", "New", async)
	assert.NoError(t, err)
	assert.Equal(t, "New", entity.Name)
	entity, _ = GetByID[updateEntity](orm, ids[0])
	assert.Equal(t, "New", entity.Name)

	err = runEditEntityField(orm, entity, "Level1SubName", "Sub New", async)
	assert.NoError(t, err)
	assert.Equal(t, "Sub New", entity.Level1.SubName)
	entity, _ = GetByID[updateEntity](orm, ids[0])
	assert.Equal(t, "Sub New", entity.Level1.SubName)

	err = runEditEntityField(orm, entity, "Level1SubName", "", async)
	assert.NoError(t, err)
	assert.Equal(t, "", entity.Level1.SubName)
	entity, _ = GetByID[updateEntity](orm, ids[0])
	assert.Equal(t, "", entity.Level1.SubName)

	entity, _ = GetByID[updateEntity](orm, ids[1])
	err = runEditEntityField(orm, entity, "Level1SubName", nil, async)
	assert.NoError(t, err)
	assert.Equal(t, "", entity.Level1.SubName)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Equal(t, "", entity.Level1.SubName)

	err = runEditEntityField(orm, entity, "Name", "123456789ab", async)
	assert.EqualError(t, err, "[Name] text too long, max 10 allowed")

	err = runEditEntityField(orm, entity, "Name", "", async)
	assert.EqualError(t, err, "[Name] empty string not allowed")

	/* uint */
	intValues := []any{"1", float32(2), float64(3), uint8(4), uint16(5), uint32(6), uint(7), uint64(8), int8(9), int16(10), int32(11), int64(12), 13}
	for i, val := range intValues {
		err = runEditEntityField(orm, entity, "Uint", val, async)
		assert.NoError(t, err)
		assert.Equal(t, uint16(i+1), entity.Uint)
		entity, _ = GetByID[updateEntity](orm, ids[1])
		assert.Equal(t, uint16(i+1), entity.Uint)
		err = runEditEntityField(orm, entity, "Level1Uint", val, async)
		assert.NoError(t, err)
		assert.Equal(t, uint16(i+1), entity.Level1.Uint)
		entity, _ = GetByID[updateEntity](orm, ids[1])
		assert.Equal(t, uint16(i+1), entity.Level1.Uint)
	}
	err = runEditEntityField(orm, entity, "Uint", -14, async)
	assert.EqualError(t, err, "[Uint] negative number -14 not allowed")
	err = runEditEntityField(orm, entity, "Uint", math.MaxUint16+1, async)
	assert.EqualError(t, err, "[Uint] value 65536 exceeded max allowed value")
	err = runEditEntityField(orm, entity, "Uint", "invalid", async)
	assert.EqualError(t, err, "[Uint] invalid number invalid")

	/* int */
	for i, val := range intValues {
		err = runEditEntityField(orm, entity, "Int", val, async)
		assert.NoError(t, err)
		assert.Equal(t, int16(i+1), entity.Int)
		entity, _ = GetByID[updateEntity](orm, ids[1])
		assert.Equal(t, int16(i+1), entity.Int)
		err = runEditEntityField(orm, entity, "Level1Int", val, async)
		assert.NoError(t, err)
		assert.Equal(t, int16(i+1), entity.Level1.Int)
		entity, _ = GetByID[updateEntity](orm, ids[1])
		assert.Equal(t, int16(i+1), entity.Level1.Int)
	}
	err = runEditEntityField(orm, entity, "Int", math.MaxInt16+1, async)
	assert.EqualError(t, err, "[Int] value 32768 exceeded max allowed value")
	err = runEditEntityField(orm, entity, "Int", math.MinInt16-1, async)
	assert.EqualError(t, err, "[Int] value -32769 exceeded min allowed value")
	err = runEditEntityField(orm, entity, "Int", "invalid", async)
	assert.EqualError(t, err, "[Int] invalid number invalid")

	/* *uint */
	for i, val := range intValues {
		err = runEditEntityField(orm, entity, "UintNullable", val, async)
		assert.NoError(t, err)
		assert.Equal(t, uint16(i+1), *entity.UintNullable)
		entity, _ = GetByID[updateEntity](orm, ids[1])
		assert.Equal(t, uint16(i+1), *entity.UintNullable)
		err = runEditEntityField(orm, entity, "Level1UintNullable", val, async)
		assert.NoError(t, err)
		assert.Equal(t, uint16(i+1), *entity.Level1.UintNullable)
		entity, _ = GetByID[updateEntity](orm, ids[1])
		assert.Equal(t, uint16(i+1), *entity.Level1.UintNullable)
	}
	err = runEditEntityField(orm, entity, "UintNullable", nil, async)
	assert.NoError(t, err)
	assert.Nil(t, entity.UintNullable)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Nil(t, entity.UintNullable)

	/* *int */
	for i, val := range intValues {
		err = runEditEntityField(orm, entity, "IntNullable", val, async)
		assert.NoError(t, err)
		assert.Equal(t, int16(i+1), *entity.IntNullable)
		entity, _ = GetByID[updateEntity](orm, ids[1])
		assert.Equal(t, int16(i+1), *entity.IntNullable)
		err = runEditEntityField(orm, entity, "Level1IntNullable", val, async)
		assert.NoError(t, err)
		assert.Equal(t, int16(i+1), *entity.Level1.IntNullable)
		entity, _ = GetByID[updateEntity](orm, ids[1])
		assert.Equal(t, int16(i+1), *entity.Level1.IntNullable)
	}
	err = runEditEntityField(orm, entity, "IntNullable", nil, async)
	assert.NoError(t, err)
	assert.Nil(t, entity.IntNullable)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Nil(t, entity.IntNullable)

	/* reference */
	for i, val := range intValues {
		err = runEditEntityField(orm, entity, "Reference", val, async)
		assert.NoError(t, err)
		assert.Equal(t, uint64(i+1), entity.Reference.ID)
		entity, _ = GetByID[updateEntity](orm, ids[1])
		assert.Equal(t, uint64(i+1), entity.Reference.ID)
		err = runEditEntityField(orm, entity, "Level1Reference", val, async)
		assert.NoError(t, err)
		assert.Equal(t, uint64(i+1), entity.Level1.Reference.ID)
		entity, _ = GetByID[updateEntity](orm, ids[1])
		assert.Equal(t, uint64(i+1), entity.Level1.Reference.ID)
	}
	err = runEditEntityField(orm, entity, "Reference", &Reference[updateEntityReference]{ID: 20}, async)
	assert.NoError(t, err)
	assert.Equal(t, uint64(20), entity.Reference.ID)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Equal(t, uint64(20), entity.Reference.ID)
	err = runEditEntityField(orm, entity, "Reference", &Reference[updateEntityReference]{ID: 0}, async)
	assert.NoError(t, err)
	assert.Nil(t, entity.Reference)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Nil(t, entity.Reference)
	_ = runEditEntityField(orm, entity, "Reference", 20, async)
	err = runEditEntityField(orm, entity, "Reference", nil, async)
	assert.NoError(t, err)
	assert.Nil(t, entity.Reference)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Nil(t, entity.Reference)
	_ = runEditEntityField(orm, entity, "Reference", 20, async)
	err = runEditEntityField(orm, entity, "Reference", 0, async)
	assert.NoError(t, err)
	assert.Nil(t, entity.Reference)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Nil(t, entity.Reference)
	err = runEditEntityField(orm, entity, "Reference", "invalid", async)
	assert.EqualError(t, err, "[Reference] invalid number invalid")

	/* enum */
	err = runEditEntityField(orm, entity, "Enum", testEnumDefinition.B, async)
	assert.NoError(t, err)
	assert.Equal(t, testEnumDefinition.B, entity.Enum)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Equal(t, testEnumDefinition.B, entity.Enum)
	err = runEditEntityField(orm, entity, "Enum", "c", async)
	assert.NoError(t, err)
	assert.Equal(t, testEnumDefinition.C, entity.Enum)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Equal(t, testEnumDefinition.C, entity.Enum)
	err = runEditEntityField(orm, entity, "Enum", "", async)
	assert.NoError(t, err)
	assert.Equal(t, testEnum(""), entity.Enum)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Equal(t, testEnum(""), entity.Enum)
	_ = runEditEntityField(orm, entity, "Enum", testEnumDefinition.B, async)
	err = runEditEntityField(orm, entity, "Enum", nil, async)
	assert.NoError(t, err)
	assert.Equal(t, testEnum(""), entity.Enum)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Equal(t, testEnum(""), entity.Enum)
	err = runEditEntityField(orm, entity, "Enum", "invalid", async)
	assert.EqualError(t, err, "[Enum] invalid value: invalid")
	err = runEditEntityField(orm, entity, "Level1Enum", nil, async)
	assert.EqualError(t, err, "[Level1Enum] nil is not allowed")
	err = runEditEntityField(orm, entity, "Level1Enum", "", async)
	assert.EqualError(t, err, "[Level1Enum] nil is not allowed")

	/* set */
	err = runEditEntityField(orm, entity, "Set", testEnumDefinition.B, async)
	assert.NoError(t, err)
	assert.Equal(t, []testEnum{testEnumDefinition.B}, entity.Set)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Equal(t, []testEnum{testEnumDefinition.B}, entity.Set)
	err = runEditEntityField(orm, entity, "Set", []testEnum{testEnumDefinition.A, testEnumDefinition.C}, async)
	assert.NoError(t, err)
	assert.Equal(t, []testEnum{testEnumDefinition.A, testEnumDefinition.C}, entity.Set)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Equal(t, []testEnum{testEnumDefinition.A, testEnumDefinition.C}, entity.Set)
	err = runEditEntityField(orm, entity, "Set", nil, async)
	assert.NoError(t, err)
	assert.Nil(t, entity.Set)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Nil(t, entity.Set)
	_ = runEditEntityField(orm, entity, "Set", testEnumDefinition.B, async)
	err = runEditEntityField(orm, entity, "Set", []testEnum{}, async)
	assert.NoError(t, err)
	assert.Nil(t, entity.Set)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Nil(t, entity.Set)
	err = runEditEntityField(orm, entity, "Set", "invalid", async)
	assert.EqualError(t, err, "[Set] invalid value: invalid")
	err = runEditEntityField(orm, entity, "Level1Set", "", async)
	assert.EqualError(t, err, "[Level1Set] nil is not allowed")

	/* byte */
	err = runEditEntityField(orm, entity, "Blob", "hello", async)
	assert.NoError(t, err)
	assert.Equal(t, "hello", string(entity.Blob))
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Equal(t, "hello", string(entity.Blob))
	err = runEditEntityField(orm, entity, "Blob", []byte("hello 2"), async)
	assert.NoError(t, err)
	assert.Equal(t, "hello 2", string(entity.Blob))
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Equal(t, "hello 2", string(entity.Blob))
	err = runEditEntityField(orm, entity, "Blob", nil, async)
	assert.NoError(t, err)
	assert.Nil(t, entity.Blob)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Nil(t, entity.Blob)

	/* boolean */
	err = runEditEntityField(orm, entity, "Bool", true, async)
	assert.NoError(t, err)
	assert.True(t, entity.Bool)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.True(t, entity.Bool)
	err = runEditEntityField(orm, entity, "Bool", false, async)
	assert.NoError(t, err)
	assert.False(t, entity.Bool)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.False(t, entity.Bool)
	err = runEditEntityField(orm, entity, "Bool", 1, async)
	assert.NoError(t, err)
	assert.True(t, entity.Bool)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.True(t, entity.Bool)
	_ = runEditEntityField(orm, entity, "Bool", false, async)
	err = runEditEntityField(orm, entity, "Bool", "true", async)
	assert.NoError(t, err)
	assert.True(t, entity.Bool)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.True(t, entity.Bool)
	err = runEditEntityField(orm, entity, "Bool", []string{}, async)
	assert.EqualError(t, err, "[Bool] invalid value")

	/* *boolean */
	err = runEditEntityField(orm, entity, "BoolNullable", true, async)
	assert.NoError(t, err)
	assert.True(t, *entity.BoolNullable)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.True(t, *entity.BoolNullable)
	err = runEditEntityField(orm, entity, "BoolNullable", nil, async)
	assert.NoError(t, err)
	assert.Nil(t, entity.BoolNullable)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Nil(t, entity.BoolNullable)
	err = runEditEntityField(orm, entity, "BoolNullable", []string{}, async)
	assert.EqualError(t, err, "[BoolNullable] invalid value")
	/* float */
	for i, val := range intValues {
		err = runEditEntityField(orm, entity, "Float", val, async)
		assert.NoError(t, err)
		assert.Equal(t, float64(i+1), entity.Float)
		entity, _ = GetByID[updateEntity](orm, ids[1])
		assert.Equal(t, float64(i+1), entity.Float)
	}
	err = runEditEntityField(orm, entity, "Float", 12.13, async)
	assert.NoError(t, err)
	assert.Equal(t, 12.13, entity.Float)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Equal(t, 12.13, entity.Float)
	err = runEditEntityField(orm, entity, "Float", "12.14", async)
	assert.NoError(t, err)
	assert.Equal(t, 12.14, entity.Float)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Equal(t, 12.14, entity.Float)
	err = runEditEntityField(orm, entity, "Float", 12.136, async)
	assert.NoError(t, err)
	assert.Equal(t, 12.14, entity.Float)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Equal(t, 12.14, entity.Float)
	err = runEditEntityField(orm, entity, "Decimal", -1, async)
	assert.EqualError(t, err, "[Decimal] negative number -1 not allowed")
	err = runEditEntityField(orm, entity, "Decimal", 1234.45, async)
	assert.EqualError(t, err, "[Decimal] decimal size too big, max 3 allowed")
	err = runEditEntityField(orm, entity, "Decimal", "invalid", async)
	assert.EqualError(t, err, "[Decimal] invalid number invalid")

	/* *float */
	for i, val := range intValues {
		err = runEditEntityField(orm, entity, "FloatNullable", val, async)
		assert.NoError(t, err)
		assert.Equal(t, float32(i+1), *entity.FloatNullable)
		entity, _ = GetByID[updateEntity](orm, ids[1])
		assert.Equal(t, float32(i+1), *entity.FloatNullable)
	}
	err = runEditEntityField(orm, entity, "FloatNullable", "invalid", async)
	assert.EqualError(t, err, "[FloatNullable] invalid number invalid")

	/* time */
	date := time.Date(2023, 11, 12, 22, 12, 34, 4, time.UTC)
	err = runEditEntityField(orm, entity, "Time", date, async)
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2023, 11, 12, 22, 12, 34, 0, time.UTC), entity.Time)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Equal(t, time.Date(2023, 11, 12, 22, 12, 34, 0, time.UTC), entity.Time)
	err = runEditEntityField(orm, entity, "Time", "2024-02-03 11:44:55", async)
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2024, 2, 3, 11, 44, 55, 0, time.UTC), entity.Time)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Equal(t, time.Date(2024, 2, 3, 11, 44, 55, 0, time.UTC), entity.Time)
	err = runEditEntityField(orm, entity, "Time", "invalid", async)
	assert.EqualError(t, err, "[Time] invalid time invalid")
	l, _ := time.LoadLocation("Africa/Asmara")
	err = runEditEntityField(orm, entity, "Time", time.Now().In(l), async)
	assert.EqualError(t, err, "[Time] time must be in UTC location")

	/* *time */
	err = runEditEntityField(orm, entity, "TimeNullable", date, async)
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2023, 11, 12, 22, 12, 34, 0, time.UTC), *entity.TimeNullable)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Equal(t, time.Date(2023, 11, 12, 22, 12, 34, 0, time.UTC), *entity.TimeNullable)
	err = runEditEntityField(orm, entity, "TimeNullable", nil, async)
	assert.NoError(t, err)
	assert.Nil(t, entity.TimeNullable)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Nil(t, entity.TimeNullable)

	/* date */
	err = runEditEntityField(orm, entity, "Date", date, async)
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2023, 11, 12, 0, 0, 0, 0, time.UTC), entity.Date)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Equal(t, time.Date(2023, 11, 12, 0, 0, 0, 0, time.UTC), entity.Date)

	/* *date */
	err = runEditEntityField(orm, entity, "DateNullable", date, async)
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2023, 11, 12, 0, 0, 0, 0, time.UTC), *entity.DateNullable)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Equal(t, time.Date(2023, 11, 12, 0, 0, 0, 0, time.UTC), *entity.DateNullable)
	err = runEditEntityField(orm, entity, "DateNullable", nil, async)
	assert.NoError(t, err)
	assert.Nil(t, entity.DateNullable)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Nil(t, entity.DateNullable)

	/* many fields */
	assert.NoError(t, EditEntityField(orm, entity, "Name", "A"))
	assert.NoError(t, runEditEntityField(orm, entity, "Int", 12, async))
	assert.Equal(t, "A", entity.Name)
	assert.Equal(t, int16(12), entity.Int)
	entity, _ = GetByID[updateEntity](orm, ids[1])
	assert.Equal(t, "A", entity.Name)
	assert.Equal(t, int16(12), entity.Int)

	/* unique index */
	err = runEditEntityField(orm, entity, "Name", "name 3", async)
	assert.EqualError(t, err, "duplicated value for unique index 'Name'")
	orm.ClearFlush()
	err = runEditEntityField(orm, entity, "Name", "name 100", async)
	assert.NoError(t, err)
	entity, _ = GetByUniqueIndex[updateEntity](orm, "Name", "name 100")
	assert.NotNil(t, entity)
	assert.Equal(t, ids[1], entity.ID)
	err = runEditEntityField(orm, entity, "Int", 100, async)
	assert.NoError(t, err)
	entity, _ = GetByUniqueIndex[updateEntity](orm, "Multi", 13, 100)
	assert.NotNil(t, entity)
	assert.Equal(t, ids[1], entity.ID)
}

func runEditEntityField(orm ORM, entity *updateEntity, field string, value any, async bool) error {
	err := EditEntityField(orm, entity, field, value)
	if err != nil {
		return err
	}
	if async {
		err = orm.FlushAsync()
		if err != nil {
			return err
		}
		stop := ConsumeAsyncBuffer(orm, func(err error) {
			panic(err)
		})
		stop()
		return ConsumeAsyncFlushEvents(orm, false)
	}
	return orm.Flush()
}
