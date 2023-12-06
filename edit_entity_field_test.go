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
	testUpdateFieldExecute(t, true, false, false, false)
}

func TestUpdateFieldExecuteLocalCache(t *testing.T) {
	testUpdateFieldExecute(t, true, false, true, false)
}

func TestUpdateFieldExecuteRedis(t *testing.T) {
	testUpdateFieldExecute(t, true, false, false, true)
}

func TestUpdateFieldExecuteLocalCacheRedis(t *testing.T) {
	testUpdateFieldExecute(t, true, false, true, true)
}

func TestUpdateFieldExecuteNoCacheAsync(t *testing.T) {
	testUpdateFieldExecute(t, true, true, false, false)
}

func TestUpdateFieldExecuteLocalCacheAsync(t *testing.T) {
	testUpdateFieldExecute(t, true, true, true, false)
}

func TestUpdateFieldExecuteRedisAsync(t *testing.T) {
	testUpdateFieldExecute(t, true, true, false, true)
}

func TestUpdateFieldExecuteLocalCacheRedisAsync(t *testing.T) {
	testUpdateFieldExecute(t, true, true, true, true)
}

func testUpdateFieldExecute(t *testing.T, execute, async, local, redis bool) {
	var entity *updateEntity
	var reference *updateEntityReference
	c := PrepareTables(t, NewRegistry(), entity, reference)

	schema := GetEntitySchema[updateEntity](c)
	schema.DisableCache(!local, !redis)

	var ids []uint64
	for i := 1; i <= 10; i++ {
		entity = NewEntity[updateEntity](c)
		entity.Uint = uint16(i)
		entity.Int = int16(i)
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
	err = runEditEntityField(c, entity, "Name", "New", async, execute)
	assert.NoError(t, err)
	assert.Equal(t, "New", entity.Name)
	entity = GetByID[updateEntity](c, ids[0])
	assert.Equal(t, "New", entity.Name)

	err = runEditEntityField(c, entity, "Level1SubName", "Sub New", async, execute)
	assert.NoError(t, err)
	assert.Equal(t, "Sub New", entity.Level1.SubName)
	entity = GetByID[updateEntity](c, ids[0])
	assert.Equal(t, "Sub New", entity.Level1.SubName)

	err = runEditEntityField(c, entity, "Level1SubName", "", async, execute)
	assert.NoError(t, err)
	assert.Equal(t, "", entity.Level1.SubName)
	entity = GetByID[updateEntity](c, ids[0])
	assert.Equal(t, "", entity.Level1.SubName)

	entity = GetByID[updateEntity](c, ids[1])
	err = runEditEntityField(c, entity, "Level1SubName", nil, async, execute)
	assert.NoError(t, err)
	assert.Equal(t, "", entity.Level1.SubName)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, "", entity.Level1.SubName)

	err = runEditEntityField(c, entity, "Name", "123456789ab", async, execute)
	assert.EqualError(t, err, "[Name] text too long, max 10 allowed")

	err = runEditEntityField(c, entity, "Name", "", async, execute)
	assert.EqualError(t, err, "[Name] empty string not allowed")

	/* uint */
	intValues := []any{"1", float32(2), float64(3), uint8(4), uint16(5), uint32(6), uint(7), uint64(8), int8(9), int16(10), int32(11), int64(12), 13}
	for i, val := range intValues {
		err = runEditEntityField(c, entity, "Uint", val, async, execute)
		assert.NoError(t, err)
		assert.Equal(t, uint16(i+1), entity.Uint)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, uint16(i+1), entity.Uint)
		err = runEditEntityField(c, entity, "Level1Uint", val, async, execute)
		assert.NoError(t, err)
		assert.Equal(t, uint16(i+1), entity.Level1.Uint)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, uint16(i+1), entity.Level1.Uint)
	}
	err = runEditEntityField(c, entity, "Uint", -14, async, execute)
	assert.EqualError(t, err, "[Uint] negative number -14 not allowed")
	err = runEditEntityField(c, entity, "Uint", math.MaxUint16+1, async, execute)
	assert.EqualError(t, err, "[Uint] value 65536 exceeded max allowed value")
	err = runEditEntityField(c, entity, "Uint", "invalid", async, execute)
	assert.EqualError(t, err, "[Uint] invalid number invalid")

	/* int */
	for i, val := range intValues {
		err = runEditEntityField(c, entity, "Int", val, async, execute)
		assert.NoError(t, err)
		assert.Equal(t, int16(i+1), entity.Int)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, int16(i+1), entity.Int)
		err = runEditEntityField(c, entity, "Level1Int", val, async, execute)
		assert.NoError(t, err)
		assert.Equal(t, int16(i+1), entity.Level1.Int)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, int16(i+1), entity.Level1.Int)
	}
	err = runEditEntityField(c, entity, "Int", math.MaxInt16+1, async, execute)
	assert.EqualError(t, err, "[Int] value 32768 exceeded max allowed value")
	err = runEditEntityField(c, entity, "Int", math.MinInt16-1, async, execute)
	assert.EqualError(t, err, "[Int] value -32769 exceeded min allowed value")
	err = runEditEntityField(c, entity, "Int", "invalid", async, execute)
	assert.EqualError(t, err, "[Int] invalid number invalid")

	/* *uint */
	for i, val := range intValues {
		err = runEditEntityField(c, entity, "UintNullable", val, async, execute)
		assert.NoError(t, err)
		assert.Equal(t, uint16(i+1), *entity.UintNullable)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, uint16(i+1), *entity.UintNullable)
		err = runEditEntityField(c, entity, "Level1UintNullable", val, async, execute)
		assert.NoError(t, err)
		assert.Equal(t, uint16(i+1), *entity.Level1.UintNullable)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, uint16(i+1), *entity.Level1.UintNullable)
	}
	err = runEditEntityField(c, entity, "UintNullable", nil, async, execute)
	assert.NoError(t, err)
	assert.Nil(t, entity.UintNullable)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Nil(t, entity.UintNullable)

	/* *int */
	for i, val := range intValues {
		err = runEditEntityField(c, entity, "IntNullable", val, async, execute)
		assert.NoError(t, err)
		assert.Equal(t, int16(i+1), *entity.IntNullable)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, int16(i+1), *entity.IntNullable)
		err = runEditEntityField(c, entity, "Level1IntNullable", val, async, execute)
		assert.NoError(t, err)
		assert.Equal(t, int16(i+1), *entity.Level1.IntNullable)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, int16(i+1), *entity.Level1.IntNullable)
	}
	err = runEditEntityField(c, entity, "IntNullable", nil, async, execute)
	assert.NoError(t, err)
	assert.Nil(t, entity.IntNullable)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Nil(t, entity.IntNullable)

	/* reference */
	for i, val := range intValues {
		err = runEditEntityField(c, entity, "Reference", val, async, execute)
		assert.NoError(t, err)
		assert.Equal(t, uint64(i+1), entity.Reference.ID)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, uint64(i+1), entity.Reference.ID)
		err = runEditEntityField(c, entity, "Level1Reference", val, async, execute)
		assert.NoError(t, err)
		assert.Equal(t, uint64(i+1), entity.Level1.Reference.ID)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, uint64(i+1), entity.Level1.Reference.ID)
	}
	err = runEditEntityField(c, entity, "Reference", &Reference[updateEntityReference]{ID: 20}, async, execute)
	assert.NoError(t, err)
	assert.Equal(t, uint64(20), entity.Reference.ID)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, uint64(20), entity.Reference.ID)
	err = runEditEntityField(c, entity, "Reference", &Reference[updateEntityReference]{ID: 0}, async, execute)
	assert.NoError(t, err)
	assert.Nil(t, entity.Reference)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Nil(t, entity.Reference)
	_ = runEditEntityField(c, entity, "Reference", 20, async, execute)
	err = runEditEntityField(c, entity, "Reference", nil, async, execute)
	assert.NoError(t, err)
	assert.Nil(t, entity.Reference)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Nil(t, entity.Reference)
	_ = runEditEntityField(c, entity, "Reference", 20, async, execute)
	err = runEditEntityField(c, entity, "Reference", 0, async, execute)
	assert.NoError(t, err)
	assert.Nil(t, entity.Reference)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Nil(t, entity.Reference)
	err = runEditEntityField(c, entity, "Reference", "invalid", async, execute)
	assert.EqualError(t, err, "[Reference] invalid number invalid")

	/* enum */
	err = runEditEntityField(c, entity, "Enum", testEnumDefinition.B, async, execute)
	assert.NoError(t, err)
	assert.Equal(t, testEnumDefinition.B, entity.Enum)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, testEnumDefinition.B, entity.Enum)
	err = runEditEntityField(c, entity, "Enum", "c", async, execute)
	assert.NoError(t, err)
	assert.Equal(t, testEnumDefinition.C, entity.Enum)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, testEnumDefinition.C, entity.Enum)
	err = runEditEntityField(c, entity, "Enum", "", async, execute)
	assert.NoError(t, err)
	assert.Equal(t, testEnum(""), entity.Enum)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, testEnum(""), entity.Enum)
	_ = runEditEntityField(c, entity, "Enum", testEnumDefinition.B, async, execute)
	err = runEditEntityField(c, entity, "Enum", nil, async, execute)
	assert.NoError(t, err)
	assert.Equal(t, testEnum(""), entity.Enum)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, testEnum(""), entity.Enum)
	err = runEditEntityField(c, entity, "Enum", "invalid", async, execute)
	assert.EqualError(t, err, "[Enum] invalid value: invalid")
	err = runEditEntityField(c, entity, "Level1Enum", nil, async, execute)
	assert.EqualError(t, err, "[Level1Enum] nil is not allowed")
	err = runEditEntityField(c, entity, "Level1Enum", "", async, execute)
	assert.EqualError(t, err, "[Level1Enum] nil is not allowed")

	/* set */
	err = runEditEntityField(c, entity, "Set", testEnumDefinition.B, async, execute)
	assert.NoError(t, err)
	assert.Equal(t, []testEnum{testEnumDefinition.B}, entity.Set)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, []testEnum{testEnumDefinition.B}, entity.Set)
	err = runEditEntityField(c, entity, "Set", []testEnum{testEnumDefinition.A, testEnumDefinition.C}, async, execute)
	assert.NoError(t, err)
	assert.Equal(t, []testEnum{testEnumDefinition.A, testEnumDefinition.C}, entity.Set)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, []testEnum{testEnumDefinition.A, testEnumDefinition.C}, entity.Set)
	err = runEditEntityField(c, entity, "Set", nil, async, execute)
	assert.NoError(t, err)
	assert.Nil(t, entity.Set)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Nil(t, entity.Set)
	_ = runEditEntityField(c, entity, "Set", testEnumDefinition.B, async, execute)
	err = runEditEntityField(c, entity, "Set", []testEnum{}, async, execute)
	assert.NoError(t, err)
	assert.Nil(t, entity.Set)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Nil(t, entity.Set)
	err = runEditEntityField(c, entity, "Set", "invalid", async, execute)
	assert.EqualError(t, err, "[Set] invalid value: invalid")
	err = runEditEntityField(c, entity, "Level1Set", "", async, execute)
	assert.EqualError(t, err, "[Level1Set] nil is not allowed")

	/* byte */
	err = runEditEntityField(c, entity, "Blob", "hello", async, execute)
	assert.NoError(t, err)
	assert.Equal(t, "hello", string(entity.Blob))
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, "hello", string(entity.Blob))
	err = runEditEntityField(c, entity, "Blob", []byte("hello 2"), async, execute)
	assert.NoError(t, err)
	assert.Equal(t, "hello 2", string(entity.Blob))
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, "hello 2", string(entity.Blob))
	err = runEditEntityField(c, entity, "Blob", nil, async, execute)
	assert.NoError(t, err)
	assert.Nil(t, entity.Blob)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Nil(t, entity.Blob)

	/* boolean */
	err = runEditEntityField(c, entity, "Bool", true, async, execute)
	assert.NoError(t, err)
	assert.True(t, entity.Bool)
	entity = GetByID[updateEntity](c, ids[1])
	assert.True(t, entity.Bool)
	err = runEditEntityField(c, entity, "Bool", false, async, execute)
	assert.NoError(t, err)
	assert.False(t, entity.Bool)
	entity = GetByID[updateEntity](c, ids[1])
	assert.False(t, entity.Bool)
	err = runEditEntityField(c, entity, "Bool", 1, async, execute)
	assert.NoError(t, err)
	assert.True(t, entity.Bool)
	entity = GetByID[updateEntity](c, ids[1])
	assert.True(t, entity.Bool)
	_ = runEditEntityField(c, entity, "Bool", false, async, execute)
	err = runEditEntityField(c, entity, "Bool", "true", async, execute)
	assert.NoError(t, err)
	assert.True(t, entity.Bool)
	entity = GetByID[updateEntity](c, ids[1])
	assert.True(t, entity.Bool)
	err = runEditEntityField(c, entity, "Bool", []string{}, async, execute)
	assert.EqualError(t, err, "[Bool] invalid value")

	/* *boolean */
	err = runEditEntityField(c, entity, "BoolNullable", true, async, execute)
	assert.NoError(t, err)
	assert.True(t, *entity.BoolNullable)
	entity = GetByID[updateEntity](c, ids[1])
	assert.True(t, *entity.BoolNullable)
	err = runEditEntityField(c, entity, "BoolNullable", nil, async, execute)
	assert.NoError(t, err)
	assert.Nil(t, entity.BoolNullable)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Nil(t, entity.BoolNullable)
	err = runEditEntityField(c, entity, "BoolNullable", []string{}, async, execute)
	assert.EqualError(t, err, "[BoolNullable] invalid value")

	/* float */
	for i, val := range intValues {
		err = runEditEntityField(c, entity, "Float", val, async, execute)
		assert.NoError(t, err)
		assert.Equal(t, float64(i+1), entity.Float)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, float64(i+1), entity.Float)
	}
	err = runEditEntityField(c, entity, "Float", 12.13, async, execute)
	assert.NoError(t, err)
	assert.Equal(t, 12.13, entity.Float)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, 12.13, entity.Float)
	err = runEditEntityField(c, entity, "Float", "12.14", async, execute)
	assert.NoError(t, err)
	assert.Equal(t, 12.14, entity.Float)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, 12.14, entity.Float)
	err = runEditEntityField(c, entity, "Float", 12.136, async, execute)
	assert.NoError(t, err)
	assert.Equal(t, 12.14, entity.Float)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, 12.14, entity.Float)
	err = runEditEntityField(c, entity, "Decimal", -1, async, execute)
	assert.EqualError(t, err, "[Decimal] negative number -1 not allowed")
	err = runEditEntityField(c, entity, "Decimal", 1234.45, async, execute)
	assert.EqualError(t, err, "[Decimal] decimal size too big, max 3 allowed")
	err = runEditEntityField(c, entity, "Decimal", "invalid", async, execute)
	assert.EqualError(t, err, "[Decimal] invalid number invalid")

	/* *float */
	for i, val := range intValues {
		err = runEditEntityField(c, entity, "FloatNullable", val, async, execute)
		assert.NoError(t, err)
		assert.Equal(t, float32(i+1), *entity.FloatNullable)
		entity = GetByID[updateEntity](c, ids[1])
		assert.Equal(t, float32(i+1), *entity.FloatNullable)
	}
	err = runEditEntityField(c, entity, "FloatNullable", "invalid", async, execute)
	assert.EqualError(t, err, "[FloatNullable] invalid number invalid")

	/* time */
	date := time.Date(2023, 11, 12, 22, 12, 34, 4, time.UTC)
	err = runEditEntityField(c, entity, "Time", date, async, execute)
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2023, 11, 12, 22, 12, 34, 0, time.UTC), entity.Time)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, time.Date(2023, 11, 12, 22, 12, 34, 0, time.UTC), entity.Time)
	err = runEditEntityField(c, entity, "Time", "2024-02-03 11:44:55", async, execute)
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2024, 2, 3, 11, 44, 55, 0, time.UTC), entity.Time)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, time.Date(2024, 2, 3, 11, 44, 55, 0, time.UTC), entity.Time)
	err = runEditEntityField(c, entity, "Time", "invalid", async, execute)
	assert.EqualError(t, err, "[Time] invalid time invalid")
	l, _ := time.LoadLocation("Africa/Asmara")
	err = runEditEntityField(c, entity, "Time", time.Now().In(l), async, execute)
	assert.EqualError(t, err, "[Time] time must be in UTC location")

	/* *time */
	err = runEditEntityField(c, entity, "TimeNullable", date, async, execute)
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2023, 11, 12, 22, 12, 34, 0, time.UTC), *entity.TimeNullable)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, time.Date(2023, 11, 12, 22, 12, 34, 0, time.UTC), *entity.TimeNullable)
	err = runEditEntityField(c, entity, "TimeNullable", nil, async, execute)
	assert.NoError(t, err)
	assert.Nil(t, entity.TimeNullable)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Nil(t, entity.TimeNullable)

	/* date */
	err = runEditEntityField(c, entity, "Date", date, async, execute)
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2023, 11, 12, 0, 0, 0, 0, time.UTC), entity.Date)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, time.Date(2023, 11, 12, 0, 0, 0, 0, time.UTC), entity.Date)

	/* *date */
	err = runEditEntityField(c, entity, "DateNullable", date, async, execute)
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2023, 11, 12, 0, 0, 0, 0, time.UTC), *entity.DateNullable)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Equal(t, time.Date(2023, 11, 12, 0, 0, 0, 0, time.UTC), *entity.DateNullable)
	err = runEditEntityField(c, entity, "DateNullable", nil, async, execute)
	assert.NoError(t, err)
	assert.Nil(t, entity.DateNullable)
	entity = GetByID[updateEntity](c, ids[1])
	assert.Nil(t, entity.DateNullable)

	/* unique index */
	err = runEditEntityField(c, entity, "Name", "name 3", async, execute)
	assert.EqualError(t, err, "duplicated value for unique index 'Name'")
	err = runEditEntityField(c, entity, "Name", "name 100", async, execute)
	assert.NoError(t, err)
	entity = GetByUniqueIndex[updateEntity](c, "Name", "name 100")
	assert.NotNil(t, entity)
	assert.Equal(t, ids[1], entity.ID)
	err = runEditEntityField(c, entity, "Int", 100, async, execute)
	assert.NoError(t, err)
	entity = GetByUniqueIndex[updateEntity](c, "Multi", 13, 100)
	assert.NotNil(t, entity)
	assert.Equal(t, ids[1], entity.ID)
}

func runEditEntityField(c Context, entity *updateEntity, field string, value any, async, execute bool) error {
	if async {
		err := EditEntityFieldAsync(c, entity, field, value, execute)
		if err != nil {
			return err
		}
		if !execute {
			err = c.FlushAsync()
			if err != nil {
				return err
			}
		}
		stop := ConsumeAsyncFlushTemporaryEvents(c, func(err error) {
			panic(err)
		})
		stop()
		return ConsumeAsyncFlushEvents(c, false)
	}
	err := EditEntityField(c, entity, field, value, true)
	if err != nil {
		return err
	}
	if !execute {
		return c.FlushAsync()
	}
	return nil
}
