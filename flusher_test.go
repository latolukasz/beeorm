package beeorm

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type obj struct {
	ID         uint64
	StorageKey string
	Data       interface{}
}

type flushStruct struct {
	Name2 string
	Age   int
}

type flushStructAnonymous struct {
	SubName string
	SubAge  float32 `orm:"decimal=9,5;unsigned=false"`
}

var TestSet = struct {
	D string
	E string
	F string
}{
	D: "d",
	E: "e",
	F: "f",
}

type attributesValues map[uint64][]interface{}

func (av attributesValues) UnmarshalJSON(data []byte) error {
	temp := map[uint64][]interface{}{}
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	for attributeID, values := range temp {
		valuesNew := make([]interface{}, len(values))

		for i, value := range values {
			if _, ok := value.(string); ok {
				valuesNew[i] = value
			} else {
				valuesNew[i] = uint64(value.(float64))
			}
		}
		av[attributeID] = valuesNew
	}

	return nil
}

type flushEntity struct {
	ORM                   `orm:"localCache;redisCache;dirty=entity_changed"`
	ID                    uint
	City                  string `orm:"unique=city"`
	Name                  string `orm:"unique=name;required"`
	NameTranslated        map[string]string
	Age                   int
	Uint                  uint
	UintNullable          *uint
	IntNullable           *int
	Year                  uint16  `orm:"year"`
	YearNullable          *uint16 `orm:"year"`
	BoolNullable          *bool
	FloatNullable         *float64              `orm:"precision=10"`
	Float32Nullable       *float32              `orm:"precision=4"`
	ReferenceOne          *flushEntityReference `orm:"unique=ReferenceOne"`
	ReferenceTwo          *flushEntityReference `orm:"unique=ReferenceTwo"`
	ReferenceMany         []*flushEntityReference
	ReferenceManyRequired []*flushEntityReference `orm:"required"`
	StringSlice           []string
	StringSliceNotNull    []string `orm:"required"`
	SetNullable           []string `orm:"set=beeorm.TestSet"`
	SetNotNull            []string `orm:"set=beeorm.TestSet;required"`
	EnumNullable          string   `orm:"enum=beeorm.TestEnum"`
	EnumNotNull           string   `orm:"enum=beeorm.TestEnum;required"`
	Ignored               []string `orm:"ignore"`
	Blob                  []uint8
	Bool                  bool
	FakeDelete            bool
	Float64               float64  `orm:"precision=10"`
	Decimal               float64  `orm:"decimal=5,2"`
	DecimalNullable       *float64 `orm:"decimal=5,2"`
	Float64Default        float64  `orm:"unsigned"`
	Float64Signed         float64
	CachedQuery           *CachedQuery
	Time                  time.Time
	TimeWithTime          time.Time `orm:"time"`
	TimeNullable          *time.Time
	TimeWithTimeNullable  *time.Time `orm:"time"`
	Interface             interface{}
	FlushStruct           flushStruct
	FlushStructPtr        *flushStruct
	Int8Nullable          *int8
	Int16Nullable         *int16
	Int32Nullable         *int32
	Int64Nullable         *int64
	Uint8Nullable         *uint8
	Uint16Nullable        *uint16
	Uint32Nullable        *uint32
	Uint64Nullable        *uint64
	Images                []obj
	AttributesValues      attributesValues
	flushStructAnonymous
}

type flushEntityReference struct {
	ORM  `orm:"localCache;redisCache"`
	ID   uint
	Name string
	Age  int
}

type flushEntityBenchmark struct {
	ORM  `orm:"localCache;redisCache"`
	ID   uint
	Name string
	Age  int
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
	var reference *flushEntityReference
	registry := &Registry{}
	registry.RegisterRedisStream("entity_changed", "default", []string{"test-group-1"})
	registry.RegisterEnumStruct("beeorm.TestEnum", TestEnum)
	registry.RegisterEnumStruct("beeorm.TestSet", TestSet)
	engine, def := prepareTables(t, registry, 5, "", entity, reference)
	defer def()

	schema := engine.registry.GetTableSchemaForEntity(entity).(*tableSchema)
	schema2 := engine.registry.GetTableSchemaForEntity(reference).(*tableSchema)
	if !local {
		schema.hasLocalCache = false
		schema.localCacheName = ""
		schema2.hasLocalCache = false
		schema2.localCacheName = ""
	}
	if !redis {
		schema.hasRedisCache = false
		schema.redisCacheName = ""
		schema2.hasRedisCache = false
		schema2.redisCacheName = ""
	}

	now := time.Now()
	entity = &flushEntity{Name: "Tom", Age: 12, Uint: 7, Year: 1982}
	entity.NameTranslated = map[string]string{"pl": "kot", "en": "cat"}
	entity.ReferenceOne = &flushEntityReference{Name: "John", Age: 30}
	entity.ReferenceMany = []*flushEntityReference{{Name: "Adam", Age: 18}}
	entity.StringSlice = []string{"a", "b"}
	entity.StringSliceNotNull = []string{"c", "d"}
	entity.SetNotNull = []string{"d", "e"}
	entity.FlushStructPtr = &flushStruct{"A", 12}
	entity.EnumNotNull = "a"
	entity.FlushStruct.Name2 = "Ita"
	entity.TimeWithTime = now
	entity.Float64 = 2.12
	entity.Decimal = 6.15
	entity.TimeWithTimeNullable = &now
	entity.Images = []obj{{ID: 1, StorageKey: "aaa", Data: map[string]string{"sss": "vv", "bb": "cc"}}}
	entity.flushStructAnonymous = flushStructAnonymous{"Adam", 39.123}
	entity.AttributesValues = attributesValues{12: []interface{}{"a", "b"}}
	assert.True(t, entity.IsDirty())
	assert.True(t, entity.ReferenceOne.IsDirty())
	flusher := engine.NewFlusher().Track(entity)
	flusher.Track(entity)
	flusher.Flush()
	flusher.Flush()

	assert.True(t, entity.IsLoaded())
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.False(t, entity.IsDirty())
	assert.False(t, entity.ReferenceOne.IsDirty())
	assert.Equal(t, uint(1), entity.ID)
	assert.NotEqual(t, uint(0), entity.ReferenceOne.ID)
	assert.True(t, entity.IsLoaded())
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.NotEqual(t, uint(0), entity.ReferenceMany[0].ID)
	assert.True(t, entity.ReferenceMany[0].IsLoaded())
	refOneID := entity.ReferenceOne.ID
	refManyID := entity.ReferenceMany[0].ID

	entity = &flushEntity{}
	found := engine.LoadByID(2, entity)
	assert.False(t, found)
	found = engine.LoadByID(1, entity)

	assert.True(t, found)
	assert.Equal(t, "Tom", entity.Name)
	assert.Equal(t, 12, entity.Age)
	assert.Equal(t, uint(7), entity.Uint)
	assert.Equal(t, uint16(1982), entity.Year)
	assert.Equal(t, map[string]string{"pl": "kot", "en": "cat"}, entity.NameTranslated)
	assert.Equal(t, attributesValues{12: []interface{}{"a", "b"}}, entity.AttributesValues)
	assert.Equal(t, []string{"a", "b"}, entity.StringSlice)
	assert.Equal(t, []string{"c", "d"}, entity.StringSliceNotNull)
	assert.Equal(t, "", entity.EnumNullable)
	assert.Equal(t, "a", entity.EnumNotNull)
	assert.Equal(t, now.Format(timeFormat), entity.TimeWithTime.Format(timeFormat))
	assert.Equal(t, now.Unix(), entity.TimeWithTime.Unix())
	assert.Equal(t, now.Format(timeFormat), entity.TimeWithTimeNullable.Format(timeFormat))
	assert.Equal(t, now.Unix(), entity.TimeWithTimeNullable.Unix())
	assert.Nil(t, entity.SetNullable)
	assert.Equal(t, "", entity.City)
	assert.NotNil(t, entity.FlushStructPtr)
	assert.Equal(t, "A", entity.FlushStructPtr.Name2)
	assert.Equal(t, 12, entity.FlushStructPtr.Age)
	assert.Nil(t, entity.UintNullable)
	assert.Nil(t, entity.IntNullable)
	assert.Nil(t, entity.YearNullable)
	assert.Nil(t, entity.BoolNullable)
	assert.Nil(t, entity.FloatNullable)
	assert.Nil(t, entity.Float32Nullable)
	assert.False(t, entity.IsDirty())
	assert.True(t, entity.IsLoaded())
	assert.False(t, entity.ReferenceOne.IsLoaded())
	assert.Equal(t, refOneID, entity.ReferenceOne.ID)
	assert.Nil(t, entity.Blob)
	assert.Nil(t, entity.Int8Nullable)
	assert.Nil(t, entity.Int16Nullable)
	assert.Nil(t, entity.Int32Nullable)
	assert.Nil(t, entity.Int64Nullable)
	assert.Nil(t, entity.Uint8Nullable)
	assert.Nil(t, entity.Uint16Nullable)
	assert.Nil(t, entity.Uint32Nullable)
	assert.Nil(t, entity.Uint64Nullable)
	assert.Equal(t, "Adam", entity.SubName)
	assert.Equal(t, float32(39.123), entity.SubAge)
	assert.NotNil(t, entity.ReferenceMany)
	assert.Len(t, entity.ReferenceMany, 1)
	assert.Equal(t, refManyID, entity.ReferenceMany[0].ID)

	entity.ReferenceOne.Name = "John 2"
	assert.PanicsWithError(t, fmt.Sprintf("entity is not loaded and can't be updated: beeorm.flushEntityReference [%d]", refOneID), func() {
		engine.Flush(entity.ReferenceOne)
	})

	i := 42
	i2 := uint(42)
	i3 := uint16(1982)
	i4 := false
	i5 := 134.345
	i6 := true
	i7 := int8(4)
	i8 := int16(4)
	i9 := int32(4)
	i10 := int64(4)
	i11 := uint8(4)
	i12 := uint16(4)
	i13 := uint32(4)
	i14 := uint64(4)
	i15 := float32(134.345)
	entity.IntNullable = &i
	entity.UintNullable = &i2
	entity.Int8Nullable = &i7
	entity.Int16Nullable = &i8
	entity.Int32Nullable = &i9
	entity.Int64Nullable = &i10
	entity.Uint8Nullable = &i11
	entity.Uint16Nullable = &i12
	entity.Uint32Nullable = &i13
	entity.Uint64Nullable = &i14
	entity.YearNullable = &i3
	entity.BoolNullable = &i4
	entity.FloatNullable = &i5
	entity.Float32Nullable = &i15
	entity.City = "New York"
	entity.Blob = []uint8("Tom has a house")
	entity.Bool = true
	entity.BoolNullable = &i6
	entity.Float64 = 134.345
	entity.Decimal = 134.345
	entity.StringSlice = []string{"a"}
	entity.DecimalNullable = &entity.Decimal
	entity.Interface = map[string]int{"test": 12}
	entity.ReferenceMany = nil
	engine.Flush(entity)

	reference = &flushEntityReference{}
	found = engine.LoadByID(uint64(refOneID), reference)
	assert.True(t, found)
	assert.Equal(t, "John", reference.Name)
	assert.Equal(t, 30, reference.Age)

	entity = &flushEntity{}
	engine.LoadByID(1, entity)
	assert.Equal(t, 42, *entity.IntNullable)
	assert.Equal(t, int8(4), *entity.Int8Nullable)
	assert.Equal(t, int16(4), *entity.Int16Nullable)
	assert.Equal(t, int32(4), *entity.Int32Nullable)
	assert.Equal(t, int64(4), *entity.Int64Nullable)
	assert.Equal(t, uint8(4), *entity.Uint8Nullable)
	assert.Equal(t, uint16(4), *entity.Uint16Nullable)
	assert.Equal(t, uint32(4), *entity.Uint32Nullable)
	assert.Equal(t, uint64(4), *entity.Uint64Nullable)
	assert.Equal(t, uint(42), *entity.UintNullable)
	assert.Equal(t, uint16(1982), *entity.YearNullable)
	assert.True(t, *entity.BoolNullable)
	assert.True(t, entity.Bool)
	assert.Equal(t, 134.345, *entity.FloatNullable)
	assert.Equal(t, float32(134.345), *entity.Float32Nullable)
	assert.Equal(t, []string{"a"}, entity.StringSlice)
	assert.Equal(t, "New York", entity.City)
	assert.Equal(t, []uint8("Tom has a house"), entity.Blob)
	assert.Equal(t, 134.345, entity.Float64)
	assert.Equal(t, 134.35, entity.Decimal)
	assert.Equal(t, 134.35, *entity.DecimalNullable)
	assert.Equal(t, float32(39.123), entity.SubAge)
	assert.Nil(t, entity.ReferenceMany)
	assert.False(t, entity.IsDirty())
	assert.False(t, reference.IsDirty())
	assert.True(t, reference.IsLoaded())

	entity.ReferenceMany = []*flushEntityReference{}
	assert.False(t, entity.IsDirty())
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(1, entity)
	assert.Nil(t, entity.ReferenceMany)

	entity2 := &flushEntity{Name: "Tom", Age: 12, EnumNotNull: "a"}
	assert.PanicsWithError(t, "Duplicate entry 'Tom' for key 'name'", func() {
		engine.Flush(entity2)
	})

	entity2.Name = "Lucas"
	entity2.ReferenceOne = &flushEntityReference{ID: 3}
	assert.PanicsWithError(t, "foreign key error in key `test:flushEntity:ReferenceOne`", func() {
		engine.Flush(entity2)
	})

	entity2.ReferenceOne = nil
	entity2.Name = "Tom"
	var uIntNullable *uint
	entity2.SetOnDuplicateKeyUpdate(Bind{"Age": 40, "Year": 2020, "City": "Moscow", "UintNullable": uIntNullable,
		"BoolNullable": nil, "TimeWithTime": now, "Time": now})
	engine.Flush(entity2)

	assert.Equal(t, uint(1), entity2.ID)
	assert.Equal(t, 40, entity2.Age)
	entity = &flushEntity{}
	engine.LoadByID(1, entity)
	assert.Equal(t, "Tom", entity.Name)
	assert.Equal(t, "Moscow", entity.City)
	assert.Nil(t, entity.UintNullable)
	assert.Equal(t, 40, entity.Age)
	assert.Equal(t, uint(1), entity.ID)
	assert.Equal(t, now.Unix(), entity.TimeWithTime.Unix())
	assert.Equal(t, entity.Time.Format(dateformat), now.Format(dateformat))

	entity2 = &flushEntity{Name: "Tom", Age: 12, EnumNotNull: "a"}
	entity2.SetOnDuplicateKeyUpdate(Bind{})
	engine.Flush(entity2)
	assert.Equal(t, uint(1), entity2.ID)
	entity = &flushEntity{}
	engine.LoadByID(1, entity)
	assert.Equal(t, uint(1), entity.ID)

	entity2 = &flushEntity{Name: "Arthur", Age: 18, EnumNotNull: "a"}
	entity2.ReferenceTwo = reference
	entity2.SetOnDuplicateKeyUpdate(Bind{})
	engine.Flush(entity2)
	assert.Equal(t, uint(6), entity2.ID)
	entity = &flushEntity{}
	engine.LoadByID(6, entity)
	assert.Equal(t, uint(6), entity.ID)
	engine.LoadByID(1, entity)

	entity.Bool = false
	now = now.Add(time.Hour * 40)
	entity.TimeWithTime = now
	entity.Name = ""
	entity.IntNullable = nil
	entity.EnumNullable = "b"
	entity.Blob = nil
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(1, entity)
	assert.Equal(t, false, entity.Bool)
	assert.Equal(t, now.Format(timeFormat), entity.TimeWithTime.Format(timeFormat))
	assert.Equal(t, "", entity.Name)
	assert.Equal(t, "b", entity.EnumNullable)
	assert.Nil(t, entity.IntNullable)
	assert.Nil(t, entity.Blob)
	entity.EnumNullable = ""
	entity.Blob = []uint8("Tom has a house")
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(1, entity)
	assert.Equal(t, "", entity.EnumNullable)

	assert.PanicsWithError(t, "empty enum value for EnumNotNull", func() {
		entity.EnumNotNull = ""
		engine.Flush(entity)
	})
	entity = &flushEntity{Name: "Cat"}
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(1, entity)
	assert.Equal(t, "a", entity.EnumNotNull)

	entity2 = &flushEntity{Name: "Adam", Age: 20, ID: 10, EnumNotNull: "a"}
	engine.Flush(entity2)
	found = engine.LoadByID(10, entity2)
	assert.True(t, found)

	entity2.Age = 21
	entity2.UintNullable = &i2
	entity2.BoolNullable = &i4
	entity2.FloatNullable = &i5
	entity2.City = "War'saw '; New"
	assert.True(t, entity2.IsDirty())
	engine.Flush(entity2)
	assert.False(t, entity2.IsDirty())
	engine.LoadByID(10, entity2)
	assert.Equal(t, 21, entity2.Age)
	entity2.City = "War\\'saw"
	engine.Flush(entity2)
	engine.LoadByID(10, entity2)
	assert.Equal(t, "War\\'saw", entity2.City)
	entity2.Time = time.Now()
	n := time.Now()
	entity2.TimeNullable = &n
	engine.Flush(entity2)
	engine.LoadByID(10, entity2)
	assert.NotNil(t, entity2.Time)
	assert.NotNil(t, entity2.TimeNullable)

	entity2.UintNullable = nil
	entity2.BoolNullable = nil
	entity2.FloatNullable = nil
	entity2.City = ""
	assert.True(t, entity2.IsDirty())

	engine.Flush(entity2)
	assert.False(t, entity2.IsDirty())
	entity2 = &flushEntity{}
	engine.LoadByID(10, entity2)
	assert.Nil(t, entity2.UintNullable)
	assert.Nil(t, entity2.BoolNullable)
	assert.Nil(t, entity2.FloatNullable)
	assert.Equal(t, "", entity2.City)

	entity2.markToDelete()
	assert.True(t, entity2.IsDirty())
	engine.Delete(entity2)
	found = engine.LoadByID(10, entity2)
	assert.True(t, found)
	assert.True(t, entity2.FakeDelete)

	engine.Flush(&flushEntity{Name: "Tom", Age: 12, Uint: 7, Year: 1982, EnumNotNull: "a"})
	entity3 := &flushEntity{}
	found = engine.LoadByID(11, entity3)
	assert.True(t, found)
	assert.Nil(t, entity3.NameTranslated)

	engine.Flush(&flushEntity{Name: "Eva", SetNullable: []string{}, EnumNotNull: "a"})
	entity4 := &flushEntity{}
	found = engine.LoadByID(12, entity4)
	assert.True(t, found)
	assert.Nil(t, entity4.SetNotNull)
	assert.Nil(t, entity4.SetNullable)
	entity4.SetNullable = []string{"d", "e"}
	engine.Flush(entity4)
	entity4 = &flushEntity{}
	engine.LoadByID(12, entity4)
	assert.Equal(t, []string{"d", "e"}, entity4.SetNullable)

	engine.GetMysql().Begin()
	entity5 := &flushEntity{Name: "test_transaction", EnumNotNull: "a"}
	engine.Flush(entity5)
	entity5.Age = 38
	engine.Flush(entity5)
	engine.GetMysql().Commit()
	entity5 = &flushEntity{}
	found = engine.LoadByID(13, entity5)
	assert.True(t, found)
	assert.Equal(t, "test_transaction", entity5.Name)
	assert.Equal(t, 38, entity5.Age)

	entity6 := &flushEntity{Name: "test_transaction_2", EnumNotNull: "a"}
	flusher.Clear()
	flusher.Flush()
	flusher.Track(entity6)
	flusher.Flush()
	entity6 = &flushEntity{}
	found = engine.LoadByID(14, entity6)
	assert.True(t, found)
	assert.Equal(t, "test_transaction_2", entity6.Name)

	entity7 := &flushEntity{Name: "test_check", EnumNotNull: "a"}
	flusher.Track(entity7)
	err := flusher.FlushWithCheck()
	assert.NoError(t, err)
	entity7 = &flushEntity{}
	found = engine.LoadByID(15, entity7)
	assert.True(t, found)
	assert.Equal(t, "test_check", entity7.Name)

	entity7 = &flushEntity{Name: "test_check", EnumNotNull: "a"}
	flusher.Track(entity7)
	err = flusher.FlushWithCheck()
	assert.EqualError(t, err, "Duplicate entry 'test_check' for key 'name'")
	entity7 = &flushEntity{Name: "test_check_2", EnumNotNull: "a", ReferenceOne: &flushEntityReference{ID: 100}}
	err = engine.FlushWithCheck(entity7)
	assert.EqualError(t, err, "foreign key error in key `test:flushEntity:ReferenceOne`")

	entity7 = &flushEntity{Name: "test_check_3", EnumNotNull: "Y"}
	flusher.Track(entity7)
	err = flusher.FlushWithFullCheck()
	assert.EqualError(t, err, "unknown enum value for EnumNotNull - Y")
	flusher.Track(entity7)
	assert.Panics(t, func() {
		_ = flusher.FlushWithCheck()
	})

	entity8 := &flushEntity{Name: "test_check", EnumNotNull: "a"}
	flusher.Track(entity8)
	err = flusher.FlushWithCheck()
	assert.EqualError(t, err, "Duplicate entry 'test_check' for key 'name'")

	assert.PanicsWithError(t, "track limit 10000 exceeded", func() {
		for i := 1; i <= 10001; i++ {
			flusher.Track(&flushEntity{})
		}
	})

	flusher.Clear()
	entity2 = &flushEntity{ID: 100, Name: "Eva", Age: 1, EnumNotNull: "a"}
	entity2.SetOnDuplicateKeyUpdate(Bind{"Age": 2})
	engine.Flush(entity2)
	assert.Equal(t, uint(12), entity2.ID)
	assert.Equal(t, 2, entity2.Age)
	entity2 = &flushEntity{}
	found = engine.LoadByID(100, entity2)
	assert.False(t, found)
	entity2 = &flushEntity{Name: "Frank", ID: 100, Age: 1, EnumNotNull: "a"}
	entity2.SetOnDuplicateKeyUpdate(Bind{"Age": 2})
	engine.Flush(entity2)
	found = engine.LoadByID(100, entity2)
	assert.True(t, found)
	assert.Equal(t, 1, entity2.Age)

	entity2 = &flushEntity{ID: 100, Age: 1, EnumNotNull: "a"}
	entity2.SetOnDuplicateKeyUpdate(Bind{"Age": 2})
	engine.Flush(entity2)
	assert.Equal(t, uint(100), entity2.ID)
	assert.Equal(t, 2, entity2.Age)
	entity2 = &flushEntity{}
	found = engine.LoadByID(100, entity2)
	assert.True(t, found)
	assert.Equal(t, 2, entity2.Age)

	receiver := NewBackgroundConsumer(engine)
	receiver.DisableLoop()
	receiver.blockTime = time.Millisecond

	testLogger := &testLogHandler{}
	engine.RegisterQueryLogger(testLogger, true, false, false)

	flusher = engine.NewFlusher()
	entity1 := &flushEntity{}
	engine.LoadByID(10, entity1)
	entity2 = &flushEntity{}
	engine.LoadByID(11, entity2)
	entity3 = &flushEntity{}
	engine.LoadByID(12, entity3)
	entity1.Age = 99
	entity2.Uint = 99
	entity3.Name = "sss"
	flusher.Track(entity1, entity2, entity3)
	flusher.Flush()

	receiver.Digest(context.Background())
	if local {
		assert.Len(t, testLogger.Logs, 3)
		assert.Equal(t, "START TRANSACTION", testLogger.Logs[0]["query"])
		assert.Equal(t, "UPDATE flushEntity SET `Age`=99 WHERE `ID` = 10;UPDATE flushEntity SET `Uint`=99 "+
			"WHERE `ID` = 11;UPDATE flushEntity SET `Name`='sss' WHERE `ID` = 12;", testLogger.Logs[1]["query"])
		assert.Equal(t, "COMMIT", testLogger.Logs[2]["query"])
	}

	entity = &flushEntity{Name: "Monica", EnumNotNull: "a", ReferenceMany: []*flushEntityReference{{Name: "Adam Junior"}}}
	engine.Flush(entity)
	assert.Equal(t, uint(101), entity.ID)
	assert.Equal(t, uint(3), entity.ReferenceMany[0].ID)

	entity = &flushEntity{Name: "John", EnumNotNull: "a", ReferenceMany: []*flushEntityReference{}}
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(102, entity)
	assert.Nil(t, entity.ReferenceMany)

	entity1 = &flushEntity{}
	engine.LoadByID(13, entity1)
	entity2 = &flushEntity{}
	engine.LoadByID(14, entity2)
	entity3 = &flushEntity{}
	engine.LoadByID(15, entity3)

	engine.LoadByID(1, entity)
	engine.ForceDelete(entity)

	flusher = engine.NewFlusher()
	entity1.ReferenceOne = &flushEntityReference{ID: 1}
	entity1.Name = "A"
	entity2.ReferenceOne = &flushEntityReference{ID: 2}
	entity2.Name = "B"
	entity3.ReferenceOne = &flushEntityReference{ID: 3}
	entity3.Name = "C"
	flusher.Track(entity1, entity2, entity3)
	flusher.Flush()

	entities := make([]*flushEntity, 0)
	engine.LoadByIDs([]uint64{13, 14, 15}, &entities, "ReferenceOne")
	flusher = engine.NewFlusher()
	for _, e := range entities {
		newRef := &flushEntityReference{}
		newRef.Name = fmt.Sprintf("%d33", e.ID)
		oldRef := e.ReferenceOne
		oldRef.Name = fmt.Sprintf("%d34", e.ID)
		flusher.Track(oldRef)
		e.Name = fmt.Sprintf("%d35", e.ID)
		e.ReferenceOne = newRef
		flusher.Track(e)
	}

	flusher.Flush()
	entities = make([]*flushEntity, 0)
	engine.LoadByIDs([]uint64{13, 14, 15}, &entities, "ReferenceOne")
	assert.Equal(t, "1335", entities[0].Name)
	assert.Equal(t, "1435", entities[1].Name)
	assert.Equal(t, "1535", entities[2].Name)
	assert.Equal(t, "1333", entities[0].ReferenceOne.Name)
	assert.Equal(t, "1433", entities[1].ReferenceOne.Name)
	assert.Equal(t, "1533", entities[2].ReferenceOne.Name)
	entitiesRefs := make([]*flushEntityReference, 0)
	engine.LoadByIDs([]uint64{1, 2, 3}, &entitiesRefs)
	assert.Equal(t, "1334", entitiesRefs[0].Name)
	assert.Equal(t, "1434", entitiesRefs[1].Name)
	assert.Equal(t, "1534", entitiesRefs[2].Name)

	if redis && !local {
		testLogger2 := &testLogHandler{}
		engine.RegisterQueryLogger(testLogger2, true, true, false)
		testLogger.clear()
		engine.GetMysql().Begin()
		entity4.ReferenceOne = &flushEntityReference{}
		engine.Flush(entity4)
		engine.GetMysql().Commit()
		assert.Len(t, testLogger2.Logs, 5)
		assert.Equal(t, "BEGIN", testLogger2.Logs[0]["operation"])
		assert.Equal(t, "EXEC", testLogger2.Logs[1]["operation"])
		assert.Equal(t, "EXEC", testLogger2.Logs[2]["operation"])
		assert.Equal(t, "COMMIT", testLogger2.Logs[3]["operation"])
		assert.Equal(t, "PIPELINE EXEC", testLogger2.Logs[4]["operation"])
	}

	entity = &flushEntity{}
	found = engine.LoadByID(6, entity)
	entity.FlushStructPtr = &flushStruct{Name2: `okddlk"nokddlkno'kddlkn f;mf	jg`}
	engine.Flush(entity)

	flusher.Clear()
	flusher.ForceDelete(entity)
	entity = &flushEntity{}
	engine.LoadByID(7, entity)
	flusher.ForceDelete(entity)
	flusher.Flush()
	found = engine.LoadByID(6, entity)
	assert.False(t, found)
	found = engine.LoadByID(7, entity)
	assert.False(t, found)

	entity = &flushEntity{}
	engine.LoadByID(102, entity)
	entity.Float64Default = 0.3
	assert.True(t, entity.IsDirty())
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(102, entity)
	assert.Equal(t, 0.3, entity.Float64Default)
	entity.Float64Default = 0.4
	entity.Float64Signed = -0.4
	assert.True(t, entity.IsDirty())
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(102, entity)
	assert.Equal(t, 0.4, entity.Float64Default)
	assert.Equal(t, -0.4, entity.Float64Signed)

	entity.SetNullable = []string{}
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(102, entity)
	assert.Nil(t, nil, entity.SetNullable)
	entity.SetNullable = []string{"d"}
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(102, entity)
	assert.Equal(t, []string{"d"}, entity.SetNullable)
	entity.SetNullable = []string{"f"}
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(102, entity)
	assert.Equal(t, []string{"f"}, entity.SetNullable)
	entity.SetNullable = []string{"f", "d"}
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(102, entity)
	assert.Len(t, entity.SetNullable, 2)
	assert.False(t, entity.IsDirty())
	entity.SetNullable = []string{"f", "d"}
	assert.False(t, entity.IsDirty())

	entity.ReferenceMany = []*flushEntityReference{{ID: 1}, {ID: 2}}
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(102, entity)
	assert.Len(t, entity.ReferenceMany, 2)
	assert.True(t, entity.ReferenceMany[0].ID == 1 || entity.ReferenceMany[0].ID == 2)
	assert.True(t, entity.ReferenceMany[1].ID == 1 || entity.ReferenceMany[1].ID == 2)
	entity.ReferenceMany = []*flushEntityReference{{ID: 1}, {ID: 3}}
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(102, entity)
	assert.Len(t, entity.ReferenceMany, 2)
	assert.True(t, entity.ReferenceMany[0].ID == 1 || entity.ReferenceMany[0].ID == 3)
	assert.True(t, entity.ReferenceMany[1].ID == 1 || entity.ReferenceMany[1].ID == 3)

	entity.ReferenceManyRequired = []*flushEntityReference{{ID: 1}, {ID: 2}}
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(102, entity)
	assert.Len(t, entity.ReferenceManyRequired, 2)
	assert.True(t, entity.ReferenceManyRequired[0].ID == 1 || entity.ReferenceManyRequired[0].ID == 2)
	assert.True(t, entity.ReferenceManyRequired[1].ID == 1 || entity.ReferenceManyRequired[1].ID == 2)
	assert.False(t, entity.IsDirty())
	entity.ReferenceManyRequired = nil
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(102, entity)
	assert.Len(t, entity.ReferenceManyRequired, 0)
	assert.False(t, entity.IsDirty())
	engine.ForceDelete(entity)

	now = time.Unix(1, 0).UTC()
	entity = &flushEntity{}
	engine.LoadByID(11, entity)
	entity.TimeWithTime = now
	engine.Flush(entity)
	entity = &flushEntity{}
	i2 = 13
	i7 = 3
	i4 = true
	i5 = 12.33
	n = time.Now()
	entity.UintNullable = &i2
	entity.Int8Nullable = &i7
	entity.BoolNullable = &i4
	entity.FloatNullable = &i5
	entity.TimeNullable = &n
	entity.TimeWithTimeNullable = &n
	entity.Interface = "ss"
	entity.ReferenceMany = []*flushEntityReference{}
	engine.LoadByID(11, entity)
	assert.Equal(t, now, entity.TimeWithTime.UTC())
	assert.Nil(t, entity.UintNullable)
	assert.Nil(t, entity.Int8Nullable)
	assert.Nil(t, entity.BoolNullable)
	assert.Nil(t, entity.FloatNullable)
	assert.Nil(t, entity.TimeNullable)
	assert.Nil(t, entity.TimeWithTimeNullable)
	assert.Nil(t, entity.Interface)
	assert.Nil(t, entity.ReferenceMany)

	entity = &flushEntity{}
	engine.LoadByID(101, entity)
	engine.DeleteMany(entity)
	entity = &flushEntity{}
	engine.GetLocalCache().Clear()
	engine.GetRedis().FlushDB()
	assert.True(t, engine.LoadByID(101, entity))
	assert.True(t, entity.FakeDelete)
	assert.False(t, entity.IsDirty())
	engine.ForceDeleteMany(entity)
	engine.GetLocalCache().Clear()
	engine.GetRedis().FlushDB()
	assert.False(t, engine.LoadByID(101, entity))

	testLogger.clear()
	flusher = engine.NewFlusher()
	flusher.Track(&flushEntityReference{})
	flusher.Track(&flushEntityReference{})
	flusher.Flush()
	assert.Len(t, testLogger.Logs, 1)
	testLogger.clear()
	flusher = engine.NewFlusher()
	flusher.Track(&flushEntityReference{})
	flusher.Track(&flushEntity{})
	flusher.Flush()
	assert.Len(t, testLogger.Logs, 4)
	assert.Equal(t, "START TRANSACTION", testLogger.Logs[0]["query"])
	assert.Equal(t, "COMMIT", testLogger.Logs[3]["query"])
}

// 17 allocs/op - 6 for Exec
func BenchmarkFlusherUpdateNoCache(b *testing.B) {
	benchmarkFlusher(b, false, false)
}

func benchmarkFlusher(b *testing.B, useLocaCache, useRedisCache bool) {
	var entity *flushEntityBenchmark
	registry := &Registry{}
	registry.RegisterRedisStream("entity_changed", "default", []string{"test-group-1"})
	registry.RegisterEnum("beeorm.TestEnum", []string{"a", "b", "c"})
	engine, def := prepareTables(nil, registry, 5, "", entity)
	defer def()

	schema := engine.registry.GetTableSchemaForEntity(entity).(*tableSchema)
	if !useLocaCache {
		schema.hasLocalCache = false
		schema.localCacheName = ""
	}
	if !useRedisCache {
		schema.hasRedisCache = false
		schema.redisCacheName = ""
	}

	entity = &flushEntityBenchmark{Name: "Tom"}
	engine.Flush(entity)
	engine.LoadByID(1, entity)
	flusher := engine.NewFlusher()
	flusher.Track(entity)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		entity.Age = n + 1
		flusher.Flush()
	}
}
