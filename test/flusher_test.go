package test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/latolukasz/beeorm/v2"

	"github.com/stretchr/testify/assert"
)

type obj struct {
	ID         uint64
	StorageKey string
	Data       interface{}
}

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

var testSet = struct {
	D string
	E string
	F string
}{
	D: "d",
	E: "e",
	F: "f",
}

var testEnum = struct {
	A string
	B string
	C string
}{
	A: "a",
	B: "b",
	C: "c",
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
	beeorm.ORM           `orm:"localCache;redisCache"`
	City                 string `orm:"unique=city"`
	Name                 string `orm:"unique=name;required"`
	NameTranslated       map[string]string
	Age                  int
	Uint                 uint
	UintNullable         *uint
	IntNullable          *int
	Year                 uint16  `orm:"year"`
	YearNullable         *uint16 `orm:"year"`
	BoolNullable         *bool
	FloatNullable        *float64              `orm:"precision=10"`
	Float32Nullable      *float32              `orm:"precision=4"`
	ReferenceOne         *flushEntityReference `orm:"unique=ReferenceOne"`
	ReferenceTwo         *flushEntityReference `orm:"unique=ReferenceTwo"`
	StringSlice          []string
	StringSliceNotNull   []string `orm:"required"`
	SetNullable          []string `orm:"set=test.testSet"`
	SetNotNull           []string `orm:"set=test.testSet;required"`
	EnumNullable         string   `orm:"enum=test.testEnum"`
	EnumNotNull          string   `orm:"enum=test.testEnum;required"`
	Ignored              []string `orm:"ignore"`
	Blob                 []uint8
	Bool                 bool
	FakeDelete           bool
	Float64              float64  `orm:"precision=10"`
	Decimal              float64  `orm:"decimal=5,2"`
	DecimalNullable      *float64 `orm:"decimal=5,2"`
	Float64Default       float64  `orm:"unsigned"`
	Float64Signed        float64
	CachedQuery          *beeorm.CachedQuery
	Time                 time.Time
	TimeWithTime         time.Time `orm:"time"`
	TimeNullable         *time.Time
	TimeWithTimeNullable *time.Time `orm:"time"`
	Interface            interface{}
	FlushStruct          flushStruct
	FlushStructPtr       *flushStruct
	Int8Nullable         *int8
	Int16Nullable        *int16
	Int32Nullable        *int32
	Int64Nullable        *int64
	Uint8Nullable        *uint8
	Uint16Nullable       *uint16
	Uint32Nullable       *uint32
	Uint64Nullable       *uint64
	Images               []obj
	AttributesValues     attributesValues
	flushStructAnonymous
}

type flushEntityReference struct {
	beeorm.ORM `orm:"localCache;redisCache"`
	Name       string
	Age        int
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
	registry := &beeorm.Registry{}
	registry.RegisterRedisStream("entity_changed", "default")
	registry.RegisterRedisStreamConsumerGroups("entity_changed", "test-group-1")
	registry.RegisterEnumStruct("test.testEnum", testEnum)
	registry.RegisterEnumStruct("test.testSet", testSet)
	engine := PrepareTables(t, registry, 5, 6, "", entity, reference)

	schema := engine.GetRegistry().GetTableSchemaForEntity(entity)
	schema2 := engine.GetRegistry().GetTableSchemaForEntity(reference)
	schema.DisableCache(!local, !redis)
	schema2.DisableCache(!local, !redis)

	date := time.Date(2049, 1, 12, 18, 34, 40, 0, time.Local)
	entity = &flushEntity{Name: "Tom", Age: 12, Uint: 7, Year: 1982}
	entity.NameTranslated = map[string]string{"pl": "kot", "en": "cat"}
	entity.ReferenceOne = &flushEntityReference{Name: "John", Age: 30}
	entity.StringSlice = []string{"a", "b"}
	entity.StringSliceNotNull = []string{"c", "d"}
	entity.SetNotNull = []string{"d", "e"}
	entity.FlushStructPtr = &flushStruct{"A", 12, flushSubStruct{Age3: 11, Name3: "G"}, nil}
	entity.EnumNotNull = "a"
	entity.FlushStruct.Name2 = "Ita"
	entity.FlushStruct.Sub.Age3 = 13
	entity.FlushStruct.Sub.Name3 = "Nanami"
	entity.FlushStruct.TestTime = &date
	entity.TimeWithTime = date
	entity.Float64 = 2.12
	entity.Decimal = 6.15
	entity.TimeWithTimeNullable = &date
	entity.Images = []obj{{ID: 1, StorageKey: "aaa", Data: map[string]string{"sss": "vv", "bb": "cc"}}}
	entity.flushStructAnonymous = flushStructAnonymous{"Adam", 39.123}
	entity.AttributesValues = attributesValues{12: []interface{}{"a", "b"}}
	assert.True(t, engine.IsDirty(entity))
	assert.True(t, engine.IsDirty(entity.ReferenceOne))
	sqlEvent, _ := engine.GetDirtyBind(entity.ReferenceOne)
	assert.NotNil(t, sqlEvent)

	flusher := engine.NewFlusher().Track(entity)
	flusher.Track(entity)
	flusher.Flush()
	bind, isDirty := engine.GetDirtyBind(entity)
	assert.False(t, isDirty)
	assert.Nil(t, bind)
	flusher.Flush()

	assert.True(t, entity.IsLoaded())
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.False(t, engine.IsDirty(entity))
	assert.False(t, engine.IsDirty(entity.ReferenceOne))
	assert.Equal(t, uint64(1), entity.GetID())
	assert.Equal(t, uint64(1), entity.ReferenceOne.GetID())
	assert.True(t, entity.IsLoaded())
	assert.True(t, entity.ReferenceOne.IsLoaded())
	refOneID := entity.ReferenceOne.GetID()

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
	assert.Equal(t, date.Format(beeorm.TimeFormat), entity.TimeWithTime.Format(beeorm.TimeFormat))
	assert.Equal(t, date.Unix(), entity.TimeWithTime.Unix())
	assert.Equal(t, date.Format(beeorm.TimeFormat), entity.TimeWithTimeNullable.Format(beeorm.TimeFormat))
	assert.Equal(t, date.Unix(), entity.TimeWithTimeNullable.Unix())
	assert.Nil(t, entity.SetNullable)
	assert.Equal(t, "", entity.City)
	assert.NotNil(t, entity.FlushStructPtr)
	assert.Equal(t, "A", entity.FlushStructPtr.Name2)
	assert.Equal(t, 12, entity.FlushStructPtr.Age)
	assert.Equal(t, "G", entity.FlushStructPtr.Sub.Name3)
	assert.Equal(t, 11, entity.FlushStructPtr.Sub.Age3)
	assert.Equal(t, date.Format(beeorm.TimeFormat), entity.FlushStruct.TestTime.Format(beeorm.TimeFormat))
	assert.Nil(t, entity.UintNullable)
	assert.Nil(t, entity.IntNullable)
	assert.Nil(t, entity.YearNullable)
	assert.Nil(t, entity.BoolNullable)
	assert.Nil(t, entity.FloatNullable)
	assert.Nil(t, entity.Float32Nullable)
	assert.False(t, engine.IsDirty(entity))
	assert.True(t, entity.IsLoaded())
	assert.False(t, entity.ReferenceOne.IsLoaded())
	assert.Equal(t, refOneID, entity.ReferenceOne.GetID())
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
	assert.Equal(t, "Ita", entity.FlushStruct.Name2)
	assert.Equal(t, 13, entity.FlushStruct.Sub.Age3)
	assert.Equal(t, "Nanami", entity.FlushStruct.Sub.Name3)

	entity.FlushStructPtr = nil
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(1, entity)
	assert.Nil(t, entity.FlushStructPtr)

	entity.ReferenceOne.Name = "John 2"
	assert.PanicsWithError(t, fmt.Sprintf("entity is not loaded and can't be updated: test.flushEntityReference [%d]", refOneID), func() {
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

	engine.Flush(entity)

	reference = &flushEntityReference{}
	found = engine.LoadByID(refOneID, reference)
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
	assert.False(t, engine.IsDirty(entity))
	assert.False(t, engine.IsDirty(reference))
	assert.True(t, reference.IsLoaded())

	entity2 := &flushEntity{Name: "Tom", Age: 12, EnumNotNull: "a"}
	assert.PanicsWithError(t, "Error 1062 (23000): Duplicate entry 'Tom' for key 'name'", func() {
		engine.Flush(entity2)
	})

	entity2.Name = "Lucas"
	entity2.ReferenceOne = &flushEntityReference{}
	entity2.ReferenceOne.SetID(3)
	assert.PanicsWithError(t, "Error 1452 (23000): Cannot add or update a child row: a foreign key constraint fails (`test`.`flushEntity`, CONSTRAINT `test:flushEntity:ReferenceOne` FOREIGN KEY (`ReferenceOne`) REFERENCES `flushEntityReference` (`ID`))", func() {
		engine.Flush(entity2)
	})

	entity2.ReferenceOne = nil
	entity2.Name = "Tom"
	entity2.SetOnDuplicateKeyUpdate(beeorm.Bind{"Age": "40", "Year": "2020", "City": "Moscow", "UintNullable": "NULL",
		"BoolNullable": "NULL", "TimeWithTime": date.Format(beeorm.TimeFormat), "Time": date.Format(beeorm.DateFormat)})
	engine.Flush(entity2)

	assert.Equal(t, uint64(1), entity2.GetID())
	assert.Equal(t, 40, entity2.Age)
	entity = &flushEntity{}
	engine.LoadByID(1, entity)
	assert.Equal(t, "Tom", entity.Name)
	assert.Equal(t, "Moscow", entity.City)
	assert.Nil(t, entity.UintNullable)
	assert.Equal(t, 40, entity.Age)
	assert.Equal(t, uint64(1), entity.GetID())
	assert.Equal(t, date.Unix(), entity.TimeWithTime.Unix())
	assert.Equal(t, entity.Time.Format(beeorm.DateFormat), date.Format(beeorm.DateFormat))

	entity2 = &flushEntity{Name: "Tom", Age: 12, EnumNotNull: "a"}
	entity2.SetOnDuplicateKeyUpdate(beeorm.Bind{})
	engine.Flush(entity2)
	assert.Equal(t, uint64(1), entity2.GetID())
	entity = &flushEntity{}
	engine.LoadByID(1, entity)
	assert.Equal(t, uint64(1), entity.GetID())

	entity2 = &flushEntity{Name: "Arthur", Age: 18, EnumNotNull: "a"}
	entity2.ReferenceTwo = reference
	entity2.SetOnDuplicateKeyUpdate(beeorm.Bind{})
	engine.Flush(entity2)
	assert.Equal(t, uint64(6), entity2.GetID())

	entity = &flushEntity{}
	engine.LoadByID(6, entity)
	assert.Equal(t, uint64(6), entity.GetID())

	engine.LoadByID(1, entity)
	entity.Bool = false
	date = date.Add(time.Hour * 40)
	entity.TimeWithTime = date
	entity.Name = ""
	entity.IntNullable = nil
	entity.EnumNullable = "b"
	entity.Blob = nil
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(1, entity)
	assert.Equal(t, false, entity.Bool)
	assert.Equal(t, date.Format(beeorm.TimeFormat), entity.TimeWithTime.Format(beeorm.TimeFormat))
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
	id := entity.GetID()
	entity = &flushEntity{}
	engine.LoadByID(id, entity)
	assert.Equal(t, "a", entity.EnumNotNull)

	entity2 = &flushEntity{Name: "Adam", Age: 20, EnumNotNull: "a"}
	entity2.SetID(10)
	engine.Flush(entity2)
	found = engine.LoadByID(10, entity2)
	assert.True(t, found)

	entity2.Age = 21
	entity2.UintNullable = &i2
	entity2.BoolNullable = &i4
	entity2.FloatNullable = &i5
	entity2.City = "War'saw '; New"
	assert.True(t, engine.IsDirty(entity2))
	engine.Flush(entity2)
	bind, _ = engine.GetDirtyBind(entity)
	assert.False(t, engine.IsDirty(entity2))
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
	assert.True(t, engine.IsDirty(entity2))
	engine.Flush(entity2)
	assert.False(t, engine.IsDirty(entity2))
	entity2 = &flushEntity{}
	engine.LoadByID(10, entity2)
	assert.Nil(t, entity2.UintNullable)
	assert.Nil(t, entity2.BoolNullable)
	assert.Nil(t, entity2.FloatNullable)
	assert.Equal(t, "", entity2.City)

	engine.NewFlusher().Delete(entity2)
	assert.True(t, engine.IsDirty(entity2))
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
	entity7 = &flushEntity{Name: "test_check_2", EnumNotNull: "a", ReferenceOne: &flushEntityReference{}}
	entity7.ReferenceOne.SetID(100)
	err = engine.FlushWithCheck(entity7)
	assert.EqualError(t, err, "foreign key error in key `test:flushEntity:ReferenceOne`")

	flusher.Clear()
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
	entity2 = &flushEntity{Name: "Eva", Age: 1, EnumNotNull: "a"}
	entity2.SetID(100)
	entity2.SetOnDuplicateKeyUpdate(beeorm.Bind{"Age": "2"})
	engine.Flush(entity2)
	assert.Equal(t, uint64(12), entity2.GetID())
	assert.Equal(t, 2, entity2.Age)
	entity2 = &flushEntity{}
	found = engine.LoadByID(100, entity2)
	assert.False(t, found)
	entity2 = &flushEntity{Name: "Frank", Age: 1, EnumNotNull: "a"}
	entity2.SetID(100)
	entity2.SetOnDuplicateKeyUpdate(beeorm.Bind{"Age": "2"})
	engine.Flush(entity2)
	entity2 = &flushEntity{}
	found = engine.LoadByID(100, entity2)
	assert.True(t, found)
	assert.Equal(t, 1, entity2.Age)

	entity2 = &flushEntity{Age: 1, EnumNotNull: "a"}
	entity2.SetID(100)
	entity2.SetOnDuplicateKeyUpdate(beeorm.Bind{"Age": "2"})
	engine.Flush(entity2)
	assert.Equal(t, uint64(100), entity2.GetID())
	assert.Equal(t, 2, entity2.Age)
	entity2 = &flushEntity{}
	found = engine.LoadByID(100, entity2)
	assert.True(t, found)
	assert.Equal(t, 2, entity2.Age)

	flusher.Clear()
	engine.LoadByID(6, entity)
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
	engine.LoadByID(100, entity)
	entity.Float64Default = 0.3
	assert.True(t, engine.IsDirty(entity))
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(100, entity)
	assert.Equal(t, 0.3, entity.Float64Default)
	entity.Float64Default = 0.4
	entity.Float64Signed = -0.4
	assert.True(t, engine.IsDirty(entity))
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(100, entity)
	assert.Equal(t, 0.4, entity.Float64Default)
	assert.Equal(t, -0.4, entity.Float64Signed)

	entity.SetNullable = []string{}
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(100, entity)
	assert.Nil(t, nil, entity.SetNullable)
	entity.SetNullable = []string{"d"}
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(100, entity)
	assert.Equal(t, []string{"d"}, entity.SetNullable)
	entity.SetNullable = []string{"f"}
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(100, entity)
	assert.Equal(t, []string{"f"}, entity.SetNullable)
	entity.SetNullable = []string{"f", "d"}
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(100, entity)
	assert.Len(t, entity.SetNullable, 2)
	assert.False(t, engine.IsDirty(entity))
	entity.SetNullable = []string{"f", "d"}
	assert.False(t, engine.IsDirty(entity))

	date = time.Unix(1, 0).UTC()
	entity = &flushEntity{}
	engine.LoadByID(11, entity)
	entity.TimeWithTime = date
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
	engine.LoadByID(11, entity)
	assert.Equal(t, date, entity.TimeWithTime.UTC())
	assert.Nil(t, entity.UintNullable)
	assert.Nil(t, entity.Int8Nullable)
	assert.Nil(t, entity.BoolNullable)
	assert.Nil(t, entity.FloatNullable)
	assert.Nil(t, entity.TimeNullable)
	assert.Nil(t, entity.TimeWithTimeNullable)
	assert.Nil(t, entity.Interface)

	entity = &flushEntity{}
	engine.LoadByID(100, entity)
	engine.Delete(entity)
	entity = &flushEntity{}
	engine.GetLocalCache().Clear()
	engine.GetRedis().FlushDB()
	assert.True(t, engine.LoadByID(100, entity))
	assert.True(t, entity.FakeDelete)
	assert.False(t, engine.IsDirty(entity))
	engine.ForceDelete(entity)
	engine.GetLocalCache().Clear()
	engine.GetRedis().FlushDB()
	assert.False(t, engine.LoadByID(100, entity))

	entity = &flushEntity{}
	engine.LoadByID(1, entity)
	engine.ForceDelete(entity)
	flusher = engine.NewFlusher()
	flusher.Track(&flushEntityReference{})
	flusher.Track(&flushEntityReference{})
	flusher.Flush()
	flusher = engine.NewFlusher()
	flusher.Track(&flushEntityReference{})
	flusher.Track(&flushEntity{})
	flusher.Flush()

	flusher = engine.NewFlusher()
	flusher.Track(&flushEntityReference{})
	flusher.Track(&flushEntity{Name: "Adam"})
	err = flusher.FlushWithCheck()
	assert.NotNil(t, err)

	entity = schema.NewEntity().(*flushEntity)
	entity.Name = "WithID"
	entity.SetID(676)
	engine.Flush(entity)
	entity = &flushEntity{}
	assert.True(t, engine.LoadByID(676, entity))

	entity = &flushEntity{}
	engine.LoadByID(13, entity)
	entity.City = "Warsaw"
	entity.SubName = "testSub"
	engine.Flush(entity)
	clonedEntity := entity.Clone().(*flushEntity)
	assert.Equal(t, uint64(0), clonedEntity.GetID())
	assert.False(t, clonedEntity.IsLoaded())
	assert.True(t, engine.IsDirty(clonedEntity))
	assert.Equal(t, "Warsaw", clonedEntity.City)
	assert.Equal(t, "test_transaction", clonedEntity.Name)
	assert.Equal(t, "testSub", clonedEntity.SubName)
	assert.Equal(t, 38, clonedEntity.Age)
	clonedEntity.Name = "Cloned"
	clonedEntity.City = "Cracow"
	clonedEntity.ReferenceOne = nil
	engine.Flush(clonedEntity)
	entity = &flushEntity{}
	assert.True(t, engine.LoadByID(677, entity))
	assert.Equal(t, 38, clonedEntity.Age)
	assert.Equal(t, "testSub", clonedEntity.SubName)
}
