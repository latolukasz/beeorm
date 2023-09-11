package beeorm

//
//import (
//	"encoding/json"
//	"fmt"
//	"testing"
//	"time"
//
//	"github.com/stretchr/testify/assert"
//)
//
//type obj struct {
//	ID         uint64
//	StorageKey string
//	Data       interface{}
//}
//
//type flushStruct struct {
//	Name2    string
//	Age      int
//	Sub      flushSubStruct
//	TestTime *time.Time `orm:"time=true"`
//}
//
//type flushSubStruct struct {
//	Name3 string
//	Age3  int
//}
//
//type flushStructAnonymous struct {
//	SubName string
//	SubAge  float32 `orm:"decimal=9,5;unsigned=false"`
//}
//
//var testSet = struct {
//	D string
//	E string
//	F string
//}{
//	D: "d",
//	E: "e",
//	F: "f",
//}
//
//var testEnum = struct {
//	A string
//	B string
//	C string
//}{
//	A: "a",
//	B: "b",
//	C: "c",
//}
//
//type attributesValues map[uint64][]interface{}
//
//func (av attributesValues) UnmarshalJSON(data []byte) error {
//	temp := map[uint64][]interface{}{}
//	if err := json.Unmarshal(data, &temp); err != nil {
//		return err
//	}
//
//	for attributeID, values := range temp {
//		valuesNew := make([]interface{}, len(values))
//
//		for i, value := range values {
//			if _, ok := value.(string); ok {
//				valuesNew[i] = value
//			} else {
//				valuesNew[i] = uint64(value.(float64))
//			}
//		}
//		av[attributeID] = valuesNew
//	}
//
//	return nil
//}
//
//type flushEntity struct {
//	ORM                  `orm:"localCache;redisCache"`
//	ID                   uint64
//	City                 string `orm:"unique=city"`
//	Name                 string `orm:"unique=name;required"`
//	NameTranslated       map[string]string
//	Age                  int
//	Uint                 uint
//	UintNullable         *uint
//	IntNullable          *int
//	Year                 uint16  `orm:"year"`
//	YearNullable         *uint16 `orm:"year"`
//	BoolNullable         *bool
//	FloatNullable        *float64              `orm:"precision=10"`
//	Float32Nullable      *float32              `orm:"precision=4"`
//	ReferenceOne         *flushEntityReference `orm:"unique=ReferenceOne"`
//	ReferenceTwo         *flushEntityReference `orm:"unique=ReferenceTwo"`
//	StringSlice          []string
//	StringSliceNotNull   []string `orm:"required"`
//	SetNullable          []string `orm:"set=beeorm.testSet"`
//	SetNotNull           []string `orm:"set=beeorm.testSet;required"`
//	EnumNullable         string   `orm:"enum=beeorm.testEnum"`
//	EnumNotNull          string   `orm:"enum=beeorm.testEnum;required"`
//	Ignored              []string `orm:"ignore"`
//	Blob                 []uint8
//	Bool                 bool
//	Float64              float64  `orm:"precision=10"`
//	Decimal              float64  `orm:"decimal=5,2"`
//	DecimalNullable      *float64 `orm:"decimal=5,2"`
//	Float64Default       float64  `orm:"unsigned"`
//	Float64Signed        float64
//	CachedQuery          *CachedQuery
//	Time                 time.Time
//	TimeWithTime         time.Time `orm:"time"`
//	TimeNullable         *time.Time
//	TimeWithTimeNullable *time.Time `orm:"time"`
//	Interface            interface{}
//	FlushStruct          flushStruct
//	FlushStructPtr       *flushStruct
//	Int8Nullable         *int8
//	Int16Nullable        *int16
//	Int32Nullable        *int32
//	Int64Nullable        *int64
//	Uint8Nullable        *uint8
//	Uint16Nullable       *uint16
//	Uint32Nullable       *uint32
//	Uint64Nullable       *uint64
//	Images               []obj
//	AttributesValues     attributesValues
//	flushStructAnonymous
//}
//
//type flushEntityReference struct {
//	ORM  `orm:"localCache;redisCache"`
//	ID   uint64
//	Name string
//	Age  int
//}
//
//func TestFlushLocalRedis(t *testing.T) {
//	testFlush(t, true, true)
//}
//
//func TestFlushLocal(t *testing.T) {
//	testFlush(t, true, false)
//}
//
//func TestFlushNoCache(t *testing.T) {
//	testFlush(t, false, false)
//}
//
//func TestFlushRedis(t *testing.T) {
//	testFlush(t, false, true)
//}
//
//func testFlush(t *testing.T, local bool, redis bool) {
//	var entity *flushEntity
//	var reference *flushEntityReference
//	registry := &Registry{}
//	registry.RegisterRedisStream("entity_changed", DefaultPoolCode)
//	registry.RegisterRedisStreamConsumerGroups("entity_changed", "test-group-1")
//	registry.RegisterEnumStruct("beeorm.testEnum", testEnum)
//	registry.RegisterEnumStruct("beeorm.testSet", testSet)
//	c := PrepareTables(t, registry, 5, 6, "", entity, reference)
//
//	schema := GetEntitySchema[*flushEntity](c)
//	schema2 := GetEntitySchema[*flushEntityReference](c)
//	schema.DisableCache(!local, !redis)
//	schema2.DisableCache(!local, !redis)
//
//	date := time.Date(2049, 1, 12, 18, 34, 40, 0, time.UTC)
//	entity = &flushEntity{Name: "Tom", Age: 12, Uint: 7, Year: 1982}
//	entity.NameTranslated = map[string]string{"pl": "kot", "en": "cat"}
//	entity.ReferenceOne = &flushEntityReference{Name: "John", Age: 30}
//	entity.StringSlice = []string{"a", "b"}
//	entity.StringSliceNotNull = []string{"c", "d"}
//	entity.SetNotNull = []string{"d", "e"}
//	entity.FlushStructPtr = &flushStruct{"A", 12, flushSubStruct{Age3: 11, Name3: "G"}, nil}
//	entity.EnumNotNull = "a"
//	entity.FlushStruct.Name2 = "Ita"
//	entity.FlushStruct.Sub.Age3 = 13
//	entity.FlushStruct.Sub.Name3 = "Nanami"
//	entity.FlushStruct.TestTime = &date
//	entity.TimeWithTime = date
//	entity.Float64 = 2.12
//	entity.Decimal = 6.15
//	entity.TimeWithTimeNullable = &date
//	entity.Images = []obj{{ID: 1, StorageKey: "aaa", Data: map[string]string{"sss": "vv", "bb": "cc"}}}
//	entity.flushStructAnonymous = flushStructAnonymous{"Adam", 39.123}
//	entity.AttributesValues = attributesValues{12: []interface{}{"a", "b"}}
//	isDirty, _ := IsDirty(c, entity)
//	assert.True(t, isDirty)
//	isDirty, flushData := IsDirty(c, entity.ReferenceOne)
//	assert.True(t, isDirty)
//	assert.NotNil(t, flushData)
//
//	flusher := c.Flusher().Track(entity)
//	flusher.Track(entity).Flush()
//	isDirty, flushData = IsDirty(c, entity)
//	assert.False(t, isDirty)
//	assert.Nil(t, flushData)
//	flusher.Flush()
//
//	assert.True(t, entity.IsLoaded())
//	assert.True(t, entity.ReferenceOne.IsLoaded())
//	isDirty, _ = IsDirty(c, entity)
//	assert.False(t, isDirty)
//	isDirty, _ = IsDirty(c, entity.ReferenceOne)
//	assert.False(t, isDirty)
//	assert.Equal(t, uint64(1), entity.GetID())
//	assert.Equal(t, uint64(1), entity.ReferenceOne.GetID())
//	assert.True(t, entity.IsLoaded())
//	assert.True(t, entity.ReferenceOne.IsLoaded())
//	refOneID := entity.ReferenceOne.GetID()
//
//	entity = GetByID[*flushEntity](c, 2)
//	assert.NotNil(t, entity)
//	entity = GetByID[*flushEntity](c, 1)
//	assert.NotNil(t, entity)
//	assert.Equal(t, uint64(1), entity.ID)
//	assert.Equal(t, "Tom", entity.Name)
//	assert.Equal(t, 12, entity.Age)
//	assert.Equal(t, uint(7), entity.Uint)
//	assert.Equal(t, uint16(1982), entity.Year)
//	assert.Equal(t, map[string]string{"pl": "kot", "en": "cat"}, entity.NameTranslated)
//	assert.Equal(t, attributesValues{12: []interface{}{"a", "b"}}, entity.AttributesValues)
//	assert.Equal(t, []string{"a", "b"}, entity.StringSlice)
//	assert.Equal(t, []string{"c", "d"}, entity.StringSliceNotNull)
//	assert.Equal(t, "", entity.EnumNullable)
//	assert.Equal(t, "a", entity.EnumNotNull)
//	assert.Equal(t, date.Format(TimeFormat), entity.TimeWithTime.UTC().Format(TimeFormat))
//	assert.Equal(t, date.Unix(), entity.TimeWithTime.Unix())
//	assert.Equal(t, date.Format(TimeFormat), entity.TimeWithTimeNullable.UTC().Format(TimeFormat))
//	assert.Equal(t, date.Unix(), entity.TimeWithTimeNullable.Unix())
//	assert.Nil(t, entity.SetNullable)
//	assert.Equal(t, "", entity.City)
//	assert.NotNil(t, entity.FlushStructPtr)
//	assert.Equal(t, "A", entity.FlushStructPtr.Name2)
//	assert.Equal(t, 12, entity.FlushStructPtr.Age)
//	assert.Equal(t, "G", entity.FlushStructPtr.Sub.Name3)
//	assert.Equal(t, 11, entity.FlushStructPtr.Sub.Age3)
//	assert.Equal(t, date.Format(TimeFormat), entity.FlushStruct.TestTime.UTC().Format(TimeFormat))
//	assert.Nil(t, entity.UintNullable)
//	assert.Nil(t, entity.IntNullable)
//	assert.Nil(t, entity.YearNullable)
//	assert.Nil(t, entity.BoolNullable)
//	assert.Nil(t, entity.FloatNullable)
//	assert.Nil(t, entity.Float32Nullable)
//	isDirty, _ = IsDirty(c, entity)
//	assert.False(t, isDirty)
//	assert.True(t, entity.IsLoaded())
//	assert.False(t, entity.ReferenceOne.IsLoaded())
//	assert.Equal(t, refOneID, entity.ReferenceOne.GetID())
//	assert.Nil(t, entity.Blob)
//	assert.Nil(t, entity.Int8Nullable)
//	assert.Nil(t, entity.Int16Nullable)
//	assert.Nil(t, entity.Int32Nullable)
//	assert.Nil(t, entity.Int64Nullable)
//	assert.Nil(t, entity.Uint8Nullable)
//	assert.Nil(t, entity.Uint16Nullable)
//	assert.Nil(t, entity.Uint32Nullable)
//	assert.Nil(t, entity.Uint64Nullable)
//	assert.Equal(t, "Adam", entity.SubName)
//	assert.Equal(t, float32(39.123), entity.SubAge)
//	assert.Equal(t, "Ita", entity.FlushStruct.Name2)
//	assert.Equal(t, 13, entity.FlushStruct.Sub.Age3)
//	assert.Equal(t, "Nanami", entity.FlushStruct.Sub.Name3)
//
//	entity.FlushStructPtr = nil
//	c.Flusher().Track(entity).Flush()
//	entity = GetByID[*flushEntity](c, 1)
//	assert.Nil(t, entity.FlushStructPtr)
//
//	entity.ReferenceOne.Name = "John 2"
//	assert.PanicsWithError(t, fmt.Sprintf("entity is not loaded and can't be updated: beeorm.flushEntityReference [%d]", refOneID), func() {
//		c.Flusher().Track(entity.ReferenceOne).Flush()
//	})
//
//	i := 42
//	i2 := uint(42)
//	i3 := uint16(1982)
//	i4 := false
//	i5 := 134.345
//	i6 := true
//	i7 := int8(4)
//	i8 := int16(4)
//	i9 := int32(4)
//	i10 := int64(4)
//	i11 := uint8(4)
//	i12 := uint16(4)
//	i13 := uint32(4)
//	i14 := uint64(4)
//	i15 := float32(134.345)
//	entity.IntNullable = &i
//	entity.UintNullable = &i2
//	entity.Int8Nullable = &i7
//	entity.Int16Nullable = &i8
//	entity.Int32Nullable = &i9
//	entity.Int64Nullable = &i10
//	entity.Uint8Nullable = &i11
//	entity.Uint16Nullable = &i12
//	entity.Uint32Nullable = &i13
//	entity.Uint64Nullable = &i14
//	entity.YearNullable = &i3
//	entity.BoolNullable = &i4
//	entity.FloatNullable = &i5
//	entity.Float32Nullable = &i15
//	entity.City = "New York"
//	entity.Blob = []uint8("Tom has a house")
//	entity.Bool = true
//	entity.BoolNullable = &i6
//	entity.Float64 = 134.345
//	entity.Decimal = 134.345
//	entity.StringSlice = []string{"a"}
//	entity.DecimalNullable = &entity.Decimal
//	entity.Interface = map[string]int{"test": 12}
//
//	c.Flusher().Track(entity).Flush()
//
//	reference = &flushEntityReference{}
//	reference = GetByID[*flushEntityReference](c, refOneID)
//	assert.NotNil(t, reference)
//	assert.Equal(t, "John", reference.Name)
//	assert.Equal(t, 30, reference.Age)
//
//	entity = GetByID[*flushEntity](c, 1)
//	assert.Equal(t, uint64(1), entity.ID)
//	assert.Equal(t, 42, *entity.IntNullable)
//	assert.Equal(t, int8(4), *entity.Int8Nullable)
//	assert.Equal(t, int16(4), *entity.Int16Nullable)
//	assert.Equal(t, int32(4), *entity.Int32Nullable)
//	assert.Equal(t, int64(4), *entity.Int64Nullable)
//	assert.Equal(t, uint8(4), *entity.Uint8Nullable)
//	assert.Equal(t, uint16(4), *entity.Uint16Nullable)
//	assert.Equal(t, uint32(4), *entity.Uint32Nullable)
//	assert.Equal(t, uint64(4), *entity.Uint64Nullable)
//	assert.Equal(t, uint(42), *entity.UintNullable)
//	assert.Equal(t, uint16(1982), *entity.YearNullable)
//	assert.True(t, *entity.BoolNullable)
//	assert.True(t, entity.Bool)
//	assert.Equal(t, 134.345, *entity.FloatNullable)
//	assert.Equal(t, float32(134.345), *entity.Float32Nullable)
//	assert.Equal(t, []string{"a"}, entity.StringSlice)
//	assert.Equal(t, "New York", entity.City)
//	assert.Equal(t, []uint8("Tom has a house"), entity.Blob)
//	assert.Equal(t, 134.345, entity.Float64)
//	assert.Equal(t, 134.35, entity.Decimal)
//	assert.Equal(t, 134.35, *entity.DecimalNullable)
//	assert.Equal(t, float32(39.123), entity.SubAge)
//	isDirty, _ = IsDirty(c, entity)
//	assert.False(t, isDirty)
//	isDirty, _ = IsDirty(c, reference)
//	assert.False(t, isDirty)
//	assert.True(t, reference.IsLoaded())
//
//	entity2 := &flushEntity{Name: "Tom", Age: 12, EnumNotNull: "a"}
//	assert.PanicsWithError(t, "Error 1062 (23000): Duplicate entry 'Tom' for key 'name'", func() {
//		c.Flusher().Track(entity2).Flush()
//	})
//
//	entity2.ReferenceOne = nil
//	entity2.Name = "Tom"
//	entity2.SetOnDuplicateKeyUpdate(Bind{"Age": 40, "Year": 2020, "City": "Moscow", "UintNullable": nil,
//		"BoolNullable": nil, "TimeWithTime": date, "Time": date})
//	c.Flusher().Track(entity2).Flush()
//
//	assert.Equal(t, uint64(1), entity2.GetID())
//	assert.Equal(t, 40, entity2.Age)
//	entity = GetByID[*flushEntity](c, 1)
//	assert.NotNil(t, entity)
//	assert.Equal(t, "Tom", entity.Name)
//	assert.Equal(t, "Moscow", entity.City)
//	assert.Nil(t, entity.UintNullable)
//	assert.Equal(t, 40, entity.Age)
//	assert.Equal(t, uint64(1), entity.GetID())
//	assert.Equal(t, date.Unix(), entity.TimeWithTime.Unix())
//	assert.Equal(t, entity.Time.Format(DateFormat), date.Format(DateFormat))
//
//	entity2 = &flushEntity{Name: "Tom", Age: 12, EnumNotNull: "a"}
//	entity2.SetOnDuplicateKeyUpdate(Bind{})
//	c.Flusher().Track(entity2).Flush()
//	assert.Equal(t, uint64(1), entity2.GetID())
//	entity = GetByID[*flushEntity](c, 1)
//	assert.Equal(t, uint64(1), entity.GetID())
//
//	entity2 = &flushEntity{Name: "Arthur", Age: 18, EnumNotNull: "a"}
//	entity2.ReferenceTwo = reference
//	entity2.SetOnDuplicateKeyUpdate(Bind{})
//	c.Flusher().Track(entity2).Flush()
//	assert.Equal(t, uint64(5), entity2.GetID())
//
//	entity = GetByID[*flushEntity](c, 5)
//	assert.Equal(t, uint64(5), entity.GetID())
//
//	entity = GetByID[*flushEntity](c, 1)
//	entity.Bool = false
//	date = date.Add(time.Hour * 40)
//	entity.TimeWithTime = date
//	entity.Name = ""
//	entity.IntNullable = nil
//	entity.EnumNullable = "b"
//	entity.Blob = nil
//	c.Flusher().Track(entity).Flush()
//	entity = GetByID[*flushEntity](c, 5)
//	assert.Equal(t, false, entity.Bool)
//	assert.Equal(t, date.Format(TimeFormat), entity.TimeWithTime.UTC().Format(TimeFormat))
//	assert.Equal(t, "", entity.Name)
//	assert.Equal(t, "b", entity.EnumNullable)
//	assert.Nil(t, entity.IntNullable)
//	assert.Nil(t, entity.Blob)
//	entity.EnumNullable = ""
//	entity.Blob = []uint8("Tom has a house")
//	c.Flusher().Track(entity).Flush()
//	entity = GetByID[*flushEntity](c, 1)
//	assert.Equal(t, "", entity.EnumNullable)
//
//	assert.PanicsWithError(t, "empty enum value for EnumNotNull", func() {
//		entity.EnumNotNull = ""
//		c.Flusher().Track(entity).Flush()
//	})
//
//	entity = &flushEntity{Name: "Cat"}
//	c.Flusher().Track(entity).Flush()
//	id := entity.GetID()
//	entity = GetByID[*flushEntity](c, id)
//	assert.Equal(t, "a", entity.EnumNotNull)
//
//	entity2 = &flushEntity{ID: 10, Name: "Adam", Age: 20, EnumNotNull: "a"}
//	c.Flusher().Track(entity2).Flush()
//	entity2 = GetByID[*flushEntity](c, 10)
//	assert.NotNil(t, entity2)
//
//	entity2.Age = 21
//	entity2.UintNullable = &i2
//	entity2.BoolNullable = &i4
//	entity2.FloatNullable = &i5
//	entity2.City = "War'saw '; New"
//	isDirty, _ = IsDirty(c, entity2)
//	assert.True(t, isDirty)
//	c.Flusher().Track(entity2).Flush()
//	isDirty, _ = IsDirty(c, entity)
//	assert.False(t, isDirty)
//	entity2 = GetByID[*flushEntity](c, 10)
//	assert.Equal(t, 21, entity2.Age)
//	entity2.City = "War\\'saw"
//	c.Flusher().Track(entity2).Flush()
//	entity2 = GetByID[*flushEntity](c, 10)
//	assert.Equal(t, "War\\'saw", entity2.City)
//	entity2.Time = time.Now()
//	n := time.Now()
//	entity2.TimeNullable = &n
//	c.Flusher().Track(entity2).Flush()
//	entity2 = GetByID[*flushEntity](c, 10)
//	assert.NotNil(t, entity2.Time)
//	assert.NotNil(t, entity2.TimeNullable)
//
//	entity2.UintNullable = nil
//	entity2.BoolNullable = nil
//	entity2.FloatNullable = nil
//	entity2.City = ""
//	isDirty, _ = IsDirty(c, entity2)
//	assert.True(t, isDirty)
//	c.Flusher().Track(entity2).Flush()
//	isDirty, _ = IsDirty(c, entity2)
//	assert.False(t, isDirty)
//	entity2 = GetByID[*flushEntity](c, 10)
//	assert.Nil(t, entity2.UintNullable)
//	assert.Nil(t, entity2.BoolNullable)
//	assert.Nil(t, entity2.FloatNullable)
//	assert.Equal(t, "", entity2.City)
//
//	c.Flusher().Delete(entity2).Flush()
//	isDirty, _ = IsDirty(c, entity)
//	assert.True(t, isDirty)
//	c.Flusher().Delete(entity2).Flush()
//	entity2 = GetByID[*flushEntity](c, 10)
//	assert.NotNil(t, entity2)
//
//	c.Flusher().Track(&flushEntity{Name: "Tom", Age: 12, Uint: 7, Year: 1982, EnumNotNull: "a"}).Flush()
//	entity3 := GetByID[*flushEntity](c, 11)
//	assert.NotNil(t, entity3)
//	assert.Nil(t, entity3.NameTranslated)
//
//	c.Flusher().Track(&flushEntity{Name: "Eva", SetNullable: []string{}, EnumNotNull: "a"}).Flush()
//	entity4 := GetByID[*flushEntity](c, 12)
//	assert.NotNil(t, entity4)
//	assert.Nil(t, entity4.SetNotNull)
//	assert.Nil(t, entity4.SetNullable)
//	entity4.SetNullable = []string{"d", "e"}
//	c.Flusher().Track(entity4).Flush()
//	entity4 = GetByID[*flushEntity](c, 12)
//	assert.Equal(t, []string{"d", "e"}, entity4.SetNullable)
//
//	c.Engine().DB(DefaultPoolCode).Begin(c)
//	entity5 := &flushEntity{Name: "test_transaction", EnumNotNull: "a"}
//	c.Flusher().Track(entity5).Flush()
//	entity5.Age = 38
//	c.Flusher().Track(entity5).Flush()
//	c.Engine().DB(DefaultPoolCode).Commit(c)
//	entity5 = GetByID[*flushEntity](c, 13)
//	assert.NotNil(t, entity5)
//	assert.Equal(t, "test_transaction", entity5.Name)
//	assert.Equal(t, 38, entity5.Age)
//
//	entity6 := &flushEntity{Name: "test_transaction_2", EnumNotNull: "a"}
//	flusher.Clear()
//	flusher.Flush()
//	flusher.Track(entity6)
//	flusher.Flush()
//	entity6 = GetByID[*flushEntity](c, 14)
//	assert.NotNil(t, entity6)
//	assert.Equal(t, "test_transaction_2", entity6.Name)
//
//	entity7 := &flushEntity{Name: "test_check", EnumNotNull: "a"}
//	flusher.Track(entity7)
//	err := flusher.FlushWithCheck()
//	assert.NoError(t, err)
//	entity7 = GetByID[*flushEntity](c, 14)
//	assert.NotNil(t, entity7)
//	assert.Equal(t, "test_check", entity7.Name)
//
//	entity7 = &flushEntity{Name: "test_check", EnumNotNull: "a"}
//	flusher.Track(entity7)
//	err = flusher.FlushWithCheck()
//	assert.EqualError(t, err, "Duplicate entry 'test_check' for key 'name'")
//
//	flusher.Clear()
//	entity7 = &flushEntity{Name: "test_check_3", EnumNotNull: "Y"}
//	flusher.Track(entity7)
//	err = flusher.FlushWithFullCheck()
//	assert.EqualError(t, err, "unknown enum value for EnumNotNull - Y")
//	flusher.Track(entity7)
//	assert.Panics(t, func() {
//		_ = flusher.FlushWithCheck()
//	})
//
//	entity8 := &flushEntity{Name: "test_check", EnumNotNull: "a"}
//	flusher.Track(entity8)
//	err = flusher.FlushWithCheck()
//	assert.EqualError(t, err, "Duplicate entry 'test_check' for key 'name'")
//
//	assert.PanicsWithError(t, "track limit 10000 exceeded", func() {
//		for i := 1; i <= 10001; i++ {
//			flusher.Track(&flushEntity{})
//		}
//	})
//
//	flusher.Clear()
//	entity2 = &flushEntity{ID: 100, Name: "Eva", Age: 1, EnumNotNull: "a"}
//	entity2.SetOnDuplicateKeyUpdate(Bind{"Age": 2})
//	c.Flusher().Track(entity2).Flush()
//	assert.Equal(t, uint64(12), entity2.GetID())
//	assert.Equal(t, 2, entity2.Age)
//	entity2 = GetByID[*flushEntity](c, 100)
//	assert.Nil(t, entity2)
//	entity2 = &flushEntity{ID: 100, Name: "Frank", Age: 1, EnumNotNull: "a"}
//	entity2.SetOnDuplicateKeyUpdate(Bind{"Age": 2})
//	c.Flusher().Track(entity2).Flush()
//	entity2 = GetByID[*flushEntity](c, 100)
//	assert.NotNil(t, entity2)
//	assert.Equal(t, 1, entity2.Age)
//
//	entity2 = &flushEntity{ID: 100, Age: 1, EnumNotNull: "a"}
//	entity2.SetOnDuplicateKeyUpdate(Bind{"Age": 2})
//	c.Flusher().Track(entity2).Flush()
//	assert.Equal(t, uint64(100), entity2.GetID())
//	assert.Equal(t, 2, entity2.Age)
//	entity2 = &flushEntity{}
//	entity2 = GetByID[*flushEntity](c, 100)
//	assert.NotNil(t, entity2)
//	assert.Equal(t, 2, entity2.Age)
//
//	flusher.Clear()
//	entity = GetByID[*flushEntity](c, 6)
//	flusher.Delete(entity)
//	entity = GetByID[*flushEntity](c, 7)
//	flusher.Delete(entity)
//	flusher.Flush()
//	entity = GetByID[*flushEntity](c, 6)
//	assert.Nil(t, entity2)
//	entity = GetByID[*flushEntity](c, 7)
//	assert.Nil(t, entity)
//
//	entity = GetByID[*flushEntity](c, 100)
//	entity.Float64Default = 0.3
//	isDirty, _ = IsDirty(c, entity)
//	assert.True(t, isDirty)
//	c.Flusher().Track(entity).Flush()
//	entity = GetByID[*flushEntity](c, 100)
//	assert.Equal(t, 0.3, entity.Float64Default)
//	entity.Float64Default = 0.4
//	entity.Float64Signed = -0.4
//	isDirty, _ = IsDirty(c, entity)
//	assert.True(t, isDirty)
//	c.Flusher().Track(entity).Flush()
//	entity = GetByID[*flushEntity](c, 100)
//	assert.Equal(t, 0.4, entity.Float64Default)
//	assert.Equal(t, -0.4, entity.Float64Signed)
//
//	entity.SetNullable = []string{}
//	c.Flusher().Track(entity).Flush()
//	entity = GetByID[*flushEntity](c, 100)
//	assert.Nil(t, nil, entity.SetNullable)
//	entity.SetNullable = []string{"d"}
//	c.Flusher().Track(entity).Flush()
//	entity = GetByID[*flushEntity](c, 100)
//	assert.Equal(t, []string{"d"}, entity.SetNullable)
//	entity.SetNullable = []string{"f"}
//	c.Flusher().Track(entity).Flush()
//	entity = GetByID[*flushEntity](c, 100)
//	assert.Equal(t, []string{"f"}, entity.SetNullable)
//	entity.SetNullable = []string{"f", "d"}
//	c.Flusher().Track(entity).Flush()
//	entity = GetByID[*flushEntity](c, 100)
//	assert.Len(t, entity.SetNullable, 2)
//	isDirty, _ = IsDirty(c, entity)
//	assert.False(t, isDirty)
//	entity.SetNullable = []string{"f", "d"}
//	isDirty, _ = IsDirty(c, entity)
//	assert.False(t, isDirty)
//
//	date = time.Unix(1, 0).UTC()
//	entity = GetByID[*flushEntity](c, 11)
//	entity.TimeWithTime = date
//	c.Flusher().Track(entity).Flush()
//	entity = &flushEntity{}
//	i2 = 13
//	i7 = 3
//	i4 = true
//	i5 = 12.33
//	n = time.Now()
//	entity.UintNullable = &i2
//	entity.Int8Nullable = &i7
//	entity.BoolNullable = &i4
//	entity.FloatNullable = &i5
//	entity.TimeNullable = &n
//	entity.TimeWithTimeNullable = &n
//	entity.Interface = "ss"
//	entity = GetByID[*flushEntity](c, 11)
//	assert.Equal(t, date, entity.TimeWithTime.UTC())
//	assert.Nil(t, entity.UintNullable)
//	assert.Nil(t, entity.Int8Nullable)
//	assert.Nil(t, entity.BoolNullable)
//	assert.Nil(t, entity.FloatNullable)
//	assert.Nil(t, entity.TimeNullable)
//	assert.Nil(t, entity.TimeWithTimeNullable)
//	assert.Nil(t, entity.Interface)
//
//	entity = &flushEntity{}
//	entity = GetByID[*flushEntity](c, 100)
//	c.Flusher().Delete(entity).Flush()
//	entity = &flushEntity{}
//	c.Engine().LocalCache(DefaultPoolCode).Clear(c)
//	c.Engine().Redis(DefaultPoolCode).FlushDB(c)
//	entity = GetByID[*flushEntity](c, 100)
//	assert.Nil(t, entity)
//
//	entity = GetByID[*flushEntity](c, 1)
//	c.Flusher().Delete(entity).Flush()
//	flusher = c.Flusher()
//	flusher.Track(&flushEntityReference{})
//	flusher.Track(&flushEntityReference{})
//	flusher.Flush()
//	flusher = c.Flusher()
//	flusher.Track(&flushEntityReference{})
//	flusher.Track(&flushEntity{})
//	flusher.Flush()
//
//	flusher = c.Flusher()
//	flusher.Track(&flushEntityReference{})
//	flusher.Track(&flushEntity{Name: "Adam"})
//	err = flusher.FlushWithCheck()
//	assert.Nil(t, err)
//
//	entity = schema.NewEntity().(*flushEntity)
//	entity.Name = "WithID"
//	entity.ID = 676
//	c.Flusher().Track(entity).Flush()
//	entity = GetByID[*flushEntity](c, 676)
//	assert.NotNil(t, entity)
//
//	entity = GetByID[*flushEntity](c, 13)
//	entity.City = "Warsaw"
//	entity.SubName = "testSub"
//	c.Flusher().Track(entity).Flush()
//	clonedEntity := entity.Clone().(*flushEntity)
//	assert.Equal(t, uint64(0), clonedEntity.GetID())
//	assert.False(t, clonedEntity.IsLoaded())
//	isDirty, _ = IsDirty(c, clonedEntity)
//	assert.NotNil(t, isDirty)
//	assert.Equal(t, "Warsaw", clonedEntity.City)
//	assert.Equal(t, "test_transaction", clonedEntity.Name)
//	assert.Equal(t, "testSub", clonedEntity.SubName)
//	assert.Equal(t, 38, clonedEntity.Age)
//	clonedEntity.Name = "Cloned"
//	clonedEntity.City = "Cracow"
//	clonedEntity.ReferenceOne = nil
//	c.Flusher().Track(clonedEntity).Flush()
//	entity = GetByID[*flushEntity](c, 677)
//	assert.NotNil(t, entity)
//	assert.Equal(t, 38, clonedEntity.Age)
//	assert.Equal(t, "testSub", clonedEntity.SubName)
//}
