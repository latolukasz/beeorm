package beeorm

import (
	"context"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type redisSearchEntity struct {
	ORM             `orm:"redisSearch=search"`
	ID              uint               `orm:"searchable;sortable"`
	Age             uint64             `orm:"searchable;sortable"`
	Balance         int64              `orm:"sortable"`
	Weight          float64            `orm:"searchable"`
	AgeNullable     *uint64            `orm:"searchable;sortable"`
	BalanceNullable *int64             `orm:"searchable;sortable"`
	Enum            string             `orm:"enum=beeorm.TestEnum;required;searchable"`
	EnumNullable    string             `orm:"enum=beeorm.TestEnum;searchable"`
	Name            string             `orm:"searchable"`
	NameStem        string             `orm:"searchable;stem"`
	Set             []string           `orm:"set=beeorm.TestEnum;required;searchable"`
	SetNullable     []string           `orm:"set=beeorm.TestEnum;searchable"`
	Bool            bool               `orm:"searchable;sortable"`
	BoolNullable    *bool              `orm:"searchable"`
	WeightNullable  *float64           `orm:"searchable"`
	Date            time.Time          `orm:"searchable"`
	DateTime        time.Time          `orm:"time;searchable"`
	DateNullable    *time.Time         `orm:"searchable"`
	Ref             *redisSearchEntity `orm:"searchable"`
	Another         string
	AnotherNumeric  int64
	AnotherTag      bool
	FakeDelete      bool
	Balance32       int32 `orm:"sortable"`
}

func TestEntityRedisSearch(t *testing.T) {
	var entity *redisSearchEntity
	registry := &Registry{}
	registry.RegisterEnumStruct("beeorm.TestEnum", TestEnum)
	engine := PrepareTables(t, registry, 5, entity)

	assert.Len(t, engine.GetRedisSearch().ListIndices(), 1)

	indexer := NewBackgroundConsumer(engine)
	indexer.DisableLoop()
	indexer.blockTime = time.Millisecond

	flusher := engine.NewFlusher()
	now := time.Now()

	list := make([]*redisSearchEntity, 0)
	for i := 1; i <= 50; i++ {
		e := &redisSearchEntity{Age: uint64(i)}
		list = append(list, e)
		e.Weight = 100.3 + float64(i)
		e.Balance = 20 - int64(i)
		e.Enum = TestEnum.A
		e.Set = []string{"a"}
		e.Name = "dog " + strconv.Itoa(i)
		e.NameStem = "carrot " + strconv.Itoa(i)
		if i > 20 {
			v := uint64(i)
			e.AgeNullable = &v
			v2 := int64(i)
			e.BalanceNullable = &v2
			e.Enum = TestEnum.B
			e.Set = []string{"a", "b"}
			e.SetNullable = []string{"a", "b"}
			e.EnumNullable = TestEnum.B
			e.Name = "Cat " + strconv.Itoa(i)
			e.NameStem = "Orange " + strconv.Itoa(i)
			b := false
			e.BoolNullable = &b
			f := 10.2
			e.WeightNullable = &f
			e.Date = now
			e.DateTime = now
			e.DateNullable = &now
		}
		if i > 40 {
			e.Enum = TestEnum.C
			e.EnumNullable = TestEnum.C
			e.Set = []string{"a", "b", "c"}
			e.SetNullable = []string{"a", "b", "c"}
			e.Name = "cats " + strconv.Itoa(i)
			e.NameStem = "oranges " + strconv.Itoa(i)
			e.Bool = true
			b := true
			e.BoolNullable = &b
			f := 20.2
			e.WeightNullable = &f
			e.Date = now.Add(time.Hour * 48)
			e.DateTime = e.Date
		}
		flusher.Track(e)
	}
	flusher.Flush()
	list[0].Ref = list[30]
	list[1].Ref = list[30]
	list[2].Ref = list[31]
	list[3].Ref = list[31]
	list[4].Ref = list[31]
	flusher.Flush()

	indices := engine.GetRedisSearch("search").ListIndices()
	assert.Len(t, indices, 1)
	assert.Equal(t, "beeorm.redisSearchEntity", indices[0])
	info := engine.GetRedisSearch("search").Info(indices[0])
	assert.False(t, info.Indexing)
	assert.True(t, info.Options.NoFreqs)
	assert.False(t, info.Options.NoFields)
	assert.True(t, info.Options.NoOffsets)
	assert.False(t, info.Options.MaxTextFields)
	assert.Equal(t, []string{"7499e:"}, info.Definition.Prefixes)
	assert.Len(t, info.Fields, 20)
	assert.Equal(t, "ID", info.Fields[0].Name)
	assert.Equal(t, "NUMERIC", info.Fields[0].Type)
	assert.True(t, info.Fields[0].Sortable)
	assert.False(t, info.Fields[0].NoIndex)
	assert.Equal(t, "Age", info.Fields[1].Name)
	assert.Equal(t, "NUMERIC", info.Fields[1].Type)
	assert.True(t, info.Fields[1].Sortable)
	assert.False(t, info.Fields[1].NoIndex)
	assert.Equal(t, "Balance", info.Fields[2].Name)
	assert.Equal(t, "NUMERIC", info.Fields[2].Type)
	assert.True(t, info.Fields[2].Sortable)
	assert.True(t, info.Fields[2].NoIndex)
	assert.Equal(t, "Weight", info.Fields[3].Name)
	assert.Equal(t, "NUMERIC", info.Fields[3].Type)
	assert.False(t, info.Fields[3].Sortable)
	assert.False(t, info.Fields[3].NoIndex)
	assert.Equal(t, "AgeNullable", info.Fields[4].Name)
	assert.Equal(t, "NUMERIC", info.Fields[4].Type)
	assert.True(t, info.Fields[4].Sortable)
	assert.False(t, info.Fields[4].NoIndex)
	assert.Equal(t, "BalanceNullable", info.Fields[5].Name)
	assert.Equal(t, "NUMERIC", info.Fields[5].Type)
	assert.True(t, info.Fields[5].Sortable)
	assert.False(t, info.Fields[5].NoIndex)
	assert.Equal(t, "Enum", info.Fields[6].Name)
	assert.Equal(t, "TAG", info.Fields[6].Type)
	assert.False(t, info.Fields[6].Sortable)
	assert.False(t, info.Fields[6].NoIndex)
	assert.Equal(t, "EnumNullable", info.Fields[7].Name)
	assert.Equal(t, "TAG", info.Fields[7].Type)
	assert.False(t, info.Fields[7].Sortable)
	assert.False(t, info.Fields[7].NoIndex)
	assert.Equal(t, "Name", info.Fields[8].Name)
	assert.Equal(t, "TEXT", info.Fields[8].Type)
	assert.False(t, info.Fields[8].Sortable)
	assert.False(t, info.Fields[8].NoIndex)
	assert.True(t, info.Fields[8].NoStem)
	assert.Equal(t, 1.0, info.Fields[8].Weight)
	assert.Equal(t, "NameStem", info.Fields[9].Name)
	assert.Equal(t, "TEXT", info.Fields[9].Type)
	assert.False(t, info.Fields[9].Sortable)
	assert.False(t, info.Fields[9].NoIndex)
	assert.False(t, info.Fields[9].NoStem)
	assert.Equal(t, 1.0, info.Fields[9].Weight)
	assert.Equal(t, "Set", info.Fields[10].Name)
	assert.Equal(t, "TAG", info.Fields[10].Type)
	assert.False(t, info.Fields[10].Sortable)
	assert.False(t, info.Fields[10].NoIndex)
	assert.Equal(t, "SetNullable", info.Fields[11].Name)
	assert.Equal(t, "TAG", info.Fields[11].Type)
	assert.False(t, info.Fields[11].Sortable)
	assert.False(t, info.Fields[11].NoIndex)
	assert.Equal(t, "Bool", info.Fields[12].Name)
	assert.Equal(t, "TAG", info.Fields[12].Type)
	assert.True(t, info.Fields[12].Sortable)
	assert.False(t, info.Fields[12].NoIndex)
	assert.Equal(t, "BoolNullable", info.Fields[13].Name)
	assert.Equal(t, "TAG", info.Fields[13].Type)
	assert.False(t, info.Fields[13].Sortable)
	assert.False(t, info.Fields[13].NoIndex)
	assert.Equal(t, "WeightNullable", info.Fields[14].Name)
	assert.Equal(t, "NUMERIC", info.Fields[14].Type)
	assert.False(t, info.Fields[14].Sortable)
	assert.False(t, info.Fields[14].NoIndex)
	assert.Equal(t, "Date", info.Fields[15].Name)
	assert.Equal(t, "NUMERIC", info.Fields[15].Type)
	assert.False(t, info.Fields[15].Sortable)
	assert.False(t, info.Fields[15].NoIndex)
	assert.Equal(t, "DateTime", info.Fields[16].Name)
	assert.Equal(t, "NUMERIC", info.Fields[16].Type)
	assert.False(t, info.Fields[16].Sortable)
	assert.False(t, info.Fields[16].NoIndex)
	assert.Equal(t, "DateNullable", info.Fields[17].Name)
	assert.Equal(t, "NUMERIC", info.Fields[17].Type)
	assert.False(t, info.Fields[17].Sortable)
	assert.False(t, info.Fields[17].NoIndex)
	assert.Equal(t, "Ref", info.Fields[18].Name)
	assert.Equal(t, "NUMERIC", info.Fields[18].Type)
	assert.False(t, info.Fields[18].Sortable)
	assert.False(t, info.Fields[18].NoIndex)
	assert.Equal(t, "Balance32", info.Fields[19].Name)
	assert.Equal(t, "NUMERIC", info.Fields[19].Type)
	assert.True(t, info.Fields[19].Sortable)
	assert.True(t, info.Fields[19].NoIndex)

	query := NewRedisSearchQuery()
	query.Sort("Age", false)
	ids, total := engine.RedisSearchIds(entity, query, NewPager(1, 10))
	assert.Equal(t, uint64(50), total)
	assert.Equal(t, uint64(1), ids[0])
	assert.Equal(t, uint64(2), ids[1])
	assert.Equal(t, uint64(10), ids[9])
	assert.Len(t, ids, 10)
	query.FilterIntMinMax("Age", 6, 8)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 10))
	assert.Equal(t, uint64(3), total)
	assert.Len(t, ids, 3)
	assert.Equal(t, uint64(6), ids[0])
	assert.Equal(t, uint64(7), ids[1])
	assert.Equal(t, uint64(8), ids[2])

	query = &RedisSearchQuery{}
	query.Sort("ID", true)
	query.FilterInt("ID", 4, 6, 2)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 10))
	assert.Equal(t, uint64(3), total)
	assert.Len(t, ids, 3)
	assert.Equal(t, uint64(6), ids[0])
	assert.Equal(t, uint64(4), ids[1])
	assert.Equal(t, uint64(2), ids[2])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterIntGreaterEqual("Age", 20)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 10))
	assert.Equal(t, uint64(31), total)
	assert.Len(t, ids, 10)
	assert.Equal(t, uint64(20), ids[0])
	assert.Equal(t, uint64(21), ids[1])
	assert.Equal(t, uint64(29), ids[9])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterIntLessEqual("Age", 20)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 30))
	assert.Equal(t, uint64(20), total)
	assert.Len(t, ids, 20)
	assert.Equal(t, uint64(20), ids[19])
	assert.Equal(t, uint64(19), ids[18])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterIntGreater("Age", 20)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 10))
	assert.Equal(t, uint64(30), total)
	assert.Len(t, ids, 10)
	assert.Equal(t, uint64(21), ids[0])
	assert.Equal(t, uint64(22), ids[1])
	assert.Equal(t, uint64(30), ids[9])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterIntLess("Age", 20)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 30))
	assert.Equal(t, uint64(19), total)
	assert.Len(t, ids, 19)
	assert.Equal(t, uint64(19), ids[18])
	assert.Equal(t, uint64(18), ids[17])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterInt("Age", 18)
	query.FilterInt("Age", 38)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 30))
	assert.Equal(t, uint64(2), total)
	assert.Len(t, ids, 2)
	assert.Equal(t, uint64(18), ids[0])
	assert.Equal(t, uint64(38), ids[1])

	query = &RedisSearchQuery{}
	query.Sort("Balance", false)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 3))
	assert.Equal(t, uint64(50), total)
	assert.Len(t, ids, 3)
	assert.Equal(t, uint64(50), ids[0])
	assert.Equal(t, uint64(49), ids[1])
	assert.Equal(t, uint64(48), ids[2])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterFloat("Weight", 101.3)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 10))
	assert.Equal(t, uint64(1), total)
	assert.Len(t, ids, 1)
	assert.Equal(t, uint64(1), ids[0])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterFloatMinMax("Weight", 105, 116.3)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 20))
	assert.Equal(t, uint64(12), total)
	assert.Len(t, ids, 12)
	assert.Equal(t, uint64(5), ids[0])
	assert.Equal(t, uint64(16), ids[11])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterFloatGreater("Weight", 148.3)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 20))
	assert.Equal(t, uint64(2), total)
	assert.Len(t, ids, 2)
	assert.Equal(t, uint64(49), ids[0])
	assert.Equal(t, uint64(50), ids[1])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterFloatGreaterEqual("Weight", 148.3)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 20))
	assert.Equal(t, uint64(3), total)
	assert.Len(t, ids, 3)
	assert.Equal(t, uint64(48), ids[0])
	assert.Equal(t, uint64(49), ids[1])
	assert.Equal(t, uint64(50), ids[2])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterFloatLess("Weight", 103.3)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 20))
	assert.Equal(t, uint64(2), total)
	assert.Len(t, ids, 2)
	assert.Equal(t, uint64(1), ids[0])
	assert.Equal(t, uint64(2), ids[1])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterFloatLessEqual("Weight", 103.3)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 20))
	assert.Equal(t, uint64(3), total)
	assert.Len(t, ids, 3)
	assert.Equal(t, uint64(1), ids[0])
	assert.Equal(t, uint64(2), ids[1])
	assert.Equal(t, uint64(3), ids[2])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterIntGreaterEqual("AgeNullable", 0)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 10))
	assert.Equal(t, uint64(30), total)
	assert.Len(t, ids, 10)
	assert.Equal(t, uint64(21), ids[0])
	assert.Equal(t, uint64(22), ids[1])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterIntNull("AgeNullable")
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 10))
	assert.Equal(t, uint64(20), total)
	assert.Len(t, ids, 10)
	assert.Equal(t, uint64(1), ids[0])
	assert.Equal(t, uint64(2), ids[1])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterIntGreaterEqual("BalanceNullable", 0)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 10))
	assert.Equal(t, uint64(30), total)
	assert.Len(t, ids, 10)
	assert.Equal(t, uint64(21), ids[0])
	assert.Equal(t, uint64(22), ids[1])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterTag("Enum", "a", "c")
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 50))
	assert.Equal(t, uint64(30), total)
	assert.Len(t, ids, 30)
	assert.Equal(t, uint64(1), ids[0])
	assert.Equal(t, uint64(41), ids[20])
	assert.Equal(t, uint64(50), ids[29])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterNotTag("Enum", "a", "c")
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 50))
	assert.Equal(t, uint64(20), total)
	assert.Len(t, ids, 20)
	assert.Equal(t, uint64(21), ids[0])
	assert.Equal(t, uint64(40), ids[19])

	// TODO sometimes return 48
	//query = &RedisSearchQuery{}
	//query.Sort("Age", false)
	//query.FilterNotString("Name", "dog 10")
	//ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 50))
	//assert.Equal(t, uint64(49), total) // why sometimes 48
	//assert.Len(t, ids, 49)
	//assert.Equal(t, uint64(1), ids[0])
	//assert.Equal(t, uint64(50), ids[48])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterNotInt("Age", 30)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 50))
	assert.Equal(t, uint64(49), total)
	assert.Len(t, ids, 49)
	assert.Equal(t, uint64(29), ids[28])
	assert.Equal(t, uint64(31), ids[29])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterTag("EnumNullable", "", "c")
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 50))
	assert.Equal(t, uint64(30), total)
	assert.Len(t, ids, 30)
	assert.Equal(t, uint64(1), ids[0])
	assert.Equal(t, uint64(41), ids[20])
	assert.Equal(t, uint64(50), ids[29])

	total = engine.RedisSearchCount(entity, query)
	assert.Equal(t, uint64(30), total)

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.Query("dog")
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 50))
	assert.Equal(t, uint64(20), total)
	assert.Len(t, ids, 20)
	assert.Equal(t, uint64(1), ids[0])
	assert.Equal(t, uint64(20), ids[19])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.Query("dog 20")
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 50))
	assert.Equal(t, uint64(1), total)
	assert.Len(t, ids, 1)
	assert.Equal(t, uint64(20), ids[0])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.Query("cat")
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 50))
	assert.Equal(t, uint64(20), total)
	assert.Len(t, ids, 20)
	assert.Equal(t, uint64(21), ids[0])
	assert.Equal(t, uint64(40), ids[19])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.Query("orange")
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 50))
	assert.Equal(t, uint64(30), total)
	assert.Len(t, ids, 30)
	assert.Equal(t, uint64(21), ids[0])
	assert.Equal(t, uint64(50), ids[29])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterTag("Set", "b", "c")
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 50))
	assert.Equal(t, uint64(30), total)
	assert.Len(t, ids, 30)
	assert.Equal(t, uint64(21), ids[0])
	assert.Equal(t, uint64(50), ids[29])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterTag("SetNullable", "NULL", "c")
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 50))
	assert.Equal(t, uint64(30), total)
	assert.Len(t, ids, 30)
	assert.Equal(t, uint64(1), ids[0])
	assert.Equal(t, uint64(20), ids[19])
	assert.Equal(t, uint64(50), ids[29])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterBool("Bool", true)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 50))
	assert.Equal(t, uint64(10), total)
	assert.Len(t, ids, 10)
	assert.Equal(t, uint64(41), ids[0])
	assert.Equal(t, uint64(50), ids[9])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterBool("Bool", false)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 50))
	assert.Equal(t, uint64(40), total)
	assert.Len(t, ids, 40)
	assert.Equal(t, uint64(1), ids[0])
	assert.Equal(t, uint64(40), ids[39])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterTag("BoolNullable", "NULL", "true")
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 30))
	assert.Equal(t, uint64(30), total)
	assert.Len(t, ids, 30)
	assert.Equal(t, uint64(1), ids[0])
	assert.Equal(t, uint64(50), ids[29])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterFloatGreaterEqual("WeightNullable", 0)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 10))
	assert.Equal(t, uint64(30), total)
	assert.Len(t, ids, 10)
	assert.Equal(t, uint64(21), ids[0])
	assert.Equal(t, uint64(22), ids[1])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterFloatNull("WeightNullable")
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 30))
	assert.Equal(t, uint64(20), total)
	assert.Len(t, ids, 20)
	assert.Equal(t, uint64(1), ids[0])
	assert.Equal(t, uint64(20), ids[19])

	newNow := time.Now()
	newNow = newNow.Add(time.Second * 5)
	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterDate("Date", newNow)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 30))
	assert.Equal(t, uint64(20), total)
	assert.Len(t, ids, 20)
	assert.Equal(t, uint64(21), ids[0])
	assert.Equal(t, uint64(40), ids[19])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterDateGreater("Date", newNow)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 30))
	assert.Equal(t, uint64(10), total)
	assert.Len(t, ids, 10)
	assert.Equal(t, uint64(41), ids[0])
	assert.Equal(t, uint64(50), ids[9])

	newNow = now.Add(time.Microsecond * 3)
	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterDateTime("DateTime", newNow)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 30))
	assert.Equal(t, uint64(20), total)
	assert.Len(t, ids, 20)
	assert.Equal(t, uint64(21), ids[0])
	assert.Equal(t, uint64(40), ids[19])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterDateTimeGreater("DateTime", newNow)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 30))
	assert.Equal(t, uint64(10), total)
	assert.Len(t, ids, 10)
	assert.Equal(t, uint64(41), ids[0])
	assert.Equal(t, uint64(50), ids[9])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterDateNull("DateNullable")
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 30))
	assert.Equal(t, uint64(20), total)
	assert.Len(t, ids, 20)
	assert.Equal(t, uint64(1), ids[0])
	assert.Equal(t, uint64(20), ids[19])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterInt("Ref", 32)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 30))
	assert.Equal(t, uint64(3), total)
	assert.Len(t, ids, 3)
	assert.Equal(t, uint64(3), ids[0])
	assert.Equal(t, uint64(4), ids[1])
	assert.Equal(t, uint64(5), ids[2])

	entity = &redisSearchEntity{}
	engine.LoadByID(40, entity)
	engine.Delete(entity)

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 50))
	assert.Equal(t, uint64(49), total)
	assert.Len(t, ids, 49)

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.QueryRaw("(@Bool:{true})")
	query.AppendQueryRaw(" | (@Ref:[32 32])")
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 50))
	assert.Equal(t, uint64(13), total)
	assert.Len(t, ids, 13)

	entity = &redisSearchEntity{}
	engine.LoadByID(1, entity)
	entity.Age = 100
	flusher = engine.NewFlusher()
	flusher.Track(entity)
	flusher.FlushInTransaction()

	query = &RedisSearchQuery{}
	query.Sort("Age", false).FilterInt("Age", 100)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 50))
	assert.Equal(t, uint64(1), total)
	assert.Len(t, ids, 1)
	assert.Equal(t, uint64(1), ids[0])

	entity.Age = 101
	engine.FlushLazy(entity)
	receiver := NewBackgroundConsumer(engine)
	receiver.DisableLoop()
	receiver.blockTime = time.Millisecond
	receiver.Digest()

	query = &RedisSearchQuery{}
	query.Sort("Age", false).FilterInt("Age", 101)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 50))
	assert.Equal(t, uint64(1), total)
	assert.Len(t, ids, 1)
	assert.Equal(t, uint64(1), ids[0])

	engine.GetRedis("search").FlushDB()
	for _, alter := range engine.GetRedisSearchIndexAlters() {
		alter.Execute()
	}
	indexer.Digest()
	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 10))
	assert.Equal(t, uint64(49), total)
	assert.Len(t, ids, 10)

	entities := make([]*redisSearchEntity, 0)
	total = engine.RedisSearch(&entities, query, NewPager(1, 10))
	assert.Equal(t, uint64(49), total)
	assert.Len(t, entities, 10)
	assert.Equal(t, "dog 2", entities[0].Name)
	assert.Equal(t, "dog 11", entities[9].Name)
	assert.False(t, entities[0].IsLazy())

	entities = make([]*redisSearchEntity, 0)
	total = engine.RedisSearchLazy(&entities, query, NewPager(1, 10))
	assert.Equal(t, uint64(49), total)
	assert.Len(t, entities, 10)
	assert.Equal(t, "dog 2", entities[0].GetFieldLazy(engine, "Name"))
	assert.Equal(t, "dog 11", entities[9].GetFieldLazy(engine, "Name"))
	assert.True(t, entities[0].IsLazy())

	query.FilterInt("Age", 10)
	assert.True(t, engine.RedisSearchOne(entity, query))
	assert.Equal(t, "dog 10", entity.Name)

	query.FilterInt("Age", 10)
	assert.True(t, engine.RedisSearchOneLazy(entity, query))
	assert.Equal(t, "dog 10", entity.GetFieldLazy(engine, "Name"))
	assert.True(t, entity.IsLazy())

	query.FilterInt("Balance", 700)
	assert.False(t, engine.RedisSearchOne(entity, query))

	engine.LoadByID(40, entity)
	engine.ForceDelete(entity)
	query = NewRedisSearchQuery()
	total = engine.RedisSearchCount(entity, query)
	assert.Equal(t, uint64(49), total)

	entity = &redisSearchEntity{}
	engine.LoadByID(1, entity)
	entity.Age = 120
	entity.Name = ""
	engine.Flush(entity)
	query = &RedisSearchQuery{}
	query.FilterString("Name", "")
	assert.True(t, engine.RedisSearchOne(entity, query))
	assert.PanicsWithError(t, "unknown field Name2", func() {
		query = &RedisSearchQuery{}
		query.FilterString("Name2", "")
		engine.RedisSearchOne(entity, query)
	})
	assert.PanicsWithError(t, "missing `searchable` tag for field Another", func() {
		query = &RedisSearchQuery{}
		query.FilterString("Another", "")
		engine.RedisSearchOne(entity, query)
	})
	assert.PanicsWithError(t, "string filter on fields Weight with type NUMERIC not allowed", func() {
		query = &RedisSearchQuery{}
		query.FilterString("Weight", "")
		engine.RedisSearchOne(entity, query)
	})
	assert.PanicsWithError(t, "unknown field Name2", func() {
		query = &RedisSearchQuery{}
		query.FilterInt("Name2", 23)
		engine.RedisSearchOne(entity, query)
	})
	assert.PanicsWithError(t, "missing `searchable` tag for field AnotherNumeric", func() {
		query = &RedisSearchQuery{}
		query.FilterInt("AnotherNumeric", 23)
		engine.RedisSearchOne(entity, query)
	})
	assert.PanicsWithError(t, "numeric filter on fields Name with type TEXT not allowed", func() {
		query = &RedisSearchQuery{}
		query.FilterInt("Name", 23)
		engine.RedisSearchOne(entity, query)
	})

	assert.PanicsWithError(t, "unknown field Name2", func() {
		query = &RedisSearchQuery{}
		query.FilterTag("Name2", "test")
		engine.RedisSearchOne(entity, query)
	})
	assert.PanicsWithError(t, "missing `searchable` tag for field AnotherTag", func() {
		query = &RedisSearchQuery{}
		query.FilterTag("AnotherTag", "test")
		engine.RedisSearchOne(entity, query)
	})
	assert.PanicsWithError(t, "tag filter on fields Name with type TEXT not allowed", func() {
		query = &RedisSearchQuery{}
		query.FilterTag("Name", "test")
		engine.RedisSearchOne(entity, query)
	})

	assert.PanicsWithError(t, "integer too high for redis search sort field", func() {
		entity = &redisSearchEntity{}
		engine.LoadByID(9, entity)
		entity.Balance = math.MaxInt64
		engine.Flush(entity)
	})

	engine.Flush(&redisSearchEntity{Age: 133})
	schema := engine.GetRegistry().GetTableSchemaForEntity(entity)
	schema.ReindexRedisSearchIndex(engine)
	indexer.Digest()
	query = NewRedisSearchQuery()
	query.FilterInt("Age", 133)
	found := engine.RedisSearchOne(entity, query)
	assert.True(t, found)

	entitySearch, has := schema.GetRedisSearch(engine)
	assert.True(t, has)
	assert.Equal(t, "search", entitySearch.GetPoolConfig().GetCode())

	type redisSearchEntity2 struct {
		ORM `orm:"redisSearch=invalid"`
		ID  uint `orm:"searchable;sortable"`
	}
	registry = NewRegistry()
	registry.RegisterEntity(&redisSearchEntity2{})
	_, err := registry.Validate(context.Background())
	assert.EqualError(t, err, "mysql pool 'default' not found")

	assert.PanicsWithError(t, "integer too high for redis search sort field", func() {
		engine.Flush(&redisSearchEntity{Age: math.MaxInt32 + 1})
	})
	assert.PanicsWithError(t, "integer too high for redis search sort field", func() {
		v := uint64(math.MaxInt32 + 1)
		engine.Flush(&redisSearchEntity{AgeNullable: &v})
	})
	assert.PanicsWithError(t, "integer too high for redis search sort field", func() {
		v := int64(math.MaxInt32 + 1)
		engine.Flush(&redisSearchEntity{BalanceNullable: &v})
	})
}
