package beeorm

import (
	"encoding/json"
	"fmt"
	"os"
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

var TestSet = struct {
	D string
	E string
	F string
}{
	D: "d",
	E: "e",
	F: "f",
}

type benchmarkIsDirtyEntity struct {
	ORM
	ID                                          uint32       `orm:"unique=ID"`
	Token                                       string       `orm:"unique=Token"`
	Name                                        string       `orm:"length=60;index=unique_name"`
	CreatedAt                                   time.Time    `orm:"time=true;index=index_created_at"`
	LastActiveAt                                time.Time    `orm:"time=true;index=index_last_active_at"`
	Guild                                       *flushEntity ``
	GuildRank                                   string       `orm:"enum=beeorm.TestSet"`
	Lang                                        string       `orm:"length=2"`
	CountryCode                                 string       `orm:"length=8"`
	Level                                       uint8        `orm:"index=index_level"`
	Exp                                         uint32       ``
	Gold                                        uint64       `orm:"index=index_gold"`
	Premium                                     uint64       `orm:"index=index_premium"`
	Energy                                      uint16       ``
	Heropower                                   uint32       `orm:"index=index_heropower"`
	HeropowerMax                                uint32       ``
	IsMale                                      bool         ``
	PremiumPurchased                            uint32       `orm:"index=index_premium_purchased"`
	LastPurchaseAt                              *time.Time   `orm:"time=true"`
	MaxInventorySize                            uint16       ``
	NextDailybonus                              *time.Time   `orm:"time=true"`
	DailybonusTaken                             uint16       ``
	NextDailyquestsReset                        *time.Time   `orm:"time=true"`
	DailytasksPoints                            uint16       ``
	DailytasksRewardTaken                       uint16       ``
	BankGoldTime                                *time.Time   `orm:"time=true"`
	BankWatchadsUsed                            uint8        ``
	SexChanges                                  uint8        ``
	NickChanges                                 uint8        ``
	FlashNotPayLongDays                         uint8        ``
	SpinsPremiumBuyDone                         uint8        ``
	FreeNormalSpins                             uint8        ``
	SpinWatchadsUsed                            uint8        ``
	EventSpinDone1                              uint16       ``
	EventSpinDone2                              uint16       ``
	EventSpinDone3                              uint16       ``
	EventSpinDone4                              uint16       ``
	EventSpinDone5                              uint16       ``
	EventSpinDone6                              uint16       ``
	SpinEventSlotMul1                           uint8        ``
	SpinEventSlotMul2                           uint8        ``
	SpinEventSlotMul3                           uint8        ``
	SpinEventSlotMul4                           uint8        ``
	SpinEventSlotMul5                           uint8        ``
	SpinEventSlotMul6                           uint8        ``
	ShopReloadTime                              *time.Time   `orm:"time=true"`
	ShopReloadsDone                             uint8        ``
	ShopHasEventContent                         bool         ``
	ShopItem1ID                                 *flushEntity `orm:"skip_FK"`
	ShopItem2ID                                 *flushEntity `orm:"skip_FK"`
	ShopItem3ID                                 *flushEntity `orm:"skip_FK"`
	ShopItem4ID                                 *flushEntity `orm:"skip_FK"`
	ShopItem5ID                                 *flushEntity `orm:"skip_FK"`
	ShopItem6ID                                 *flushEntity `orm:"skip_FK"`
	ShopItem7ID                                 *flushEntity `orm:"skip_FK"`
	ShopItem8ID                                 *flushEntity `orm:"skip_FK"`
	ShopItem1Amount                             uint8        ``
	ShopItem2Amount                             uint8        ``
	ShopItem3Amount                             uint8        ``
	ShopItem4Amount                             uint8        ``
	ShopItem5Amount                             uint8        ``
	ShopItem6Amount                             uint8        ``
	ShopItem7Amount                             uint8        ``
	ShopItem8Amount                             uint8        ``
	BodyHairID                                  *flushEntity `orm:"skip_FK"`
	BodyEyesID                                  *flushEntity `orm:"skip_FK"`
	BodyEyebrowsID                              *flushEntity `orm:"skip_FK"`
	BodyMouthID                                 *flushEntity `orm:"skip_FK"`
	BodyFacespecialID                           *flushEntity `orm:"skip_FK"`
	BodyFacespecial2ID                          *flushEntity `orm:"skip_FK"`
	BodyFacialhairID                            *flushEntity `orm:"skip_FK"`
	BodyEarsID                                  *flushEntity `orm:"skip_FK"`
	BodyNoseID                                  *flushEntity `orm:"skip_FK"`
	BodySkinColor                               uint32       ``
	BodyHairColor                               uint32       ``
	NextChestBronze                             *time.Time   `orm:"time=true"`
	BronzeChestLimit                            uint8        ``
	NextChestSilver                             *time.Time   `orm:"time=true"`
	NextChestGold                               *time.Time   `orm:"time=true"`
	NextChestGoldLegendary                      uint8        ``
	NextChestEvent1                             *time.Time   `orm:"time=true"`
	NextChestEvent2                             *time.Time   `orm:"time=true"`
	NextChestEvent3                             *time.Time   `orm:"time=true"`
	NextChestEvent4                             *time.Time   `orm:"time=true"`
	NextChestEvent5                             *time.Time   `orm:"time=true"`
	NextChestEvent6                             *time.Time   `orm:"time=true"`
	NextChestEvent7                             *time.Time   `orm:"time=true"`
	NextChestEvent8                             *time.Time   `orm:"time=true"`
	NextChestEvent9                             *time.Time   `orm:"time=true"`
	NextChestEvent10                            *time.Time   `orm:"time=true"`
	NextChestEvent11                            *time.Time   `orm:"time=true"`
	NextChestEvent12                            *time.Time   `orm:"time=true"`
	NextChestEvent13                            *time.Time   `orm:"time=true"`
	NextChestEvent14                            *time.Time   `orm:"time=true"`
	NextChestEvent15                            *time.Time   `orm:"time=true"`
	NextChestEventLegendary1                    uint8        ``
	NextChestEventLegendary2                    uint8        ``
	NextChestEventLegendary3                    uint8        ``
	NextChestEventLegendary4                    uint8        ``
	NextChestEventLegendary5                    uint8        ``
	NextChestEventLegendary6                    uint8        ``
	NextChestEventLegendary7                    uint8        ``
	NextChestEventLegendary8                    uint8        ``
	NextChestEventLegendary9                    uint8        ``
	NextChestEventLegendary10                   uint8        ``
	NextChestEventLegendary11                   uint8        ``
	NextChestEventLegendary12                   uint8        ``
	NextChestEventLegendary13                   uint8        ``
	NextChestEventLegendary14                   uint8        ``
	NextChestEventLegendary15                   uint8        ``
	JourneyJourney1ID                           *flushEntity `orm:"skip_FK"`
	JourneyJourney2ID                           *flushEntity `orm:"skip_FK"`
	JourneyJourney3ID                           *flushEntity `orm:"skip_FK"`
	JourneyJourney4ID                           *flushEntity `orm:"skip_FK"`
	JourneyJourney5ID                           *flushEntity `orm:"skip_FK"`
	JourneyNameSeed                             uint8
	JourneyCurrentID                            *flushEntity   `orm:"skip_FK"`
	JourneyEnd                                  *time.Time     `orm:"time=true"`
	JourneyReloadTime                           *time.Time     `orm:"time=true"`
	JourneyReloadsDone                          uint16         ``
	JourneyWatchadsUsed                         uint8          ``
	EnergyRenewUsed                             uint16         ``
	EnergyBuyUsed                               uint8          ``
	EnergyWatchadsUsed                          uint8          ``
	EnergyLastCalculated                        time.Time      `orm:"time=true"`
	LastExtractRecipeID                         uint32         ``
	Collection1UnlockedSlots                    uint8          ``
	Collection2UnlockedSlots                    uint8          ``
	Collection1Slot1Upg                         uint32         ``
	Collection1Slot2Upg                         uint32         ``
	Collection1Slot3Upg                         uint32         ``
	Collection1Slot4Upg                         uint32         ``
	Collection1Slot5Upg                         uint32         ``
	Collection1Slot6Upg                         uint32         ``
	Collection1Slot7Upg                         uint32         ``
	Collection1Slot8Upg                         uint32         ``
	Collection1Slot9Upg                         uint32         ``
	Collection1Slot10Upg                        uint32         ``
	Collection2Slot1Upg                         uint32         ``
	Collection2Slot2Upg                         uint32         ``
	Collection2Slot3Upg                         uint32         ``
	Collection2Slot4Upg                         uint32         ``
	Collection2Slot5Upg                         uint32         ``
	Collection2Slot6Upg                         uint32         ``
	Collection2Slot7Upg                         uint32         ``
	Collection2Slot8Upg                         uint32         ``
	Collection2Slot9Upg                         uint32         ``
	Collection2Slot10Upg                        uint32         ``
	NextStageAutobattle                         *time.Time     `orm:"time=true"`
	FirstChestFree                              uint16         ``
	FirstChestPaid                              uint16         ``
	NoobEventProgress                           uint8          ``
	NoobRewardsTaken                            uint8          ``
	UnreadMessages                              uint16         ``
	MessagesIDAutoincrement                     uint32         ``
	GlobalMessageIDx                            uint32         ``
	LastQuestsFullcheckTs                       uint32         ``
	TavernEnergyFlags                           uint8          ``
	LastTavernEnergyUse                         *time.Time     `orm:"time=true"`
	TutorialStep                                uint32         ``
	UtcOffset                                   int32          ``
	CacheDamageTotal                            uint32         ``
	CacheDefenceTotal                           uint32         ``
	CacheHpTotal                                uint32         ``
	CacheMagicTotal                             uint32         ``
	CacheCritvalTotal                           float64        ``
	CacheCritchanceTotal                        uint16         ``
	CacheDodgeTotal                             uint16         ``
	SecretCode                                  string         `orm:"length=9"`
	SecretCodesByothers                         uint32         `orm:"index=index_secret_codes_byothers"`
	SecretCodesByme                             uint32         ``
	SecretCodesByothersRewardsTaken             uint32         ``
	SecretCodesBymeRewardsTaken                 uint32         ``
	WatchAdsViewed                              uint8          ``
	ReforgeSeed                                 uint32         ``
	OfferBaseNo                                 uint8          ``
	OfferCycleNo                                uint8          ``
	OfferCycleExpire                            *time.Time     `orm:"time=true"`
	OfferProduct1ID                             *flushEntity   `orm:"skip_FK"`
	OfferProduct2ID                             *flushEntity   `orm:"skip_FK"`
	OfferProduct3ID                             *flushEntity   `orm:"skip_FK"`
	OfferProduct4ID                             *flushEntity   `orm:"skip_FK"`
	OfferProduct5ID                             *flushEntity   `orm:"skip_FK"`
	OfferProduct6ID                             *flushEntity   `orm:"skip_FK"`
	OfferProduct7ID                             *flushEntity   `orm:"skip_FK"`
	OfferProduct8ID                             *flushEntity   `orm:"skip_FK"`
	OfferProduct1Expire                         *time.Time     `orm:"time=true"`
	OfferProduct2Expire                         *time.Time     `orm:"time=true"`
	OfferProduct3Expire                         *time.Time     `orm:"time=true"`
	OfferProduct4Expire                         *time.Time     `orm:"time=true"`
	OfferProduct5Expire                         *time.Time     `orm:"time=true"`
	OfferProduct6Expire                         *time.Time     `orm:"time=true"`
	OfferProduct7Expire                         *time.Time     `orm:"time=true"`
	OfferProduct8Expire                         *time.Time     `orm:"time=true"`
	OfferBase2BuyDailyLimit                     uint8          ``
	ChBossID                                    uint32         ``
	ChBossEnergy                                uint32         ``
	ChBossMaxTurnsToday                         uint32         ``
	ChBossInflictedDamageMax                    uint32         `orm:"index=index_ch_boss_inflicted_damage_max"`
	ChBossInflictedDamageToday                  uint32         ``
	ChBossLeagueIndex                           uint32         ``
	CurrentEventID                              uint32         ``
	BattleEventID                               uint32         ``
	Options                                     uint32         ``
	ItemIndexPoints                             uint32         ``
	GuildJoinedAt                               *time.Time     `orm:"time=true"`
	GuildTreasureDonateBits                     uint8          ``
	GuildFamePoints                             uint32         ``
	GuildFameLevel                              uint8          ``
	GuildStatsWeekNo                            uint8          ``
	GuildShortName                              string         `orm:"length=9"`
	BattleEventMilestoneTaken                   uint64         ``
	BattleEventGuildMilestoneTaken              uint64         ``
	Ars1Expire                                  *time.Time     `orm:"time=true"`
	Ars1NextBonus                               *time.Time     `orm:"time=true"`
	ArsPlatform                                 uint8          ``
	GuildshopDailyCount                         uint8          ``
	AutoBattleDailyUsed                         uint32         ``
	BattleEventBossWarriorTicketsLastCalculated *time.Time     `orm:"time=true"`
	BattleEventBossMageTicketsLastCalculated    *time.Time     `orm:"time=true"`
	BattleEventBossGuildTicketsLastCalculated   *time.Time     `orm:"time=true"`
	BattleEventBossMultiTicketsLastCalculated   *time.Time     `orm:"time=true"`
	LastIDleClaim                               *time.Time     `orm:"time=true"`
	VipNewRewardsSent                           bool           ``
	FlagPack                                    uint32         ``
	ItemExtractRenewTime                        *time.Time     `orm:"time=true"`
	TowerCurrentStage                           uint32         ``
	Banned                                      bool           ``
	BoughtsCount                                uint32         ``
	BoughtsSum                                  float64        ``
	LastGuildCreation                           *time.Time     `orm:"time=true"`
	SessionStart                                time.Time      `orm:"time=true"`
	CachedIndexID                               *CachedQuery   `queryOne:":ID= ?" json:"-"`
	CachedIndexToken                            *CachedQuery   `queryOne:":Token = ?" json:"-"`
	ABTestsIDsCache                             []uint16       `orm:"ignore"`
	CacheOnly                                   flushSubStruct `orm:"ignore"`

	AdminLabel string `orm:"ignore"`
	AdminIcon  string `orm:"ignore"`
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
	ORM                  `orm:"localCache;redisCache"`
	ID                   uint
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
	SetNullable          []string `orm:"set=beeorm.TestSet"`
	SetNotNull           []string `orm:"set=beeorm.TestSet;required"`
	EnumNullable         string   `orm:"enum=beeorm.TestEnum"`
	EnumNotNull          string   `orm:"enum=beeorm.TestEnum;required"`
	Ignored              []string `orm:"ignore"`
	Blob                 []uint8
	Bool                 bool
	FakeDelete           bool
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
	engine := prepareTables(t, registry, 5, 6, "", entity, reference)

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
	assert.True(t, entity.IsDirty())
	assert.True(t, entity.ReferenceOne.IsDirty())
	flusher := engine.NewFlusher().Track(entity)
	flusher.Track(entity)
	flusher.Flush()
	bind, isDirty := entity.GetDirtyBind()
	assert.False(t, isDirty)
	assert.Nil(t, bind)
	flusher.Flush()

	assert.True(t, entity.IsLoaded())
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.False(t, entity.IsDirty())
	assert.False(t, entity.ReferenceOne.IsDirty())
	assert.Equal(t, uint(1), entity.ID)
	assert.Equal(t, uint(1), entity.ReferenceOne.ID)
	assert.True(t, entity.IsLoaded())
	assert.True(t, entity.ReferenceOne.IsLoaded())
	refOneID := entity.ReferenceOne.ID

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
	assert.Equal(t, date.Format(timeFormat), entity.TimeWithTime.Format(timeFormat))
	assert.Equal(t, date.Unix(), entity.TimeWithTime.Unix())
	assert.Equal(t, date.Format(timeFormat), entity.TimeWithTimeNullable.Format(timeFormat))
	assert.Equal(t, date.Unix(), entity.TimeWithTimeNullable.Unix())
	assert.Nil(t, entity.SetNullable)
	assert.Equal(t, "", entity.City)
	assert.NotNil(t, entity.FlushStructPtr)
	assert.Equal(t, "A", entity.FlushStructPtr.Name2)
	assert.Equal(t, 12, entity.FlushStructPtr.Age)
	assert.Equal(t, "G", entity.FlushStructPtr.Sub.Name3)
	assert.Equal(t, 11, entity.FlushStructPtr.Sub.Age3)
	assert.Equal(t, date.Format(timeFormat), entity.FlushStruct.TestTime.Format(timeFormat))
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
	assert.Equal(t, "Ita", entity.FlushStruct.Name2)
	assert.Equal(t, 13, entity.FlushStruct.Sub.Age3)
	assert.Equal(t, "Nanami", entity.FlushStruct.Sub.Name3)

	entity.FlushStructPtr = nil
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(1, entity)
	assert.Nil(t, entity.FlushStructPtr)

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
	assert.False(t, entity.IsDirty())
	assert.False(t, reference.IsDirty())
	assert.True(t, reference.IsLoaded())

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
	entity2.SetOnDuplicateKeyUpdate(Bind{"Age": "40", "Year": "2020", "City": "Moscow", "UintNullable": "NULL",
		"BoolNullable": "NULL", "TimeWithTime": date.Format(timeFormat), "Time": date.Format(dateformat)})
	fmt.Printf("START\n")
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
	assert.Equal(t, date.Unix(), entity.TimeWithTime.Unix())
	assert.Equal(t, entity.Time.Format(dateformat), date.Format(dateformat))

	entity2 = &flushEntity{Name: "Tom", Age: 12, EnumNotNull: "a"}
	entity2.SetOnDuplicateKeyUpdate(Bind{})
	engine.Flush(entity2)
	assert.Equal(t, uint(1), entity2.ID)
	entity = &flushEntity{}
	engine.LoadByID(1, entity)
	assert.Equal(t, uint(1), entity.ID)
	os.Exit(0)

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
	assert.Equal(t, date.Format(timeFormat), entity.TimeWithTime.Format(timeFormat))
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
	entity2.SetOnDuplicateKeyUpdate(Bind{"Age": "2"})
	engine.Flush(entity2)
	assert.Equal(t, uint(12), entity2.ID)
	assert.Equal(t, 2, entity2.Age)
	entity2 = &flushEntity{}
	found = engine.LoadByID(100, entity2)
	assert.False(t, found)
	entity2 = &flushEntity{Name: "Frank", ID: 100, Age: 1, EnumNotNull: "a"}
	entity2.SetOnDuplicateKeyUpdate(Bind{"Age": "2"})
	engine.Flush(entity2)
	found = engine.LoadByID(100, entity2)
	assert.True(t, found)
	assert.Equal(t, 1, entity2.Age)

	entity2 = &flushEntity{ID: 100, Age: 1, EnumNotNull: "a"}
	entity2.SetOnDuplicateKeyUpdate(Bind{"Age": "2"})
	engine.Flush(entity2)
	assert.Equal(t, uint(100), entity2.ID)
	assert.Equal(t, 2, entity2.Age)
	entity2 = &flushEntity{}
	found = engine.LoadByID(100, entity2)
	assert.True(t, found)
	assert.Equal(t, 2, entity2.Age)

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

	runLazyFlushConsumer(engine)
	if local {
		assert.Len(t, testLogger.Logs, 3)
		assert.Equal(t, "START TRANSACTION", testLogger.Logs[0]["query"])
		assert.Equal(t, "UPDATE `flushEntity` SET `Age`=99 WHERE `ID` = 10;UPDATE `flushEntity` SET `Uint`=99 "+
			"WHERE `ID` = 11;UPDATE `flushEntity` SET `Name`='sss' WHERE `ID` = 12;", testLogger.Logs[1]["query"])
		assert.Equal(t, "COMMIT", testLogger.Logs[2]["query"])
	}

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
		assert.Equal(t, "DEL", testLogger2.Logs[4]["operation"])
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
	engine.LoadByID(101, entity)
	engine.Delete(entity)
	entity = &flushEntity{}
	engine.GetLocalCache().Clear()
	engine.GetRedis().FlushDB()
	assert.True(t, engine.LoadByID(101, entity))
	assert.True(t, entity.FakeDelete)
	assert.False(t, entity.IsDirty())
	engine.ForceDelete(entity)
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

	testLogger.clear()
	flusher = engine.NewFlusher()
	flusher.Track(&flushEntityReference{})
	flusher.Track(&flushEntity{Name: "Adam"})
	err = flusher.FlushWithCheck()
	assert.NotNil(t, err)
	assert.Equal(t, "ROLLBACK", testLogger.Logs[len(testLogger.Logs)-1]["query"])

	entity = schema.NewEntity().(*flushEntity)
	entity.Name = "WithID"
	err = entity.SetField("ID", 676)
	assert.NoError(t, err)
	engine.Flush(entity)
	entity = &flushEntity{}
	assert.True(t, engine.LoadByID(676, entity))

	entity = &flushEntity{}
	engine.LoadByID(13, entity)
	entity.City = "Warsaw"
	entity.SubName = "testSub"
	engine.Flush(entity)
	clonedEntity := entity.Clone().(*flushEntity)
	assert.Equal(t, uint(0), clonedEntity.ID)
	assert.False(t, clonedEntity.IsLoaded())
	assert.True(t, clonedEntity.IsDirty())
	assert.Equal(t, "Warsaw", clonedEntity.City)
	assert.Equal(t, "1335", clonedEntity.Name)
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

// 17 allocs/op - 6 for Exec
func BenchmarkFlusherUpdateNoCache(b *testing.B) {
	benchmarkFlusher(b, false, false)
}

func BenchmarkIsDirty(b *testing.B) {
	var entity *benchmarkIsDirtyEntity
	registry := &Registry{}
	registry.RegisterEnum("beeorm.TestEnum", []string{"a", "b", "c"})
	registry.RegisterEnum("beeorm.TestSet", []string{"a", "b", "c"})
	registry.RegisterRedisStream("entity_changed", "default", []string{"test-group-1"})
	engine := prepareTables(nil, registry, 5, 6, "", entity, &flushEntity{}, &flushEntityReference{})
	entity = &benchmarkIsDirtyEntity{}

	t := time.Now()
	f := &flushEntity{Name: "a"}
	engine.Flush(f)

	entity.Token = "abcd"
	entity.Name = "test"
	entity.CreatedAt = t
	entity.LastActiveAt = t
	entity.Guild = f
	entity.GuildRank = "a"
	entity.Lang = "en"
	entity.CountryCode = "pl"
	entity.Level = 3
	entity.Exp = 4
	entity.Gold = 5
	entity.Premium = 6
	entity.Energy = 7
	entity.Heropower = 8
	entity.HeropowerMax = 9
	entity.IsMale = true
	entity.PremiumPurchased = 14
	entity.LastPurchaseAt = &t
	entity.MaxInventorySize = 12
	entity.NextDailybonus = &t
	entity.DailybonusTaken = 1
	entity.NextDailyquestsReset = &t
	entity.DailytasksPoints = 12
	entity.DailytasksRewardTaken = 13
	entity.BankGoldTime = &t
	entity.BankWatchadsUsed = 7
	entity.SexChanges = 7
	entity.NickChanges = 7
	entity.FlashNotPayLongDays = 7
	entity.SpinsPremiumBuyDone = 7
	entity.FreeNormalSpins = 7
	entity.SpinWatchadsUsed = 7
	entity.EventSpinDone1 = 7
	entity.EventSpinDone2 = 7
	entity.EventSpinDone3 = 7
	entity.EventSpinDone4 = 7
	entity.EventSpinDone5 = 7
	entity.EventSpinDone6 = 7
	entity.SpinEventSlotMul1 = 7
	entity.SpinEventSlotMul2 = 7
	entity.SpinEventSlotMul3 = 7
	entity.SpinEventSlotMul4 = 7
	entity.SpinEventSlotMul5 = 7
	entity.SpinEventSlotMul6 = 7
	entity.ShopReloadTime = &t
	entity.ShopReloadsDone = 7
	entity.ShopHasEventContent = true
	entity.ShopItem1ID = f
	entity.ShopItem2ID = f
	entity.ShopItem3ID = f
	entity.ShopItem4ID = f
	entity.ShopItem5ID = f
	entity.ShopItem6ID = f
	entity.ShopItem7ID = f
	entity.ShopItem8ID = f
	entity.ShopItem1Amount = 7
	entity.ShopItem2Amount = 7
	entity.ShopItem3Amount = 7
	entity.ShopItem4Amount = 7
	entity.ShopItem5Amount = 7
	entity.ShopItem6Amount = 7
	entity.ShopItem7Amount = 7
	entity.ShopItem8Amount = 7
	entity.BodyHairID = f
	entity.BodyEyesID = f
	entity.BodyEyebrowsID = f
	entity.BodyMouthID = f
	entity.BodyFacespecialID = f
	entity.BodyFacespecial2ID = f
	entity.BodyFacialhairID = f
	entity.BodyEarsID = f
	entity.BodyNoseID = f
	entity.BodySkinColor = 7
	entity.BodyHairColor = 7
	entity.NextChestBronze = &t
	entity.BronzeChestLimit = 7
	entity.NextChestSilver = &t
	entity.NextChestGold = &t
	entity.NextChestGoldLegendary = 7
	entity.NextChestEvent1 = &t
	entity.NextChestEvent2 = &t
	entity.NextChestEvent3 = &t
	entity.NextChestEvent4 = &t
	entity.NextChestEvent5 = &t
	entity.NextChestEvent6 = &t
	entity.NextChestEvent7 = &t
	entity.NextChestEvent8 = &t
	entity.NextChestEvent9 = &t
	entity.NextChestEvent10 = &t
	entity.NextChestEvent11 = &t
	entity.NextChestEvent12 = &t
	entity.NextChestEvent13 = &t
	entity.NextChestEvent14 = &t
	entity.NextChestEvent15 = &t
	entity.NextChestEventLegendary1 = 7
	entity.NextChestEventLegendary2 = 7
	entity.NextChestEventLegendary3 = 7
	entity.NextChestEventLegendary4 = 7
	entity.NextChestEventLegendary5 = 7
	entity.NextChestEventLegendary6 = 7
	entity.NextChestEventLegendary7 = 7
	entity.NextChestEventLegendary8 = 7
	entity.NextChestEventLegendary9 = 7
	entity.NextChestEventLegendary10 = 7
	entity.NextChestEventLegendary11 = 7
	entity.NextChestEventLegendary12 = 7
	entity.NextChestEventLegendary13 = 7
	entity.NextChestEventLegendary14 = 7
	entity.NextChestEventLegendary15 = 7
	entity.JourneyJourney1ID = f
	entity.JourneyJourney2ID = f
	entity.JourneyJourney3ID = f
	entity.JourneyJourney4ID = f
	entity.JourneyJourney5ID = f
	entity.JourneyNameSeed = 7
	entity.JourneyCurrentID = f
	entity.JourneyEnd = &t
	entity.JourneyReloadTime = &t
	entity.JourneyReloadsDone = 7
	entity.JourneyWatchadsUsed = 7
	entity.EnergyRenewUsed = 7
	entity.EnergyBuyUsed = 7
	entity.EnergyWatchadsUsed = 7
	entity.EnergyLastCalculated = t
	entity.LastExtractRecipeID = 7
	entity.Collection1UnlockedSlots = 7
	entity.Collection2UnlockedSlots = 7
	entity.Collection1Slot1Upg = 7
	entity.Collection1Slot2Upg = 7
	entity.Collection1Slot3Upg = 7
	entity.Collection1Slot4Upg = 7
	entity.Collection1Slot5Upg = 7
	entity.Collection1Slot6Upg = 7
	entity.Collection1Slot7Upg = 7
	entity.Collection1Slot8Upg = 7
	entity.Collection1Slot9Upg = 7
	entity.Collection1Slot10Upg = 7
	entity.Collection2Slot1Upg = 7
	entity.Collection2Slot2Upg = 7
	entity.Collection2Slot3Upg = 7
	entity.Collection2Slot4Upg = 7
	entity.Collection2Slot5Upg = 7
	entity.Collection2Slot6Upg = 7
	entity.Collection2Slot7Upg = 7
	entity.Collection2Slot8Upg = 7
	entity.Collection2Slot9Upg = 7
	entity.Collection2Slot10Upg = 7
	entity.NextStageAutobattle = &t
	entity.FirstChestFree = 7
	entity.FirstChestPaid = 7
	entity.NoobEventProgress = 7
	entity.NoobRewardsTaken = 7
	entity.UnreadMessages = 7
	entity.MessagesIDAutoincrement = 7
	entity.GlobalMessageIDx = 7
	entity.LastQuestsFullcheckTs = 7
	entity.TavernEnergyFlags = 7
	entity.LastTavernEnergyUse = &t
	entity.TutorialStep = 7
	entity.UtcOffset = 7
	entity.CacheDamageTotal = 7
	entity.CacheDefenceTotal = 7
	entity.CacheHpTotal = 7
	entity.CacheMagicTotal = 7
	entity.CacheCritvalTotal = 14.3
	entity.CacheCritchanceTotal = 7
	entity.CacheDodgeTotal = 7
	entity.SecretCode = "test"
	entity.SecretCodesByothers = 7
	entity.SecretCodesByme = 7
	entity.SecretCodesByothersRewardsTaken = 7
	entity.SecretCodesBymeRewardsTaken = 7
	entity.WatchAdsViewed = 7
	entity.ReforgeSeed = 7
	entity.OfferBaseNo = 7
	entity.OfferCycleNo = 7
	entity.OfferCycleExpire = &t
	entity.OfferProduct1ID = f
	entity.OfferProduct2ID = f
	entity.OfferProduct3ID = f
	entity.OfferProduct4ID = f
	entity.OfferProduct5ID = f
	entity.OfferProduct6ID = f
	entity.OfferProduct7ID = f
	entity.OfferProduct8ID = f
	entity.OfferProduct1Expire = &t
	entity.OfferProduct2Expire = &t
	entity.OfferProduct3Expire = &t
	entity.OfferProduct4Expire = &t
	entity.OfferProduct5Expire = &t
	entity.OfferProduct6Expire = &t
	entity.OfferProduct7Expire = &t
	entity.OfferProduct8Expire = &t
	entity.OfferBase2BuyDailyLimit = 7
	entity.ChBossID = 7
	entity.ChBossEnergy = 7
	entity.ChBossMaxTurnsToday = 7
	entity.ChBossInflictedDamageMax = 7
	entity.ChBossInflictedDamageToday = 7
	entity.ChBossLeagueIndex = 7
	entity.CurrentEventID = 7
	entity.BattleEventID = 7
	entity.Options = 7
	entity.ItemIndexPoints = 7
	entity.GuildJoinedAt = &t
	entity.GuildTreasureDonateBits = 7
	entity.GuildFamePoints = 7
	entity.GuildFameLevel = 7
	entity.GuildStatsWeekNo = 7
	entity.GuildShortName = "test"
	entity.BattleEventMilestoneTaken = 7
	entity.BattleEventGuildMilestoneTaken = 7
	entity.Ars1Expire = &t
	entity.Ars1NextBonus = &t
	entity.ArsPlatform = 7
	entity.GuildshopDailyCount = 7
	entity.AutoBattleDailyUsed = 7
	entity.BattleEventBossWarriorTicketsLastCalculated = &t
	entity.BattleEventBossMageTicketsLastCalculated = &t
	entity.BattleEventBossGuildTicketsLastCalculated = &t
	entity.BattleEventBossMultiTicketsLastCalculated = &t
	entity.LastIDleClaim = &t
	entity.VipNewRewardsSent = false
	entity.FlagPack = 7
	entity.ItemExtractRenewTime = &t
	entity.TowerCurrentStage = 7
	entity.Banned = true
	entity.BoughtsCount = 7
	entity.BoughtsSum = 17.23
	entity.LastGuildCreation = &t
	entity.SessionStart = t

	engine.Flush(entity)
	engine.LoadByID(1, entity)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		entity.Gold = uint64(n + 1)
		engine.EnableQueryDebug()
		entity.IsDirty()
	}
}

func benchmarkFlusher(b *testing.B, useLocalCache, useRedisCache bool) {
	var entity *flushEntityBenchmark
	registry := &Registry{}
	registry.RegisterRedisStream("entity_changed", "default", []string{"test-group-1"})
	registry.RegisterEnum("beeorm.TestEnum", []string{"a", "b", "c"})
	engine := prepareTables(nil, registry, 5, 6, "", entity)

	schema := engine.registry.GetTableSchemaForEntity(entity).(*tableSchema)
	if !useLocalCache {
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
