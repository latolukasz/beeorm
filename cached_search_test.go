package beeorm

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type cachedSearchEntity struct {
	ORM            `orm:"localCache=2000;redisCache;"`
	ID             uint64
	Name           string `orm:"length=100;unique=FirstIndex"`
	Age            uint16 `orm:"index=SecondIndex"`
	Added          *time.Time
	ReferenceOne   *cachedSearchRefEntity `orm:"index=IndexReference"`
	Ignore         uint16                 `orm:"ignore"`
	IndexAge       *CachedQuery           `query:":Age = ? ORDER BY ID"`
	IndexAll       *CachedQuery           `query:""`
	IndexName      *CachedQuery           `queryOne:":Name = ?"`
	IndexReference *CachedQuery           `query:":ReferenceOne = ?"`
}

type cachedSearchRefEntity struct {
	ORM
	ID        uint64
	Name      string       `orm:"unique=FirstIndex"`
	IndexName *CachedQuery `queryOne:":Name = ?"`
	IndexAll  *CachedQuery `query:""`
}

func TestCachedSearchLocal(t *testing.T) {
	testCachedSearch(t, true, false)
}

func TestCachedSearchRedis(t *testing.T) {
	testCachedSearch(t, false, true)
}

func TestCachedSearchLocalRedis(t *testing.T) {
	testCachedSearch(t, true, true)
}

func testCachedSearch(t *testing.T, localCache bool, redisCache bool) {
	var entity *cachedSearchEntity
	var entityRef *cachedSearchRefEntity
	engine := PrepareTables(t, &Registry{}, 5, 6, "", entityRef, entity)
	schema := engine.Registry().GetEntitySchemaForEntity(entity)
	assert.Equal(t, 2000, schema.localCacheLimit)
	schema.DisableCache(!localCache, !redisCache)
	for i := 1; i <= 5; i++ {
		engine.Flush(&cachedSearchRefEntity{Name: "Name " + strconv.Itoa(i)})
	}

	var entities = make([]interface{}, 10)
	for i := 1; i <= 5; i++ {
		e := &cachedSearchEntity{Name: "Name " + strconv.Itoa(i), Age: uint16(10)}
		e.ReferenceOne = &cachedSearchRefEntity{ID: uint64(i)}
		entities[i-1] = e
		engine.Flush(e)
	}
	for i := 6; i <= 10; i++ {
		e := &cachedSearchEntity{Name: "Name " + strconv.Itoa(i), Age: uint16(18)}
		entities[i-1] = e
		engine.Flush(e)
	}

	pager := NewPager(1, 100)
	var rows []*cachedSearchEntity
	totalRows := engine.CachedSearch(&rows, "IndexAge", nil, 10)
	assert.EqualValues(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint64(1), rows[0].ReferenceOne.GetID())
	assert.Equal(t, uint64(2), rows[1].ReferenceOne.GetID())
	assert.Equal(t, uint64(3), rows[2].ReferenceOne.GetID())
	assert.Equal(t, uint64(4), rows[3].ReferenceOne.GetID())
	assert.Equal(t, uint64(5), rows[4].ReferenceOne.GetID())

	totalRows = engine.CachedSearchCount(entity, "IndexAge", 10)
	assert.EqualValues(t, 5, totalRows)
	totalRows = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)

	assert.Equal(t, uint64(6), rows[0].GetID())
	assert.Equal(t, uint64(7), rows[1].GetID())
	assert.Equal(t, uint64(8), rows[2].GetID())
	assert.Equal(t, uint64(9), rows[3].GetID())
	assert.Equal(t, uint64(10), rows[4].GetID())

	dbLogger := &MockLogHandler{}
	engine.RegisterQueryLogger(dbLogger, true, false, false)
	totalRows = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint64(6), rows[0].GetID())
	assert.Equal(t, uint64(7), rows[1].GetID())
	assert.Equal(t, uint64(8), rows[2].GetID())
	assert.Equal(t, uint64(9), rows[3].GetID())
	assert.Equal(t, uint64(10), rows[4].GetID())
	assert.Len(t, dbLogger.Logs, 0)

	pager = NewPager(2, 4)
	totalRows = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 1)
	assert.Equal(t, uint64(10), rows[0].GetID())
	assert.Len(t, dbLogger.Logs, 0)

	pager = NewPager(1, 5)
	//a00d4_IndexAge598045226
	totalRows = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint64(1), rows[0].GetID())
	assert.Len(t, dbLogger.Logs, 0)

	rows[0].Age = 18
	engine.Flush(rows[0])

	pager = NewPager(1, 10)
	totalRows = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Equal(t, 6, totalRows)
	assert.Len(t, rows, 6)

	assert.Equal(t, uint64(1), rows[0].GetID())
	assert.Equal(t, uint64(1), rows[0].ReferenceOne.ID)
	assert.Equal(t, uint64(6), rows[1].GetID())

	totalRows = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Equal(t, 4, totalRows)
	assert.Len(t, rows, 4)
	assert.Equal(t, uint64(2), rows[0].ID)

	totalRows = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)

	// a00d4_IndexAll1947613349 a00d4_IndexAge598045226 a00d4_IndexName3949043050 a00d4_IndexReference1980761503
	engine.Delete(rows[1])

	totalRows = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Equal(t, 3, totalRows)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint64(3), rows[0].GetID())

	totalRows = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Equal(t, 9, totalRows)
	assert.Len(t, rows, 9)

	entity = &cachedSearchEntity{Name: "Name 11", Age: uint16(18)}
	engine.Flush(entity)

	totalRows = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Equal(t, 7, totalRows)
	assert.Len(t, rows, 7)
	assert.Equal(t, uint64(11), rows[6].GetID())

	totalRows = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)

	engine.ClearCacheByIDs(entity, 1, 3)
	totalRows = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)

	var row cachedSearchEntity
	has := engine.CachedSearchOne(&row, "IndexName", "Name 6")
	assert.True(t, has)
	assert.Equal(t, uint64(6), row.GetID())

	row = cachedSearchEntity{}
	dbLogger.Clear()
	has = engine.CachedSearchOne(&row, "IndexName", "Name 6")
	assert.True(t, has)
	assert.Equal(t, uint64(6), row.GetID())
	assert.Len(t, dbLogger.Logs, 0)

	row = cachedSearchEntity{}
	has = engine.CachedSearchOneWithReferences(&row, "IndexName", []interface{}{"Name 4"}, []string{"ReferenceOne"})
	assert.True(t, has)
	assert.Equal(t, uint64(4), row.GetID())
	assert.NotNil(t, row.ReferenceOne)
	assert.Equal(t, "Name 4", row.ReferenceOne.Name)

	has = engine.CachedSearchOne(&row, "IndexName", "Name 99")
	assert.False(t, has)

	pager = NewPager(49, 1000)
	totalRows = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Equal(t, 10, totalRows)
	totalRows = engine.CachedSearch(&rows, "IndexAge", nil, 10)
	assert.Equal(t, 3, totalRows)

	totalRows, ids := engine.CachedSearchIDs(entity, "IndexAge", nil, 10)
	assert.Equal(t, 3, totalRows)
	assert.Len(t, ids, 3)
	assert.Equal(t, []uint64{3, 4, 5}, ids)

	totalRows = engine.CachedSearchWithReferences(&rows, "IndexAge", nil, []interface{}{10}, []string{"ReferenceOne"})
	assert.Equal(t, 3, totalRows)
	assert.Equal(t, "Name 3", rows[0].ReferenceOne.Name)
	assert.Equal(t, "Name 4", rows[1].ReferenceOne.Name)
	assert.Equal(t, "Name 5", rows[2].ReferenceOne.Name)

	engine.GetLocalCache().Clear()
	totalRows = engine.CachedSearchCount(entity, "IndexAge", 10)
	assert.EqualValues(t, 3, totalRows)

	assert.PanicsWithError(t, "reference WrongReference in cachedSearchEntity is not valid", func() {
		engine.CachedSearchWithReferences(&rows, "IndexAge", nil, []interface{}{10}, []string{"WrongReference"})
	})
	assert.PanicsWithError(t, "interface *beeorm.cachedSearchEntity is no slice of beeorm.Entity", func() {
		engine.CachedSearchWithReferences(entity, "IndexAge", nil, []interface{}{10}, []string{"WrongReference"})
	})

	for i := 1; i <= 200; i++ {
		e := &cachedSearchEntity{Name: "NameNew " + strconv.Itoa(i), Age: uint16(77)}
		engine.Flush(e)
	}
	pager = NewPager(30, 1000)
	totalRows = engine.CachedSearch(&rows, "IndexAge", pager, 77)
	assert.Equal(t, 200, totalRows)

	for i := 1; i <= 10; i++ {
		e := &cachedSearchEntity{Name: "NameNew13 " + strconv.Itoa(i), Age: uint16(13)}
		engine.Flush(e)
	}
	totalRows = engine.CachedSearch(&rows, "IndexAge", NewPager(3, 10), 13)
	assert.Equal(t, 10, totalRows)

	if localCache {
		pager = NewPager(1, 100)
		//a00d4_IndexReference1980761503
		totalRows = engine.CachedSearch(&rows, "IndexReference", pager, 2)
		assert.Equal(t, 0, totalRows)
		//a00d4_IndexAge598045226
		totalRows = engine.CachedSearch(&rows, "IndexAge", pager, 10)
		assert.Equal(t, 3, totalRows)
		//a00d4_IndexAge3820524834
		totalRows = engine.CachedSearch(&rows, "IndexAge", pager, 18)
		assert.Equal(t, 7, totalRows)
		rows[0].Age = 10
		rows[1].Age = 10
		engine.FlushLazy(rows[0], rows[1])
		rows[0].ReferenceOne = &cachedSearchRefEntity{ID: 2}
		rows[1].ReferenceOne = &cachedSearchRefEntity{ID: 2}
		rows[2].ReferenceOne = &cachedSearchRefEntity{ID: 2}
		engine.FlushLazy(rows[0], rows[1], rows[2])
		assert.Equal(t, 7, engine.CachedSearch(&rows, "IndexAge", pager, 18))
		assert.Equal(t, 3, engine.CachedSearch(&rows, "IndexAge", pager, 10))
		assert.Equal(t, 0, engine.CachedSearch(&rows, "IndexReference", pager, 2))

		RunLazyFlushConsumer(engine, false)
		assert.Equal(t, 5, engine.CachedSearch(&rows, "IndexAge", pager, 18))
		assert.Equal(t, 5, engine.CachedSearch(&rows, "IndexAge", pager, 10))
		assert.Equal(t, 3, engine.CachedSearch(&rows, "IndexReference", pager, 2))
	}

	//a00d4_IndexReference2048857717
	totalRows = engine.CachedSearch(&rows, "IndexReference", nil, 4)
	assert.Equal(t, 1, totalRows)
	assert.NotNil(t, rows[0])
	e := &cachedSearchEntity{ID: 4}
	engine.Load(e)
	e.Age = 44
	engine.FlushLazy(e)
	engine.DeleteLazy(e)
	RunLazyFlushConsumer(engine, false)
	totalRows = engine.CachedSearch(&rows, "IndexReference", nil, 4)
	assert.Equal(t, 0, totalRows)

	if localCache {
		f := engine.NewFlusher()
		e := &cachedSearchEntity{Name: "Mark 1", Age: 45}
		f.Track(e)
		f.Flush()
		f.Clear()
		f.Delete(e)
		f.FlushLazy()
		RunLazyFlushConsumer(engine, false)
		//a00d4_IndexAge1723156944
		totalRows = engine.CachedSearch(&rows, "IndexAge", nil, 45)
		assert.Equal(t, 0, totalRows)
	}
}

func TestCachedSearchErrors(t *testing.T) {
	engine := PrepareTables(t, &Registry{}, 5, 6, "")
	var rows []*cachedSearchEntity
	assert.PanicsWithError(t, "entity 'beeorm.cachedSearchEntity' is not registered", func() {
		_ = engine.CachedSearch(&rows, "IndexAge", nil, 10)
	})
	var row cachedSearchEntity
	assert.PanicsWithError(t, "entity 'beeorm.cachedSearchEntity' is not registered", func() {
		_ = engine.CachedSearchOne(&row, "IndexName", 10)
	})

	var entity *cachedSearchEntity
	var entityRef *cachedSearchRefEntity
	engine = PrepareTables(t, &Registry{}, 5, 6, "", entity, entityRef)
	assert.PanicsWithError(t, "index InvalidIndex not found", func() {
		_ = engine.CachedSearch(&rows, "InvalidIndex", nil, 10)
	})

	assert.PanicsWithError(t, "index InvalidIndex not found", func() {
		_ = engine.CachedSearchOne(&row, "InvalidIndex", 10)
	})

	pager := NewPager(51, 1000)
	assert.PanicsWithError(t, "max cache index page size (50000) exceeded IndexAge", func() {
		_ = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	})

	var rows2 []*cachedSearchRefEntity
	assert.PanicsWithError(t, "cache search not allowed for entity without cache: 'beeorm.cachedSearchRefEntity'", func() {
		_ = engine.CachedSearch(&rows2, "IndexAll", nil, 10)
	})

	var row2 cachedSearchRefEntity
	assert.PanicsWithError(t, "cache search not allowed for entity without cache: 'beeorm.cachedSearchRefEntity'", func() {
		_ = engine.CachedSearchOne(&row2, "IndexName", 10)
	})
}
