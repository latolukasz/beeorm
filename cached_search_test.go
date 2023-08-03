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
	c := PrepareTables(t, &Registry{}, 5, 6, "", entityRef, entity)
	schema := c.Engine().Registry().EntitySchema(entity)
	schema.DisableCache(!localCache, !redisCache)
	for i := 1; i <= 5; i++ {
		c.Flusher().Track(&cachedSearchRefEntity{Name: "Name " + strconv.Itoa(i)}).Flush()
	}

	var entities = make([]interface{}, 10)
	for i := 1; i <= 5; i++ {
		e := &cachedSearchEntity{Name: "Name " + strconv.Itoa(i), Age: uint16(10)}
		e.ReferenceOne = &cachedSearchRefEntity{ID: uint64(i)}
		entities[i-1] = e
		c.Flusher().Track(e)
	}
	for i := 6; i <= 10; i++ {
		e := &cachedSearchEntity{Name: "Name " + strconv.Itoa(i), Age: uint16(18)}
		entities[i-1] = e
		c.Flusher().Track(e)
	}
	c.Flusher().Flush()

	pager := NewPager(1, 100)
	var rows []*cachedSearchEntity
	totalRows := CachedSearch(c, &rows, "IndexAge", nil, 10)
	assert.EqualValues(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint64(1), rows[0].ReferenceOne.GetID())
	assert.Equal(t, uint64(2), rows[1].ReferenceOne.GetID())
	assert.Equal(t, uint64(3), rows[2].ReferenceOne.GetID())
	assert.Equal(t, uint64(4), rows[3].ReferenceOne.GetID())
	assert.Equal(t, uint64(5), rows[4].ReferenceOne.GetID())

	totalRows = CachedSearch(c, &rows, "IndexAge", pager, 18)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)

	assert.Equal(t, uint64(6), rows[0].GetID())
	assert.Equal(t, uint64(7), rows[1].GetID())
	assert.Equal(t, uint64(8), rows[2].GetID())
	assert.Equal(t, uint64(9), rows[3].GetID())
	assert.Equal(t, uint64(10), rows[4].GetID())

	dbLogger := &MockLogHandler{}
	c.RegisterQueryLogger(dbLogger, true, false, false)
	totalRows = CachedSearch(c, &rows, "IndexAge", pager, 18)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint64(6), rows[0].GetID())
	assert.Equal(t, uint64(7), rows[1].GetID())
	assert.Equal(t, uint64(8), rows[2].GetID())
	assert.Equal(t, uint64(9), rows[3].GetID())
	assert.Equal(t, uint64(10), rows[4].GetID())
	assert.Len(t, dbLogger.Logs, 0)

	pager = NewPager(2, 4)
	totalRows = CachedSearch(c, &rows, "IndexAge", pager, 18)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 1)
	assert.Equal(t, uint64(10), rows[0].GetID())
	assert.Len(t, dbLogger.Logs, 0)

	pager = NewPager(1, 5)
	totalRows = CachedSearch(c, &rows, "IndexAge", pager, 10)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint64(1), rows[0].GetID())
	assert.Len(t, dbLogger.Logs, 0)

	rows[0].Age = 18
	c.Flusher().Track(rows[0]).Flush()

	pager = NewPager(1, 10)
	totalRows = CachedSearch(c, &rows, "IndexAge", pager, 18)
	assert.Equal(t, 6, totalRows)
	assert.Len(t, rows, 6)

	assert.Equal(t, uint64(1), rows[0].GetID())
	assert.Equal(t, uint64(1), rows[0].ReferenceOne.ID)
	assert.Equal(t, uint64(6), rows[1].GetID())

	totalRows = CachedSearch(c, &rows, "IndexAge", pager, 10)
	assert.Equal(t, 4, totalRows)
	assert.Len(t, rows, 4)
	assert.Equal(t, uint64(2), rows[0].ID)

	totalRows = CachedSearch(c, &rows, "IndexAll", pager)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)

	// a00d4_IndexAll1947613349 a00d4_IndexAge598045226 a00d4_IndexName3949043050 a00d4_IndexReference1980761503
	c.Flusher().Delete(rows[1]).Flush()

	totalRows = CachedSearch(c, &rows, "IndexAge", pager, 10)
	assert.Equal(t, 3, totalRows)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint64(3), rows[0].GetID())

	totalRows = CachedSearch(c, &rows, "IndexAll", pager)
	assert.Equal(t, 9, totalRows)
	assert.Len(t, rows, 9)

	entity = &cachedSearchEntity{Name: "Name 11", Age: uint16(18)}
	c.Flusher().Track(entity).Flush()

	totalRows = CachedSearch(c, &rows, "IndexAge", pager, 18)
	assert.Equal(t, 7, totalRows)
	assert.Len(t, rows, 7)
	assert.Equal(t, uint64(11), rows[6].GetID())

	totalRows = CachedSearch(c, &rows, "IndexAll", pager)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)

	ClearCacheByIDs[*cachedSearchEntity](c, 1, 3)
	totalRows = CachedSearch(c, &rows, "IndexAll", pager)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)

	row, has := CachedSearchOne[*cachedSearchEntity](c, "IndexName", "Name 6")
	assert.True(t, has)
	assert.Equal(t, uint64(6), row.GetID())

	dbLogger.Clear()
	row, has = CachedSearchOne[*cachedSearchEntity](c, "IndexName", "Name 6")
	assert.True(t, has)
	assert.Equal(t, uint64(6), row.GetID())
	assert.Len(t, dbLogger.Logs, 0)

	row, has = CachedSearchOneWithReferences[*cachedSearchEntity](c, "IndexName", []interface{}{"Name 4"}, []string{"ReferenceOne"})
	assert.True(t, has)
	assert.Equal(t, uint64(4), row.GetID())
	assert.NotNil(t, row.ReferenceOne)
	assert.Equal(t, "Name 4", row.ReferenceOne.Name)

	row, has = CachedSearchOne[*cachedSearchEntity](c, "IndexName", "Name 99")
	assert.False(t, has)
	assert.Nil(t, t, row)

	pager = NewPager(49, 1000)
	totalRows = CachedSearch(c, &rows, "IndexAll", pager)
	assert.Equal(t, 10, totalRows)
	totalRows = CachedSearch(c, &rows, "IndexAge", nil, 10)
	assert.Equal(t, 3, totalRows)

	totalRows = CachedSearchWithReferences(c, &rows, "IndexAge", nil, []interface{}{10}, []string{"ReferenceOne"})
	assert.Equal(t, 3, totalRows)
	assert.Equal(t, "Name 3", rows[0].ReferenceOne.Name)
	assert.Equal(t, "Name 4", rows[1].ReferenceOne.Name)
	assert.Equal(t, "Name 5", rows[2].ReferenceOne.Name)

	c.Engine().LocalCache(DefaultPoolCode).Clear(c)
	totalRows = CachedSearch(c, &rows, "IndexAge", nil, 10)
	assert.EqualValues(t, 3, totalRows)

	assert.PanicsWithError(t, "reference WrongReference in cachedSearchEntity is not valid", func() {
		CachedSearchWithReferences(c, &rows, "IndexAge", nil, []interface{}{10}, []string{"WrongReference"})
	})
	assert.PanicsWithError(t, "interface *beeorm.cachedSearchEntity is no slice of beeorm.Entity", func() {
		CachedSearchWithReferences(c, entity, "IndexAge", nil, []interface{}{10}, []string{"WrongReference"})
	})

	for i := 1; i <= 200; i++ {
		e := &cachedSearchEntity{Name: "NameNew " + strconv.Itoa(i), Age: uint16(77)}
		c.Flusher().Track(e)
	}
	c.Flusher().Flush()
	pager = NewPager(30, 1000)
	totalRows = CachedSearch(c, &rows, "IndexAge", pager, 77)
	assert.Equal(t, 200, totalRows)

	for i := 1; i <= 10; i++ {
		e := &cachedSearchEntity{Name: "NameNew13 " + strconv.Itoa(i), Age: uint16(13)}
		c.Flusher().Track(e)
	}
	c.Flusher().Flush()
	totalRows = CachedSearch(c, &rows, "IndexAge", NewPager(3, 10), 13)
	assert.Equal(t, 10, totalRows)

	if localCache {
		pager = NewPager(1, 100)
		//a00d4_IndexReference1980761503
		totalRows = CachedSearch(c, &rows, "IndexReference", pager, 2)
		assert.Equal(t, 0, totalRows)
		//a00d4_IndexAge598045226
		totalRows = CachedSearch(c, &rows, "IndexAge", pager, 10)
		assert.Equal(t, 3, totalRows)
		//a00d4_IndexAge3820524834
		totalRows = CachedSearch(c, &rows, "IndexAge", pager, 18)
		assert.Equal(t, 7, totalRows)
		rows[0].Age = 10
		rows[1].Age = 10
		c.Flusher().Track(rows[0], rows[1]).FlushLazy()
		rows[0].ReferenceOne = &cachedSearchRefEntity{ID: 2}
		rows[1].ReferenceOne = &cachedSearchRefEntity{ID: 2}
		rows[2].ReferenceOne = &cachedSearchRefEntity{ID: 2}
		c.Flusher().Track(rows[0], rows[1], rows[2]).FlushLazy()
		assert.Equal(t, 7, CachedSearch(c, &rows, "IndexAge", pager, 18))
		assert.Equal(t, 3, CachedSearch(c, &rows, "IndexAge", pager, 10))
		assert.Equal(t, 0, CachedSearch(c, &rows, "IndexReference", pager, 2))

		RunLazyFlushConsumer(c, false)
		assert.Equal(t, 5, CachedSearch(c, &rows, "IndexAge", pager, 18))
		assert.Equal(t, 5, CachedSearch(c, &rows, "IndexAge", pager, 10))
		assert.Equal(t, 3, CachedSearch(c, &rows, "IndexReference", pager, 2))
	}

	//a00d4_IndexReference2048857717
	totalRows = CachedSearch(c, &rows, "IndexReference", nil, 4)
	assert.Equal(t, 1, totalRows)
	assert.NotNil(t, rows[0])
	e := &cachedSearchEntity{ID: 4}
	Load(c, e)
	e.Age = 44
	c.Flusher().Track(e).FlushLazy()
	c.Flusher().Delete(e).FlushLazy()
	RunLazyFlushConsumer(c, false)
	totalRows = CachedSearch(c, &rows, "IndexReference", nil, 4)
	assert.Equal(t, 0, totalRows)

	if localCache {
		f := c.Flusher()
		e := &cachedSearchEntity{Name: "Mark 1", Age: 45}
		f.Track(e)
		f.Flush()
		f.Delete(e)
		f.FlushLazy()
		RunLazyFlushConsumer(c, false)
		//a00d4_IndexAge1723156944
		totalRows = CachedSearch(c, &rows, "IndexAge", nil, 45)
		assert.Equal(t, 0, totalRows)
	}
}

func TestCachedSearchErrors(t *testing.T) {
	c := PrepareTables(t, &Registry{}, 5, 6, "")
	var rows []*cachedSearchEntity
	assert.PanicsWithError(t, "entity 'beeorm.cachedSearchEntity' is not registered", func() {
		_ = CachedSearch(c, &rows, "IndexAge", nil, 10)
	})
	assert.PanicsWithError(t, "entity 'beeorm.cachedSearchEntity' is not registered", func() {
		_, _ = CachedSearchOne[*cachedSearchEntity](c, "IndexName", 10)
	})

	var entity *cachedSearchEntity
	var entityRef *cachedSearchRefEntity
	c = PrepareTables(t, &Registry{}, 5, 6, "", entity, entityRef)
	assert.PanicsWithError(t, "index InvalidIndex not found", func() {
		_ = CachedSearch(c, &rows, "InvalidIndex", nil, 10)
	})

	assert.PanicsWithError(t, "index InvalidIndex not found", func() {
		_, _ = CachedSearchOne[*cachedSearchEntity](c, "InvalidIndex", 10)
	})

	pager := NewPager(51, 1000)
	assert.PanicsWithError(t, "max cache index page size (50000) exceeded IndexAge", func() {
		_ = CachedSearch(c, &rows, "IndexAge", pager, 10)
	})

	var rows2 []*cachedSearchRefEntity
	assert.PanicsWithError(t, "cache search not allowed for entity without cache: 'beeorm.cachedSearchRefEntity'", func() {
		_ = CachedSearch(c, &rows2, "IndexAll", nil, 10)
	})

	assert.PanicsWithError(t, "cache search not allowed for entity without cache: 'beeorm.cachedSearchRefEntity'", func() {
		_, _ = CachedSearchOne[*cachedSearchRefEntity](c, "IndexName", 10)
	})
}
