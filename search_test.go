package beeorm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type searchEntity struct {
	ORM           `orm:"localCache;redisCache"`
	ID            uint
	Name          string
	ReferenceOne  *searchEntityReference
	ReferenceMany []*searchEntityReference
	FakeDelete    bool
}

type searchEntityReference struct {
	ORM
	ID   uint
	Name string
}

func TestSearch(t *testing.T) {
	var entity *searchEntity
	var reference *searchEntityReference
	engine := PrepareTables(t, &Registry{}, 5, entity, reference)

	flusher := engine.NewFlusher()
	for i := 1; i <= 10; i++ {
		flusher.Track(&searchEntity{Name: fmt.Sprintf("name %d", i), ReferenceOne: &searchEntityReference{Name: fmt.Sprintf("name %d", i)}})
	}
	flusher.Flush()
	entity = &searchEntity{ID: 1}
	engine.Load(entity)
	entity.ReferenceMany = []*searchEntityReference{{ID: 1}, {ID: 2}, {ID: 3}}
	engine.Flush(entity)

	var rows []*searchEntity
	missing := engine.LoadByIDs([]uint64{1, 2, 20}, &rows)
	assert.True(t, missing)
	assert.Len(t, rows, 3)
	assert.True(t, missing)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Equal(t, uint(2), rows[1].ID)
	assert.Nil(t, rows[2])

	entity = &searchEntity{}
	found := engine.SearchOne(NewWhere("ID = ?", 1), entity, "ReferenceOne")
	assert.True(t, found)
	assert.Equal(t, uint(1), entity.ID)
	assert.Equal(t, "name 1", entity.Name)
	assert.Equal(t, "name 1", entity.ReferenceOne.Name)
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.False(t, entity.ReferenceOne.IsLazy())

	entity = &searchEntity{}
	found = engine.SearchOneLazy(NewWhere("ID = ?", 1), entity, "ReferenceOne")
	assert.True(t, found)
	assert.Equal(t, uint(1), entity.ID)
	assert.Equal(t, "", entity.Name)
	assert.Equal(t, "", entity.ReferenceOne.Name)
	assert.Equal(t, "name 1", entity.GetFieldLazy(engine, "Name"))
	assert.Equal(t, "name 1", entity.ReferenceOne.GetFieldLazy(engine, "Name"))
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.True(t, entity.ReferenceOne.IsLazy())

	engine.Search(NewWhere("ID > 0"), nil, &rows, "ReferenceOne")
	assert.Len(t, rows, 10)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Equal(t, "name 1", rows[0].Name)
	assert.Equal(t, "name 1", rows[0].ReferenceOne.Name)
	assert.True(t, rows[0].ReferenceOne.IsLoaded())
	assert.False(t, rows[0].ReferenceOne.IsLazy())

	engine.SearchLazy(NewWhere("ID > 0"), nil, &rows, "ReferenceOne")
	assert.Len(t, rows, 10)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Equal(t, "", rows[0].Name)
	assert.Equal(t, "", rows[0].ReferenceOne.Name)
	assert.Equal(t, "name 1", rows[0].GetFieldLazy(engine, "Name"))
	assert.Equal(t, "name 1", rows[0].ReferenceOne.GetFieldLazy(engine, "Name"))
	assert.True(t, rows[0].ReferenceOne.IsLoaded())
	assert.True(t, rows[0].ReferenceOne.IsLazy())

	total := engine.SearchWithCount(NewWhere("ID > 2"), nil, &rows)
	assert.Equal(t, 8, total)
	assert.Len(t, rows, 8)
	assert.False(t, rows[0].IsLazy())

	total = engine.SearchWithCountLazy(NewWhere("ID > 2"), nil, &rows)
	assert.Equal(t, 8, total)
	assert.Len(t, rows, 8)
	assert.True(t, rows[0].IsLazy())

	ids, total := engine.SearchIDsWithCount(NewWhere("ID > 2"), nil, entity)
	assert.Equal(t, 8, total)
	assert.Len(t, ids, 8)
	assert.Equal(t, uint64(3), ids[0])

	ids = engine.SearchIDs(NewWhere("ID > 2"), nil, entity)
	assert.Len(t, ids, 8)
	assert.Equal(t, uint64(3), ids[0])

	entity = &searchEntity{ID: 1}
	engine.Load(entity, "ReferenceMany")
	assert.Len(t, entity.ReferenceMany, 3)
	assert.True(t, entity.ReferenceMany[0].IsLoaded())
	assert.True(t, entity.ReferenceMany[1].IsLoaded())
	assert.True(t, entity.ReferenceMany[2].IsLoaded())

	engine = PrepareTables(t, &Registry{}, 5)
	assert.PanicsWithError(t, "entity 'beeorm.searchEntity' is not registered", func() {
		engine.Search(NewWhere("ID > 0"), nil, &rows)
	})
}
