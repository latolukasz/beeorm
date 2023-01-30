package test

import (
	"fmt"
	"github.com/latolukasz/beeorm"
	"testing"

	"github.com/stretchr/testify/assert"
)

type searchEntity struct {
	beeorm.ORM   `orm:"localCache;redisCache"`
	Name         string
	ReferenceOne *searchEntityReference
	FakeDelete   bool
}

type searchEntityReference struct {
	beeorm.ORM
	Name string
}

func TestSearch(t *testing.T) {
	var entity *searchEntity
	var reference *searchEntityReference
	engine := PrepareTables(t, &beeorm.Registry{}, 5, 6, "", entity, reference)

	for i := 1; i <= 10; i++ {
		engine.Flush(&searchEntity{Name: fmt.Sprintf("name %d", i), ReferenceOne: &searchEntityReference{Name: fmt.Sprintf("name %d", i)}})
	}
	entity = &searchEntity{}
	entity.SetID(1)
	engine.Load(entity)
	engine.Flush(entity)

	var rows []*searchEntity
	engine.LoadByIDs([]uint64{1, 2, 20}, &rows)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint64(1), rows[0].GetID())
	assert.Equal(t, uint64(2), rows[1].GetID())
	assert.Nil(t, rows[2])

	entity = &searchEntity{}
	found := engine.SearchOne(beeorm.NewWhere("ID = ?", 1), entity, "ReferenceOne")
	assert.True(t, found)
	assert.Equal(t, uint64(1), entity.GetID())
	assert.Equal(t, "name 1", entity.Name)
	assert.Equal(t, "name 1", entity.ReferenceOne.Name)
	assert.True(t, entity.ReferenceOne.IsLoaded())

	engine.Search(beeorm.NewWhere("ID > 0"), nil, &rows, "ReferenceOne")
	assert.Len(t, rows, 10)
	assert.Equal(t, uint64(1), rows[0].GetID())
	assert.Equal(t, "name 1", rows[0].Name)
	assert.Equal(t, "name 1", rows[0].ReferenceOne.Name)
	assert.True(t, rows[0].ReferenceOne.IsLoaded())

	total := engine.SearchWithCount(beeorm.NewWhere("ID > 2"), nil, &rows)
	assert.Equal(t, 8, total)
	assert.Len(t, rows, 8)

	ids, total := engine.SearchIDsWithCount(beeorm.NewWhere("ID > 2"), nil, entity)
	assert.Equal(t, 8, total)
	assert.Len(t, ids, 8)
	assert.Equal(t, uint64(3), ids[0])

	ids = engine.SearchIDs(beeorm.NewWhere("ID > 2"), nil, entity)
	assert.Len(t, ids, 8)
	assert.Equal(t, uint64(3), ids[0])

	engine = PrepareTables(t, &beeorm.Registry{}, 5, 6, "")
	assert.PanicsWithError(t, "entity 'test.searchEntity' is not registered", func() {
		engine.Search(beeorm.NewWhere("ID > 0"), nil, &rows)
	})
}