package beeorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type searchEntity struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string
}

func TestSearch(t *testing.T) {
	var entity *searchEntity
	orm := PrepareTables(t, NewRegistry(), entity)
	schema := GetEntitySchema[searchEntity](orm)

	var ids []uint64
	for i := 1; i <= 10; i++ {
		entity = NewEntity[searchEntity](orm)
		entity.Name = "name %d"
		ids = append(ids, entity.ID)
	}
	err := orm.Flush()
	assert.NoError(t, err)

	rows, total := SearchWithCount[searchEntity](orm, NewWhere("ID > ?", ids[1]), nil)
	assert.Equal(t, 8, total)
	assert.Equal(t, 8, rows.Len())

	foundIDs, total := SearchIDsWithCount[searchEntity](orm, NewWhere("ID > ?", ids[1]), nil)
	assert.Equal(t, 8, total)
	assert.Len(t, foundIDs, 8)
	assert.Equal(t, ids[2], foundIDs[0])

	foundIDs = SearchIDs[searchEntity](orm, NewWhere("ID > ?", ids[1]), nil)
	assert.Len(t, foundIDs, 8)
	assert.Equal(t, ids[2], foundIDs[0])

	entity, found := SearchOne[searchEntity](orm, NewWhere("ID = ?", ids[2]))
	assert.True(t, found)
	assert.NotNil(t, entity)
	assert.Equal(t, ids[2], entity.ID)

	rowsAnonymous, total := schema.SearchWithCount(orm, NewWhere("ID > ?", ids[1]), nil)
	assert.Equal(t, 8, total)
	assert.Equal(t, 8, rowsAnonymous.Len())

	iterations := 0
	for rowsAnonymous.Next() {
		row := rowsAnonymous.Entity().(*searchEntity)
		assert.Equal(t, ids[iterations+2], row.ID)
		iterations++
	}
	assert.Equal(t, 8, iterations)
	rowsAnonymous = schema.Search(orm, NewWhere("ID > ?", ids[1]), nil)
	assert.Equal(t, 8, rowsAnonymous.Len())
	foundIDs, total = schema.SearchIDsWithCount(orm, NewWhere("ID > ?", ids[1]), nil)
	assert.Equal(t, 8, total)
	assert.Len(t, foundIDs, 8)
	assert.Equal(t, ids[2], foundIDs[0])
	foundIDs = schema.SearchIDs(orm, NewWhere("ID > ?", ids[1]), nil)
	assert.Len(t, foundIDs, 8)
	assert.Equal(t, ids[2], foundIDs[0])
}
