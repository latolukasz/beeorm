package beeorm

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

type flushEntityAsyncStats struct {
	ID   uint64 `orm:"split_async_flush"`
	Name string `orm:"required"`
}

type flushEntityAsyncStatsGroup1 struct {
	ID   uint64
	Name string `orm:"required"`
}

type flushEntityAsyncStatsGroup2 struct {
	ID   uint64
	Name string `orm:"required"`
}

func TestAsync(t *testing.T) {
	registry := NewRegistry()
	c := PrepareTables(t, registry, flushEntityAsyncStats{})
	schema := getEntitySchema[flushEntityAsyncStats](c)

	for i := 0; i < asyncConsumerPage+10; i++ {
		entity := NewEntity[flushEntityAsyncStats](c)
		entity.Name = "test " + strconv.Itoa(i)
		err := c.FlushAsync()
		assert.NoError(t, err)
	}

	stats := ReadAsyncFlushEvents(c)
	assert.Len(t, stats, 1)
	stat := stats[0]
	assert.Len(t, stat.EntitySchemas(), 1)
	assert.Equal(t, schema, stat.EntitySchemas()[0])
	assert.Equal(t, uint64(asyncConsumerPage+10), stat.EventsCount())
	assert.Equal(t, uint64(0), stat.ErrorsCount())
	events := stat.Events(100)
	assert.Len(t, events, 100)
	assert.Len(t, events[0].QueryAttributes, 2)
	assert.Contains(t, events[0].QueryAttributes[1], "test 0")
	assert.Len(t, events[1].QueryAttributes, 2)
	assert.Contains(t, events[1].QueryAttributes[1], "test 1")
	errors := stat.Errors(100)
	assert.Len(t, errors, 0)

	stat.TrimEvents(1)
	events = stat.Events(100)
	assert.Contains(t, events[0].QueryAttributes[1], "test 1")
	stat.TrimEvents(2)
	events = stat.Events(100)
	assert.Contains(t, events[0].QueryAttributes[1], "test 3")

	stat.TrimEvents(asyncConsumerPage * 2)
	events = stat.Events(100)
	assert.Len(t, events, 0)

	for i := 0; i < asyncConsumerPage+10; i++ {
		entity := NewEntity[flushEntityAsyncStats](c)
		entity.Name = "test " + strconv.Itoa(i)
		err := c.FlushAsync()
		assert.NoError(t, err)
	}

	schema.GetDB().Exec(c, "ALTER TABLE flushEntityAsyncStats DROP COLUMN Name")
	err := ConsumeAsyncFlushEvents(c, false)
	assert.NoError(t, err)

	assert.Equal(t, uint64(0), stat.EventsCount())
	assert.Equal(t, uint64(asyncConsumerPage+10), stat.ErrorsCount())
	errors = stat.Errors(10)
	assert.Len(t, errors, 10)
	assert.Len(t, errors[0].QueryAttributes, 2)
	assert.Contains(t, errors[0].QueryAttributes[1], "test 0")
	assert.Equal(t, "Error 1054 (42S22): Unknown column 'Name' in 'field list'", errors[0].Error)
	assert.Len(t, errors[1].QueryAttributes, 2)
	assert.Contains(t, errors[1].QueryAttributes[1], "test 1")
	assert.Equal(t, "Error 1054 (42S22): Unknown column 'Name' in 'field list'", errors[1].Error)

	stat.TrimErrors(1)
	errors = stat.Errors(10)
	assert.Len(t, errors, 10)
	assert.Contains(t, errors[0].QueryAttributes[1], "test 1")
}

func TestAsyncGrouped(t *testing.T) {
	registry := NewRegistry()
	c := PrepareTables(t, registry, flushEntityAsyncStats{}, flushEntityAsyncStatsGroup1{}, flushEntityAsyncStatsGroup2{})
	schema := getEntitySchema[flushEntityAsyncStats](c)
	schemaGroup1 := getEntitySchema[flushEntityAsyncStatsGroup1](c)
	schemaGroup2 := getEntitySchema[flushEntityAsyncStatsGroup2](c)

	for i := 0; i < 10; i++ {
		entity := NewEntity[flushEntityAsyncStats](c)
		entity.Name = "test " + strconv.Itoa(i)
		entity2 := NewEntity[flushEntityAsyncStatsGroup1](c)
		entity2.Name = "a " + strconv.Itoa(i)
		entity3 := NewEntity[flushEntityAsyncStatsGroup2](c)
		entity3.Name = "b " + strconv.Itoa(i)
		err := c.FlushAsync()
		assert.NoError(t, err)
	}

	stats := ReadAsyncFlushEvents(c)
	assert.Len(t, stats, 2)
	for i := 0; i < 2; i++ {
		stat := stats[0]
		if len(stat.EntitySchemas()) == 1 {
			assert.Len(t, stat.EntitySchemas(), 1)
			assert.Equal(t, schema, stat.EntitySchemas()[0])
			assert.Equal(t, uint64(10), stat.EventsCount())
			assert.Equal(t, uint64(0), stat.ErrorsCount())
		} else {
			assert.Len(t, stat.EntitySchemas(), 2)
			assert.Contains(t, stat.EntitySchemas(), schemaGroup1)
			assert.Contains(t, stat.EntitySchemas(), schemaGroup2)
			assert.Equal(t, uint64(20), stat.EventsCount())
			assert.Equal(t, uint64(0), stat.ErrorsCount())
		}

	}
}
