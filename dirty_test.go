package beeorm

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type dirtyReceiverEntity struct {
	ORM      `orm:"redisCache;dirty=entity_changed"`
	ID       uint
	Name     string `orm:"dirty=name_changed"`
	LastName string `orm:"dirty=name_changed"`
	Age      uint64
}

func TestDirtyConsumer(t *testing.T) {
	var entity *dirtyReceiverEntity
	registry := &Registry{}
	registry.RegisterRedisStream("entity_changed", "default", []string{"test-group-1"})
	registry.RegisterRedisStream("name_changed", "default", []string{"test-group-2"})
	engine, def := prepareTables(t, registry, 5, entity)
	defer def()

	channels := engine.GetRegistry().GetRedisStreams()
	assert.Len(t, channels, 1)
	assert.Len(t, channels["default"], 4)

	consumer := engine.GetEventBroker().Consumer("test-group-1")
	consumer.DisableLoop()
	consumer.(*eventsConsumer).blockTime = time.Millisecond
	consumer2 := engine.GetEventBroker().Consumer("test-group-2")
	consumer2.DisableLoop()
	consumer2.(*eventsConsumer).blockTime = time.Millisecond

	e := &dirtyReceiverEntity{Name: "John", Age: 18}
	engine.Flush(e)
	e = &dirtyReceiverEntity{Name: "Tom", Age: 18}
	engine.Flush(e)

	valid := false
	consumer.Consume(context.Background(), 10, func(events []Event) {
		valid = true
		assert.Len(t, events, 2)
		dirty1 := EventDirtyEntity(events[0])
		dirty2 := EventDirtyEntity(events[1])
		assert.Equal(t, uint64(1), dirty1.ID())
		assert.Equal(t, uint64(2), dirty2.ID())
		assert.True(t, dirty1.Added())
		assert.True(t, dirty2.Added())
		assert.False(t, dirty1.Updated())
		assert.False(t, dirty2.Updated())
		assert.False(t, dirty1.Deleted())
		assert.False(t, dirty1.Deleted())
		assert.Equal(t, "dirtyReceiverEntity", dirty1.TableSchema().GetTableName())
		assert.Equal(t, "dirtyReceiverEntity", dirty1.TableSchema().GetTableName())
	})
	assert.True(t, valid)

	iterations := 0
	consumer2.Consume(context.Background(), 1, func(events []Event) {
		iterations++
		assert.Len(t, events, 1)
		dirty := EventDirtyEntity(events[0])
		assert.Equal(t, uint64(iterations), dirty.ID())
		assert.True(t, dirty.Added())
		assert.False(t, dirty.Updated())
		assert.False(t, dirty.Deleted())
		assert.Equal(t, "dirtyReceiverEntity", dirty.TableSchema().GetTableName())
	})
	assert.Equal(t, 2, iterations)

	e.Name = "Bob"
	engine.Flush(e)
	valid = false
	consumer.Consume(context.Background(), 2, func(events []Event) {
		valid = true
		assert.Len(t, events, 1)
		dirty := EventDirtyEntity(events[0])
		assert.Equal(t, uint64(2), dirty.ID())
		assert.False(t, dirty.Added())
		assert.True(t, dirty.Updated())
		assert.False(t, dirty.Deleted())
		assert.Equal(t, "dirtyReceiverEntity", dirty.TableSchema().GetTableName())
	})
	assert.True(t, valid)

	valid = false
	consumer2.Consume(context.Background(), 1, func(events []Event) {
		valid = true
		assert.Len(t, events, 1)
		dirty := EventDirtyEntity(events[0])
		assert.Equal(t, uint64(2), dirty.ID())
		assert.False(t, dirty.Added())
		assert.True(t, dirty.Updated())
		assert.False(t, dirty.Deleted())
		assert.Equal(t, "dirtyReceiverEntity", dirty.TableSchema().GetTableName())
	})
	assert.True(t, valid)

	e.Age = 30
	engine.Flush(e)
	valid = false
	consumer.Consume(context.Background(), 2, func(events []Event) {
		valid = true
		assert.Len(t, events, 1)
		assert.Equal(t, uint64(2), EventDirtyEntity(events[0]).ID())
	})
	assert.True(t, valid)
	valid = true
	consumer.Consume(context.Background(), 1, func(events []Event) {
		valid = false
	})
	assert.True(t, valid)

	e.Name = "test transaction"
	engine.NewFlusher().Track(e).FlushInTransaction()
	valid = false
	consumer.Consume(context.Background(), 2, func(events []Event) {
		valid = true
		assert.Len(t, events, 1)
		assert.Equal(t, uint64(2), EventDirtyEntity(events[0]).ID())
	})
	assert.True(t, valid)

	valid = false
	consumer2.Consume(context.Background(), 1, func(events []Event) {
		valid = true
	})
	assert.True(t, valid)

	engine.Delete(e)

	valid = false
	consumer.Consume(context.Background(), 2, func(events []Event) {
		valid = true
		assert.Len(t, events, 1)
		dirty := EventDirtyEntity(events[0])
		assert.Equal(t, uint64(2), dirty.ID())
		assert.False(t, dirty.Added())
		assert.False(t, dirty.Updated())
		assert.True(t, dirty.Deleted())
		assert.Equal(t, "dirtyReceiverEntity", dirty.TableSchema().GetTableName())
	})
	assert.True(t, valid)

	valid = false
	consumer2.Consume(context.Background(), 1, func(events []Event) {
		valid = true
		assert.Len(t, events, 1)
		dirty := EventDirtyEntity(events[0])
		assert.Equal(t, uint64(2), dirty.ID())
		assert.False(t, dirty.Added())
		assert.False(t, dirty.Updated())
		assert.True(t, dirty.Deleted())
		assert.Equal(t, "dirtyReceiverEntity", dirty.TableSchema().GetTableName())
	})
	assert.True(t, valid)

	engine.MarkDirty(e, "name_changed", 2)
	valid = false
	consumer2.Consume(context.Background(), 1, func(events []Event) {
		valid = true
		assert.Len(t, events, 1)
		dirty := EventDirtyEntity(events[0])
		assert.Equal(t, uint64(2), dirty.ID())
		assert.False(t, dirty.Added())
		assert.True(t, dirty.Updated())
		assert.False(t, dirty.Deleted())
		assert.Equal(t, "dirtyReceiverEntity", dirty.TableSchema().GetTableName())
	})
	assert.True(t, valid)

	e = &dirtyReceiverEntity{Name: "Adam", Age: 30}
	engine.FlushLazy(e)

	valid = false
	consumer.Consume(context.Background(), 1, func(events []Event) {
		valid = true
	})
	assert.False(t, valid)

	receiver := NewBackgroundConsumer(engine)
	receiver.DisableLoop()
	receiver.blockTime = time.Millisecond
	receiver.Digest(context.Background())

	consumer.Consume(context.Background(), 1, func(events []Event) {
		assert.Len(t, events, 1)
		dirty := EventDirtyEntity(events[0])
		assert.True(t, dirty.Added())
		assert.False(t, dirty.Updated())
		assert.False(t, dirty.Deleted())
		assert.Equal(t, uint64(3), dirty.ID())
		assert.Equal(t, "dirtyReceiverEntity", dirty.TableSchema().GetTableName())
		valid = true
	})
	assert.True(t, valid)

	engine.LoadByID(3, e)
	e.Age = 40
	engine.FlushLazy(e)
	valid = false
	consumer.Consume(context.Background(), 1, func(events []Event) {
		valid = true
	})
	assert.False(t, valid)
	receiver.Digest(context.Background())
	consumer.Consume(context.Background(), 1, func(events []Event) {
		assert.Len(t, events, 1)
		dirty := EventDirtyEntity(events[0])
		assert.False(t, dirty.Added())
		assert.True(t, dirty.Updated())
		assert.False(t, dirty.Deleted())
		assert.Equal(t, uint64(3), dirty.ID())
		assert.Equal(t, "dirtyReceiverEntity", dirty.TableSchema().GetTableName())
		valid = true
	})
	assert.True(t, valid)

	engine.LoadByID(3, e)
	flusher := engine.NewFlusher()
	flusher.Delete(e)
	flusher.FlushLazy()
	valid = false
	consumer.Consume(context.Background(), 1, func(events []Event) {
		valid = true
	})
	assert.False(t, valid)
	receiver.Digest(context.Background())
	consumer.Consume(context.Background(), 1, func(events []Event) {
		assert.Len(t, events, 1)
		dirty := EventDirtyEntity(events[0])
		assert.False(t, dirty.Added())
		assert.False(t, dirty.Updated())
		assert.True(t, dirty.Deleted())
		assert.Equal(t, uint64(3), dirty.ID())
		assert.Equal(t, "dirtyReceiverEntity", dirty.TableSchema().GetTableName())
		valid = true
	})
	assert.True(t, valid)

	e = &dirtyReceiverEntity{}
	engine.LoadByID(1, e)
	flusher = engine.NewFlusher()
	e.Age = 38
	flusher.Track(e)
	engine.GetMysql().Begin()
	_ = flusher.FlushWithCheck()
	engine.GetMysql().Commit()

	valid = false
	receiver.Digest(context.Background())
	consumer.Consume(context.Background(), 1, func(events []Event) {
		assert.Len(t, events, 1)
		dirty := EventDirtyEntity(events[0])
		assert.False(t, dirty.Added())
		assert.True(t, dirty.Updated())
		assert.False(t, dirty.Deleted())
		assert.Equal(t, uint64(1), dirty.ID())
		assert.Equal(t, "dirtyReceiverEntity", dirty.TableSchema().GetTableName())
		valid = true
	})
	assert.True(t, valid)
}
